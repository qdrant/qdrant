import 'dart:ffi' show Abi;
import 'dart:io';

import 'package:code_assets/code_assets.dart';
import 'package:crypto/crypto.dart';
import 'package:hooks/hooks.dart';

/// Native Assets build hook for the Qdrant Edge SDK.
///
/// Registers the `qdrant-edge-ffi` cdylib under the asset id the generated
/// binding expects (`package:qdrant_edge/uniffi:qdrant_edge_ffi`). It provisions
/// the library three ways, in priority order:
///
///  1. **Local prebuilt** — `<prebuilt_dir>/<os_arch>/`, where `prebuilt_dir`
///     is a user-define (`hooks: user_defines: qdrant_edge: prebuilt_dir:` in
///     the app's pubspec), or `native/prebuilt/<os_arch>/` inside the package.
///     A dev/CI override.
///  2. **From source (host, in-tree)** — when the target is the host OS and
///     architecture and the Cargo workspace is present (developing inside the
///     qdrant monorepo), build the cdylib with `cargo +nightly`.
///  3. **Download** — otherwise fetch the per-platform, SHA256-pinned archive
///     from the GitHub Release and cache it. This is how a pub.dev consumer (no
///     Rust toolchain, no workspace) and cross-compile targets are served.
///
/// A target that matches none of these fails the build, except the slices
/// Flutter requests by default that the engine does not support (see section 4).

/// Release that carries the prebuilt cdylibs (published by edge-dart-native.yml).
///
/// Pre-merge the archives live on the fork that runs the release workflow; the
/// base moves to `github.com/qdrant/qdrant` once the SDK lands upstream and the
/// release is cut there. Pointing at qdrant/qdrant before that release exists
/// would 404 every download, so the fork URL here is deliberate for now.
const _releaseTag = 'edge-dart-native-v0.9.0';
const _releaseBase =
    'https://github.com/DenisovAV/qdrant/releases/download/$_releaseTag';

/// SHA256 of each `qdrant-edge-ffi-<os_arch>.tar.gz` in the release — the pin
/// that makes the download tamper-evident. Copied from the release's
/// `checksums.txt` (produced by edge-dart-native.yml); `tool/check_pins.sh`
/// checks the two agree before publishing.
const _sha256 = <String, String>{
  'linux_x86_64':
      'b235fa9f6e1e5a09a6c30f0c18fcc2ce524a18b7785494b341668b6dc68e798c',
  'linux_arm64':
      '256fa088ca7be9b0e9a5a08797078577302e1dd302d246b620abcc13fa9db0b6',
  'windows_x86_64':
      '868b0b1e3b7ffaf517a1e225be67c1022e4ed02047f96052a10c9455e06fd9c2',
  'macos_arm64':
      'ed761d34e5ce612da0164263a91594c67dc6a0e2119d337d652cb6d8e6828315',
  'ios_arm64':
      '6fbcb612d9b3ed30645eb4df7e3b4b681eaf0c2f749e8920e2b2edcede312df3',
  'ios_sim_arm64':
      'db4ec3e8da81fac489c2315f7d6def5daf4786bc4f1aa112b9b66e3c633b896c',
  'android_arm64':
      'c16f1c4de895bda0124118f26a13e329cdd132c070f1ec8fcb9b6afc415dc605',
  'android_x86_64':
      '2eb0570e5ce1a0e6cfed8744047195291c38b7d1f09da9392f4cd72b5c3c8792',
};

void main(List<String> args) async {
  await build(args, (input, output) async {
    if (!input.config.buildCodeAssets) return;

    final code = input.config.code;
    final targetOS = code.targetOS;
    final arch = code.targetArchitecture;
    final iosSdk = targetOS == OS.iOS ? code.iOS.targetSdk : null;
    final dirName = _dirName(targetOS, arch, iosSdk);
    final libFileName = _libFileName(targetOS);

    // Apple builds take a directoryTreeSignature over each input dir; if the
    // registered CodeAsset.file lives inside one (a prebuilt/cache dir we also
    // list as a dependency), Xcode's "Flutter Assemble" depends on its own
    // output → "Cycle inside Flutter Assemble". Copy into the hook's
    // outputDirectory (an allowed root, never an input) and register from there.
    Uri stage(Uri src) {
      if (targetOS != OS.macOS && targetOS != OS.iOS) return src;
      final dest = input.outputDirectory.resolve(src.pathSegments.last);
      final s = File.fromUri(src);
      final d = File.fromUri(dest);
      // Re-stage when the dest is missing, a different size, OR older than the
      // source — a rebuilt cdylib is frequently the same size.
      if (!d.existsSync() ||
          d.lengthSync() != s.lengthSync() ||
          d.statSync().modified.isBefore(s.statSync().modified)) {
        d.parent.createSync(recursive: true);
        s.copySync(dest.toFilePath());
      }
      return dest;
    }

    void register(Uri file) {
      output.assets.code.add(
        CodeAsset(
          package: 'qdrant_edge',
          name: 'uniffi:qdrant_edge_ffi',
          linkMode: DynamicLoadingBundled(),
          file: file,
        ),
      );
    }

    // ---- 1. Local prebuilt override --------------------------------------
    if (dirName != null) {
      // Hooks run with a filtered environment, so the override is a
      // user-define rather than an environment variable.
      final override = input.userDefines.path('prebuilt_dir');
      final dirs = <Uri>[
        // Directory.uri adds the trailing slash a user-defined path lacks;
        // without it resolve() would replace the last segment.
        if (override != null) Directory.fromUri(override).uri.resolve('$dirName/'),
        input.packageRoot.resolve('native/prebuilt/$dirName/'),
      ];
      for (final dir in dirs) {
        final lib = dir.resolve(libFileName);
        if (File.fromUri(lib).existsSync()) {
          register(stage(lib));
          // The file itself: a directory dependency is hashed by entry names
          // only, so replacing the library in place would not re-run the hook.
          output.dependencies
            ..add(dir)
            ..add(lib);
          return;
        }
      }
    }

    // ---- 2. From source (host, inside the monorepo) ----------------------
    // lib/edge/dart/ -> the Cargo workspace root is three levels up.
    final workspaceRoot = input.packageRoot.resolve('../../../');
    final inMonorepo = File.fromUri(
      workspaceRoot.resolve('lib/edge/ffi/Cargo.toml'),
    ).existsSync();
    // Only the host architecture: a universal macOS build also asks for the
    // other slice, which a host `cargo build` cannot produce.
    if (targetOS == OS.current && arch == Architecture.current && inMonorepo) {
      final dylib = await _buildFromSource(workspaceRoot, libFileName);
      register(dylib);
      // Re-run when any build input changes, or `dart test` silently passes
      // against a stale library. Directory dependencies are hashed by their
      // entry names only, so register every workspace source file cargo read
      // (its dep-info file), plus the manifests and lockfile.
      for (final file in _workspaceSources(workspaceRoot, dylib)) {
        output.dependencies.add(file);
      }
      for (final manifest in Directory.fromUri(workspaceRoot.resolve('lib/'))
          .listSync(recursive: true)
          .whereType<File>()
          .where((f) => f.path.endsWith('${separator}Cargo.toml'))) {
        output.dependencies.add(manifest.uri);
      }
      output.dependencies
        ..add(workspaceRoot.resolve('Cargo.toml'))
        ..add(workspaceRoot.resolve('Cargo.lock'));
      return;
    }

    // ---- 3. Download the pinned prebuilt from the release ----------------
    if (dirName != null && _sha256.containsKey(dirName)) {
      final dir = await _download(
        _cacheBase() ?? Directory.fromUri(input.outputDirectoryShared),
        dirName,
        libFileName,
      );
      register(stage(dir.resolve(libFileName)));
      output.dependencies.add(dir);
      return;
    }

    // ---- 4. A target we do not ship --------------------------------------
    final slice = '$targetOS/$arch${iosSdk == null ? '' : '/$iosSdk'}';
    // Two slices Flutter requests by default are skipped WITHOUT an asset,
    // because failing them would fail the whole app build, including the
    // slices we do ship:
    //
    //  * 32-bit ARM Android (armeabi-v7a) is in `flutter build apk`'s default
    //    ABI set; the engine is 64-bit only.
    //  * The x86_64 Apple slices on an Apple Silicon host: a universal macOS
    //    build and the iOS simulator build ask for them there.
    //
    // On an Intel Mac the x86_64 slice is the one that runs, so it fails like
    // every other unsupported target instead of building an app with no engine.
    final defaultOnlySlice =
        (targetOS == OS.android && arch == Architecture.arm) ||
        ((targetOS == OS.macOS || targetOS == OS.iOS) &&
            arch == Architecture.x64 &&
            Abi.current() == Abi.macosArm64);
    if (defaultOnlySlice) {
      stderr.writeln(
        'qdrant_edge: no native engine for $slice, skipping this slice. '
        'Code calling qdrant_edge on it will fail to load the library.',
      );
      return;
    }
    throw UnsupportedError(
      'qdrant_edge: no native engine for $slice. Prebuilt engines exist for '
      '${_sha256.keys.join(', ')}.',
    );
  });
}

/// Workspace source files the last `cargo build` of the cdylib read, from the
/// dep-info file cargo writes next to it (`<target>: <dep> <dep> …`, spaces in
/// paths escaped as `\ `). Files outside the workspace (the cargo registry)
/// never change in place and are left out.
Iterable<Uri> _workspaceSources(Uri workspaceRoot, Uri dylib) {
  final depInfo = File.fromUri(
    dylib.resolve(dylib.pathSegments.last.replaceFirst(RegExp(r'\.[^.]+$'), '.d')),
  );
  if (!depInfo.existsSync()) {
    throw Exception('cargo dep-info not found: ${depInfo.path}');
  }
  // Cargo writes physical paths; the package root may be reached through a
  // symlink (`/var` vs `/private/var` on macOS, a symlinked checkout).
  final root = Directory.fromUri(workspaceRoot).resolveSymbolicLinksSync();
  final firstLine = depInfo.readAsLinesSync().first;
  final deps = firstLine.substring(firstLine.indexOf(': ') + 2);
  final sources = deps
      .split(RegExp(r'(?<!\\) '))
      .map((p) => p.replaceAll(r'\ ', ' ').trim())
      .where((p) => p.isNotEmpty && p.startsWith(root))
      .map((p) => File(p).uri)
      .toList();
  if (sources.isEmpty) {
    // Registering nothing would make every later run skip the build and test
    // a stale library.
    throw Exception(
      'qdrant_edge: no workspace sources under $root in ${depInfo.path}',
    );
  }
  return sources;
}

/// `<os>_<arch>` subdir/archive stem; iOS distinguishes device vs simulator.
String? _dirName(OS os, Architecture arch, IOSSdk? iosSdk) {
  final archName = switch (arch) {
    Architecture.arm64 => 'arm64',
    Architecture.x64 => 'x86_64',
    _ => null,
  };
  if (archName == null) return null;
  return switch (os) {
    OS.iOS =>
      iosSdk == IOSSdk.iPhoneSimulator ? 'ios_sim_$archName' : 'ios_$archName',
    OS.macOS => 'macos_$archName',
    OS.android => 'android_$archName',
    OS.linux => 'linux_$archName',
    OS.windows => 'windows_$archName',
    _ => null,
  };
}

String _libFileName(OS os) => switch (os) {
  OS.macOS || OS.iOS => 'libqdrant_edge_ffi.dylib',
  OS.windows => 'qdrant_edge_ffi.dll',
  _ => 'libqdrant_edge_ffi.so',
};

/// Build the host cdylib from the Cargo workspace. `+nightly` (sysinfo needs
/// rustc >= 1.95) + `--no-default-features` (drop search_matrix, mobile parity).
Future<Uri> _buildFromSource(Uri workspaceRoot, String libFileName) async {
  // `+nightly` is a rustup directive only the rustup cargo shim understands.
  // Under an Xcode/Gradle build environment a Homebrew `cargo` is often first on
  // PATH and rejects it, so invoke the rustup cargo by absolute path and PREPEND
  // ~/.cargo/bin. Path-separator/home-var/exe-suffix are all platform-specific.
  final sep = Platform.pathSeparator;
  final home =
      Platform.environment['HOME'] ?? Platform.environment['USERPROFILE'] ?? '';
  final cargoBin = home.isEmpty ? '' : '$home$sep.cargo${sep}bin';
  final cargoExe = Platform.isWindows ? 'cargo.exe' : 'cargo';
  final rustupCargo =
      cargoBin.isNotEmpty && File('$cargoBin$sep$cargoExe').existsSync()
      ? '$cargoBin$sep$cargoExe'
      : 'cargo';
  final pathListSep = Platform.isWindows ? ';' : ':';
  final envPath = Platform.environment['PATH'] ?? '';
  final result = await Process.run(
    rustupCargo,
    ['+nightly', 'build', '--locked', '--no-default-features', '-p', 'qdrant-edge-ffi'],
    workingDirectory: workspaceRoot.toFilePath(),
    environment: {
      ...Platform.environment,
      'PATH': cargoBin.isEmpty ? envPath : '$cargoBin$pathListSep$envPath',
    },
  );
  if (result.exitCode != 0) {
    throw Exception(
      'cargo build -p qdrant-edge-ffi failed (exit ${result.exitCode}):\n'
      '${result.stderr}',
    );
  }
  final dylib = workspaceRoot.resolve('target/debug/$libFileName');
  if (!File.fromUri(dylib).existsSync()) {
    throw Exception('expected cdylib not produced: ${dylib.toFilePath()}');
  }
  return dylib;
}

/// Per-user cache root for downloaded prebuilts, shared across projects, or
/// null when the environment names no home directory (some CI containers and
/// sandboxes); the caller then caches per project.
Directory? _cacheBase() {
  final env = Platform.environment;
  if (Platform.isWindows) {
    final local = env['LOCALAPPDATA'];
    if (local != null && local.isNotEmpty) {
      return Directory('$local\\qdrant_edge\\native');
    }
    final profile = env['USERPROFILE'];
    if (profile == null || profile.isEmpty) return null;
    return Directory('$profile\\AppData\\Local\\qdrant_edge\\native');
  }
  final home = env['HOME'];
  if (home == null || home.isEmpty) return null;
  if (Platform.isMacOS) {
    return Directory('$home/Library/Caches/qdrant_edge/native');
  }
  return Directory('$home/.cache/qdrant_edge/native');
}

/// Download `qdrant-edge-ffi-<dirName>.tar.gz` from the release, verify its
/// SHA256, extract into a cache dir, and return that dir. The cache dir is keyed
/// by the pinned SHA256 as well as the tag, so a cache hit is always the archive
/// this version pins, even if a tag was ever re-cut.
Future<Uri> _download(Directory cache, String dirName, String libFileName) async {
  final want = _sha256[dirName]!;
  final root = Directory('${cache.path}$separator$_releaseTag');
  final target = Directory(
    '${root.path}$separator$dirName-${want.substring(0, 16)}',
  );
  if (File('${target.path}$separator$libFileName').existsSync()) {
    return target.uri;
  }
  root.createSync(recursive: true);

  final archive = 'qdrant-edge-ffi-$dirName.tar.gz';
  final url = '$_releaseBase/$archive';
  final tmpArchive = File('${root.path}$separator.dl-$dirName-$pid.tar.gz');
  stderr.writeln('qdrant_edge: downloading $archive …');

  final client = HttpClient()
    ..connectionTimeout = const Duration(seconds: 30)
    // Hooks receive the proxy variables; use them explicitly.
    ..findProxy = HttpClient.findProxyFromEnvironment;
  final sink = tmpArchive.openWrite();
  try {
    final req = await client.getUrl(Uri.parse(url));
    final resp = await req.close().timeout(const Duration(seconds: 60));
    if (resp.statusCode != 200) {
      throw Exception('qdrant_edge: download failed (HTTP ${resp.statusCode}) $url');
    }
    // `pipe` drains the response into the sink and closes it on completion.
    await resp.pipe(sink).timeout(const Duration(minutes: 5));
  } catch (_) {
    // A partial/failed download (HTTP error, timeout, connection reset) must not
    // leave a truncated archive behind. Abort the connection first: after a
    // timeout `pipe` still owns the sink, and closing a bound sink throws a
    // StateError that would replace the real error.
    client.close(force: true);
    try {
      await sink.close();
    } catch (_) {
      // Already closed or still bound; the file is deleted either way.
    }
    try {
      if (tmpArchive.existsSync()) tmpArchive.deleteSync();
    } on FileSystemException {
      // Still open on Windows; the next attempt overwrites it.
    }
    rethrow;
  } finally {
    // force: true aborts a still-open connection instead of waiting on it.
    client.close(force: true);
  }

  final got = sha256.convert(await tmpArchive.readAsBytes()).toString();
  if (got != want) {
    if (tmpArchive.existsSync()) tmpArchive.deleteSync();
    throw Exception(
      'qdrant_edge: checksum mismatch for $archive\n  expected $want\n  got      $got',
    );
  }

  // Extract into a sibling temp dir on the same filesystem, then atomically
  // rename into place — a torn extract never leaves a half-populated target.
  final tmpDir = Directory('${root.path}$separator.ex-$dirName-$pid');
  if (tmpDir.existsSync()) tmpDir.deleteSync(recursive: true);
  tmpDir.createSync(recursive: true);
  try {
    final r = await Process.run(_tar(), [
      '-xzf',
      tmpArchive.path,
      '-C',
      tmpDir.path,
    ]);
    if (r.exitCode != 0) {
      throw Exception('qdrant_edge: extract failed for $archive: ${r.stderr}');
    }
    if (target.existsSync()) target.deleteSync(recursive: true);
    tmpDir.renameSync(target.path);
  } finally {
    if (tmpDir.existsSync()) tmpDir.deleteSync(recursive: true);
  }
  tmpArchive.deleteSync();
  return target.uri;
}

final String separator = Platform.pathSeparator;

/// `tar` to extract with. On Windows the system's bsdtar, by full path: a GNU
/// tar earlier on PATH (MSYS, Cygwin) reads `C:\…` as a remote host.
String _tar() {
  if (!Platform.isWindows) return 'tar';
  final root = Platform.environment['SystemRoot'] ?? r'C:\Windows';
  final system = '$root\\System32\\tar.exe';
  return File(system).existsSync() ? system : 'tar';
}
