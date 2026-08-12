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
///  1. **Local prebuilt** — `$QDRANT_EDGE_PREBUILT_DIR/<os_arch>/` or
///     `native/prebuilt/<os_arch>/` inside the package. A dev/CI override.
///  2. **From source (host, in-tree)** — when the target is the host OS and the
///     Cargo workspace is present (developing inside the qdrant monorepo), build
///     the cdylib with `cargo +nightly` for fast incremental iteration.
///  3. **Download** — otherwise fetch the per-platform, SHA256-pinned archive
///     from the GitHub Release and cache it. This is how a pub.dev consumer (no
///     Rust toolchain, no workspace) and cross-compile targets are served.
///
/// A target that matches none of these fails loudly rather than shipping the
/// wrong platform's library.

/// Release that carries the prebuilt cdylibs (published by edge-dart-native.yml).
///
/// Pre-merge the archives live on the fork that runs the release workflow; the
/// base moves to `github.com/qdrant/qdrant` once the SDK lands upstream and the
/// release is cut there. Pointing at qdrant/qdrant before that release exists
/// would 404 every download, so the fork URL here is deliberate for now.
const _releaseTag = 'edge-dart-native-v0.8.0';
const _releaseBase =
    'https://github.com/DenisovAV/qdrant/releases/download/$_releaseTag';

/// SHA256 of each `qdrant-edge-ffi-<os_arch>.tar.gz` in the release — the pin
/// that makes the download tamper-evident. Populated from the release's
/// `checksums.txt` (produced by edge-dart-native.yml). Empty until the first
/// release is cut; download provisioning is inert until then.
const _sha256 = <String, String>{
  'linux_x86_64':
      '25b5132e1978e5954123f82270fe2d7a0782e9a54ffdbaebe2b29ab7e2e8a28d',
  'linux_arm64':
      'a11ccf7f7586f5585dd494ab7111557096d5b4cecdfc1b1226ca207fffba863e',
  'windows_x86_64':
      '738e8639e04653683f7292351742158123ca1ce28fb8078eb113ad6a935bdc2e',
  'macos_arm64':
      '9300e7086c5d45c50c19fec3bbc79f73e024f449676253bf2ef66fab799e9ccd',
  'ios_arm64':
      '24336ca17631f6d7b23a06cfd671210366788807f2504904a3063a3a34f79e2c',
  'ios_sim_arm64':
      'bd008e0b7329c293a7e51337606d02c6f61ceef3b621beb6d398465f6bc983a6',
  'android_arm64':
      '3939f45147e29f6b7d1e6438ba7d52eb6b7a6ba33a419b5d07f887a3b9c1ed92',
  'android_x86_64':
      '11408ca9a0a3e148b2553b7d1571fb541d36e7fd39aafb21d79f1bfb9ba59a26',
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
      final override = Platform.environment['QDRANT_EDGE_PREBUILT_DIR'];
      final dirs = <Uri>[
        if (override != null && override.isNotEmpty)
          Directory(override).absolute.uri.resolve('$dirName/'),
        input.packageRoot.resolve('native/prebuilt/$dirName/'),
      ];
      for (final dir in dirs) {
        final lib = dir.resolve(libFileName);
        if (File.fromUri(lib).existsSync()) {
          register(stage(lib));
          output.dependencies.add(dir);
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
    if (targetOS == OS.current && inMonorepo) {
      register(await _buildFromSource(workspaceRoot, libFileName));
      // Re-run when ANY build input changes — `qdrant-edge-ffi` compiles in the
      // edge/segment/shard/sparse crates, so editing those sources, their
      // manifests (a dep bump changes the build without touching src/), or the
      // lockfile must invalidate the cached dylib or `dart test` silently
      // passes stale.
      for (final rel in const [
        'Cargo.toml',
        'Cargo.lock',
        'lib/edge/ffi/src/',
        'lib/edge/ffi/Cargo.toml',
        'lib/edge/src/',
        'lib/edge/Cargo.toml',
        'lib/segment/src/',
        'lib/segment/Cargo.toml',
        'lib/shard/src/',
        'lib/shard/Cargo.toml',
        'lib/sparse/src/',
        'lib/sparse/Cargo.toml',
      ]) {
        output.dependencies.add(workspaceRoot.resolve(rel));
      }
      return;
    }

    // ---- 3. Download the pinned prebuilt from the release ----------------
    if (dirName != null && _sha256.containsKey(dirName)) {
      final dir = await _download(dirName, libFileName);
      register(stage(dir.resolve(libFileName)));
      output.dependencies.add(dir);
      return;
    }

    throw UnsupportedError(
      'qdrant_edge: cannot provision the engine for $targetOS/$arch'
      '${iosSdk == null ? '' : '/$iosSdk'}. No local prebuilt under '
      'native/prebuilt/$dirName (or \$QDRANT_EDGE_PREBUILT_DIR), not a host '
      'source build, and no pinned release archive for "$dirName" '
      '(release $_releaseTag). See the README "Distribution" section.',
    );
  });
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

/// Platform cache root for downloaded prebuilts.
Directory _cacheBase() {
  final env = Platform.environment;
  final home = env['HOME'] ?? env['USERPROFILE'] ?? '';
  if (Platform.isWindows) {
    final local = env['LOCALAPPDATA'] ?? '$home\\AppData\\Local';
    return Directory('$local\\qdrant_edge\\native');
  }
  if (Platform.isMacOS) {
    return Directory('$home/Library/Caches/qdrant_edge/native');
  }
  return Directory('$home/.cache/qdrant_edge/native');
}

/// Download `qdrant-edge-ffi-<dirName>.tar.gz` from the release, verify its
/// SHA256, extract into a versioned cache dir, and return that dir. Cached after
/// the first fetch (keyed by release tag → a version bump uses a fresh dir).
Future<Uri> _download(String dirName, String libFileName) async {
  final root = Directory('${_cacheBase().path}$separator$_releaseTag');
  final target = Directory('${root.path}$separator$dirName');
  if (File('${target.path}$separator$libFileName').existsSync()) {
    return target.uri;
  }
  root.createSync(recursive: true);

  final archive = 'qdrant-edge-ffi-$dirName.tar.gz';
  final url = '$_releaseBase/$archive';
  final tmpArchive = File('${root.path}$separator.dl-$dirName-$pid.tar.gz');
  stderr.writeln('qdrant_edge: downloading $archive …');

  final client = HttpClient()..connectionTimeout = const Duration(seconds: 30);
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
    // leave a truncated archive behind. Close the write sink first so nothing is
    // still writing when we delete, then drop the file.
    await sink.close().catchError((Object _) {});
    if (tmpArchive.existsSync()) tmpArchive.deleteSync();
    rethrow;
  } finally {
    // force: true aborts a still-open connection instead of waiting on it.
    client.close(force: true);
  }

  final got = sha256.convert(await tmpArchive.readAsBytes()).toString();
  final want = _sha256[dirName];
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
    final r = await Process.run('tar', ['-xzf', tmpArchive.path, '-C', tmpDir.path]);
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
