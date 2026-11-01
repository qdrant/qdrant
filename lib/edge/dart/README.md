# Qdrant Edge — Dart & Flutter

On-device vector search for Dart and Flutter, powered by
[Qdrant Edge](https://qdrant.tech/documentation/edge/) — the Qdrant search
engine running **in-process**, no server and no network.

The SDK is a thin layer over the UniFFI binding generated from the shared
`qdrant-edge-ffi` Rust crate — the **same crate** the Swift and Kotlin SDKs bind,
so the three stay in lock-step by construction. Like the mobile SDKs, it builds
`--no-default-features`, so the O(n²) `search_matrix` op is off the surface.

## Quick start

```dart
import 'dart:io';

import 'package:qdrant_edge/qdrant_edge.dart';

final dir = Directory.systemTemp.createTempSync('qdrant_edge');

final shard = EdgeShard.load(
  path: dir.path,
  config: EdgeConfig(
    vectorData: {'': VectorDataConfig(size: 4, distance: Distance.dot)},
  ),
);

final op = UpdateOperation.upsertPoints(points: [
  Point(id: NumIdPointId(1), vector: SingleVector([1, 0, 0, 0])),
]);
shard.update(operation: op);
// The operation holds the whole batch in native memory, which the Dart GC
// does not see; free it as soon as it has been applied.
op.dispose();

final hits = shard.search(request: SearchRequest(
  query: NearestQuery(vector: DenseNamedVector([1, 0, 0, 0]), using: null),
  limit: 10,
));

// Flush and release the shard's files and lock.
shard.unload();
shard.dispose();
```

See `example/` for a runnable demo and `test/` for load → upsert → search,
persistence, and error-path coverage.

## Public API

`lib/qdrant_edge.dart` re-exports the generated binding through an explicit
`show` list, so the public surface is only the **domain** API (`EdgeShard`,
`EdgeConfig`, `Point`, the query/filter/vector/error types, …). The UniFFI
plumbing (`FfiConverter*`, `RustBuffer`, the call-status `*ErrorHandler`s, the
raw `@Native` C functions) lives in `lib/src/` and is **not** exported — it stays out of the package's semver
contract, so a future UniFFI bump can reshape it without breaking consumers.
(Swift achieves the same by demoting plumbing to `internal`; Dart does it for
free with a `show` list, which Kotlin/JVM cannot.)

## Error handling

**Every exception this API throws is catchable by name.** Two exception types
cross the boundary, and both are exported:

- **`EdgeException`** — a domain/engine error. Branch on its concrete subtypes:
  - `ShardClosedEdgeException` — the shard was unloaded; reopen it.
  - `ShardLockedEdgeException` — another handle in this or another process
    already holds the shard. An ordinary, **recoverable** condition — a second
    isolate, a store the app forgot to close — and explicitly *not*
    corruption: the check runs before the shard's files are read, so retry or
    close the other handle. `EdgeShard.load` throws it, for WAL and WAL-less
    shards alike, and so does `EdgeShard.create` when another handle opens
    the same empty path at the same moment.
  - `InvalidArgumentEdgeException` — bad input; fix it and retry.
  - `OperationExceptionEdgeException` — any other engine failure (I/O, etc.).
- **`UniffiInternalError`** — a Rust panic or a bindings/native protocol
  mismatch surfacing across the FFI boundary. It comes from the generated glue,
  not the engine, so it is *not* an `EdgeException` — but it is exported by name
  so it is still catchable. Treat it as a bug to report, not a data condition.

Two `Error`s come from Dart itself, before anything reaches the engine, and
mean a bug in the calling code rather than a data condition:

- `ArgumentError` — an argument does not fit its native type, e.g. a negative
  `limit`, `offset` or numeric point id. Nothing is sent to the engine and the
  shard stays usable. Dart's `int` is signed 64-bit, so numeric point ids are
  limited to `0 … 2^63 - 1`; use UUID ids beyond that. An `ArgumentError`
  saying a native function could not be resolved means the app was built
  without the engine for this device; see
  [Unsupported targets](#unsupported-targets).
- `StateError` — a method was called on an object after its `dispose()`.

A handle that is never `unload()`ed keeps the shard's lock until it is garbage
collected, and Dart does not promise to finalize objects when an isolate exits.
Call `unload()` (or `dispose()`) on every shard you open; otherwise a later
`load` of the same path in this process can keep failing with
`ShardLockedEdgeException`. A Flutter hot restart drops the old isolate without
finalizing its objects, so a shard opened before it stays open, and locked,
until the app process exits: after a hot restart, `load` of the same path
throws `ShardLockedEdgeException`. Stop and re-run the app instead of hot
restarting while a shard is open.

The lock is an advisory `flock` on the shard directory. Network file systems
without `flock` support (some NFS and SMB mounts) fail `load` and `create`
with `OperationExceptionEdgeException`; keep shards on local storage.

```dart
try {
  final shard = EdgeShard.load(path: dir.path, config: null);
  shard.update(operation: op);
} on ShardLockedEdgeException {
  // another handle has it open — retry, or close the other one first
} on EdgeException catch (e) {
  // any other domain/engine error
} on UniffiInternalError catch (e) {
  // a native panic / protocol mismatch — report it
}
```

To decide whether to open an existing store or start a fresh one *before*
calling `load`, use `probeShard(path:)` — it reports `none` / `loadable` /
`unreadable` by reading the directory, without opening the shard or taking its
lock:

- `none` → `EdgeShard.create(path:, config:)`, which also creates the directory
  (`load` requires it to exist).
- `loadable` → `EdgeShard.load(path:, config: null)`. It can still throw
  `ShardLockedEdgeException` if another handle holds the shard.
- `unreadable` → the shard will not load as it is; `reason` says why. A shard
  created with `walEnabled: false` whose `edge_config.json` was deleted is
  reported here, and loads with a config that sets `walEnabled: false`.

## Isolates and threads

Every call is a synchronous FFI call that runs on the calling isolate's thread
until the engine returns. On Android and iOS the Flutter UI isolate shares its
thread with the platform, so a long call there drops frames, and on Android
can trigger an ANR. Long calls are `load` (it replays the WAL), `optimize()`,
`flush()`, `updateFromSnapshot`, and large upserts or searches. In an app, own
the shard in one background isolate: open, use and unload it there, and send
only plain Dart data (query vectors, ids, results) between isolates.

`EdgeShard`, `UpdateOperation` and `Expression` cannot be sent to another
isolate; `SendPort.send` and `Isolate.run` reject them with an `ArgumentError`.
Each wraps a native reference that only the owning isolate may use and free.

Upgrading an app from `0.8.0-dev.*`: those versions never truncated the WAL,
and 0.9 replays the WAL on `load`. The first `load` after the upgrade can take
noticeably longer on a shard that was written to for a long time. It is one
more reason to open the shard off the UI isolate.

## Mobile notes

- Keep shards inside the app's own container. On iOS, a shard in an App Group
  container shared with an extension holds a file lock while the app is
  suspended, and iOS terminates apps that do.
- A shard created with `walEnabled: false` loses every operation since its last
  `flush()` if the process is killed. Call `flush()` when the app goes to the
  background (`AppLifecycleState.paused`).

## How it works

- **Bindings.** `lib/src/qdrant_edge_ffi.dart` is generated by
  `uniffi-bindgen-dart` in library mode (it reads the UniFFI metadata embedded in
  the compiled cdylib; the crate uses `setup_scaffolding!()`, so there is no UDL).
  It is **not committed** — it is ~17k lines of machine output, `.gitignore`d and
  regenerated by `./build.sh`. **Run `./build.sh` once after a fresh checkout**
  before `dart test` / `dart analyze` (the same way the Swift/Kotlin SDKs need
  their build script first). `uniffi.toml` pins the Dart package name so the
  generated `@Native` asset ids resolve. Only the small curated facade
  (`lib/qdrant_edge.dart`) is committed — it is the reviewed public surface.
- **Toolchain provenance.** The bindgen must be built from the **fork** that
  carries the uniffi-0.32 + library-mode-CLI + enum-collision-fix work (upstream
  `acterglobal/uniffi-dart` lacks it, pending PRs
  [#149](https://github.com/Uniffi-Dart/uniffi-dart/pull/149) /
  [#151](https://github.com/Uniffi-Dart/uniffi-dart/pull/151) /
  [#152](https://github.com/Uniffi-Dart/uniffi-dart/pull/152)):
  `github.com/DenisovAV/uniffi-dart` @ `edge-dart-090`. Point
  `UNIFFI_BINDGEN_DART` at that build. CI regenerates the binding and diffs the
  committed **facade** to guarantee the public surface stays in sync with the
  crate (the binding itself can't drift — it is always regenerated, never stored).
- **Native library.** The engine is delivered as a
  [Native Asset](https://dart.dev/interop/c-interop#native-assets): `dart test`,
  `dart run`, and `flutter test`/`flutter run` invoke `hook/build.dart`
  automatically — no `--enable-experiment` flag on Dart 3.12+.

## Distribution

`hook/build.dart` provisions the engine three ways, in priority order, so the
**same** hook serves both an in-tree developer and a pub.dev consumer:

1. **Local prebuilt** — `native/prebuilt/<os_arch>/`, or `<dir>/<os_arch>/`
   with the user-define below. A dev/CI override, e.g. dropping in a locally
   cross-built device library. Hooks do not see the shell's environment, so the
   override directory is set in the app's `pubspec.yaml`:
   ```yaml
   hooks:
     user_defines:
       qdrant_edge:
         prebuilt_dir: path/to/prebuilt
   ```
2. **From source (host, in-tree)** — when the target is the host OS and
   architecture *and* the Cargo workspace is present, build with
   `cargo +nightly` (needs the nightly toolchain + protobuf). This is the fast
   path while developing inside the monorepo; the hook re-runs when any
   workspace source file cargo compiled changes.
3. **Download** — otherwise fetch the per-platform, **SHA256-pinned** archive
   from the GitHub Release (`edge-dart-native-v$VERSION`) and cache it under the
   platform cache dir. This is how a consumer with no Rust toolchain, and every
   cross-compile (device/emulator) target, is served — the same shape the Kotlin
   AAR and Swift XCFramework releases use.

`.github/workflows/edge-dart-native.yml` builds the eight cdylibs (Linux
x86_64/arm64, Windows x86_64, macOS arm64, iOS arm64 device/simulator, Android
arm64/x86_64), packages each as `qdrant-edge-ffi-<os_arch>.tar.gz`, and publishes
them with a `checksums.txt`. Those SHA256 lines are pinned into the `_sha256`
table in `hook/build.dart`, so a tampered or truncated archive can never be
linked in. `make publish` runs `tool/check_publish.sh` first, which refuses
to publish unless the working tree is clean, the binding generator is the
commit CI pins, the pins match the release, and the release was built from the
same Rust sources as the commit being published. A release is never re-cut: the workflow
refuses to overwrite an existing tag, because published versions pin its
archives. A new native build gets a new `lib/edge/VERSION`.

### Unsupported targets

Targets without a prebuilt fail the build with an error naming the shipped
ones. Two slices Flutter requests by default are skipped with a warning
instead, so the rest of the app still builds: 32-bit ARM Android
(armeabi-v7a) and the x86_64 Apple slices on an Apple Silicon host. An app
built that way still installs on those devices and fails at its first
qdrant_edge call, so restrict the build to the supported slices:

- Android, in `android/app/build.gradle`:
  `defaultConfig { ndk { abiFilters 'arm64-v8a', 'x86_64' } }`.
- macOS, in the Xcode project: `ARCHS = arm64` (or
  `EXCLUDED_ARCHS = x86_64`), so the app does not launch on Intel Macs.

Building on an Intel Mac, or with a Dart SDK running under Rosetta, is not
supported: the host reports x86_64 and the macOS and iOS-simulator slices
fail the build.

Developing against the monorepo (a `path:` or `git:` dependency) builds the
engine from source for the host platform, which needs the Rust nightly
toolchain and `protoc`.

The engine is downloaded from the GitHub releases of
`github.com/DenisovAV/qdrant`, the fork that runs the release workflow, until
the SDK lands in `qdrant/qdrant`; every archive is verified against the SHA256
pinned in the published `hook/build.dart`.

The Linux libraries are built on Ubuntu 22.04 and need glibc 2.34 or newer
(Ubuntu 22.04, Debian 12, Fedora 35 and later).

A version is publishable to pub.dev as `qdrant_edge` once the native-prebuilt
release named by `_releaseTag` (`edge-dart-native-v<lib/edge/VERSION>`) is cut and
its checksums are pinned. Publishing
regenerates the binding first (`make publish` runs `tool/check_publish.sh`,
then `./build.sh`, then `dart pub publish`) so the generated glue lands in the archive even though it is
`.gitignore`d — `.pubignore` replaces `.gitignore` for pub and does not exclude
it. `native/` and the on-device harness stay out (a ~70 KB archive: the Dart glue
ships as source, the native engine is downloaded at build time, never bundled).

## Versioning

The plugin tracks the native `qdrant-edge` library — `lib/edge/VERSION`, the same
single source the Swift `edge-v$VERSION` tag and the Kotlin `VERSION_NAME` use — on
**major.minor**, the API/ABI contract shared across every Edge SDK (`0.9.x` speaks the
same surface on all platforms). `lib/edge/VERSION` and the `qdrant-edge-ffi` crate
version are kept identical — they *are* the native version. The pub.dev `version`
matches them on major.minor, but its patch floats independently: a Dart-only fix (a
regenerated binding, a `hook/build.dart` change, docs) can ship as a new patch without
cutting a fresh native release — it keeps downloading the prebuilt pinned by
`_releaseTag`. So `qdrant_edge 0.9.1` may wrap native `0.9.0`, exactly as Rust `-sys`
crates decouple their own version from the C library they bundle. `edge-dart.yml`
enforces this: `VERSION == crate` exactly, and both the `pubspec` version and the
hook's native pin must match `VERSION` on major.minor.
