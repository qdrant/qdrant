# Changelog

## 0.9.0-dev.1

Moves the SDK to Qdrant Edge 0.9.0 (native `edge-dart-native-v0.9.0`).

**If you are on 0.8.0-dev.1 to 0.8.0-dev.5, upgrade.** The 0.8.0 engine has
these defects, all reachable from the Dart API and all fixed in 0.9.0:

- `query()` ignored `offset`: it returned the first `offset + limit` points
  instead of skipping `offset`. With 5 points, `limit: 2, offset: 2` returned
  4 points on 0.8.0-dev.5 and returns the expected 2 on this version. Any
  pagination built on `query()` returned repeated and extra results.
  `queryGroups()` returned hits without the requested payload and vectors.
  (qdrant/qdrant#10778)
- `flush()` never acknowledged the persisted version, so the WAL was never
  truncated and the shard's `wal/` directory grew without bound.
  (qdrant/qdrant#11082)
- Loading a shard did not replay the WAL, so operations applied after the
  last segment flush were lost if the process died before the next flush.
  (qdrant/qdrant#11084)

Fixed in the SDK:

- An argument that fails to convert in Dart (for example `search` with a
  negative `limit`) no longer leaks a reference to the shard. The shard used
  to keep its file lock after `dispose()`, so reopening it threw
  `ShardLockedEdgeException` until the process exited.
- A second `EdgeShard.load` (or `create`) of a shard another handle holds now
  throws `ShardLockedEdgeException` before anything on disk is read: the
  engine locks the shard directory. Previously a shard created with
  `walEnabled: false` took no lock, so a second load succeeded and both
  handles wrote the same files; and a second load of a WAL shard could delete
  segment directories an `optimize()` on the first handle was still writing
  before it failed.
- A shard created with `walEnabled: false` whose `edge_config.json` is gone,
  or a shard whose `wal/` directory was deleted, now keeps applying updates
  after `load`. The engine started a fresh WAL numbered below the versions
  already stored, so updates and deletes of existing points were skipped
  while reporting success.
- Calling a method after `dispose()` throws `StateError`; it used to touch freed
  native memory. A second `dispose()` does nothing.
- `EdgeShard`, `UpdateOperation` and `Expression` can no longer be sent to
  another isolate (`ArgumentError`). A sent copy shared the native reference
  with the original, so a `dispose()` or finalizer on one side freed memory the
  other still used.
- Constructors and free functions, like methods, convert all arguments before
  taking any native reference, so an argument that fails to convert no longer
  leaks references to object arguments.
- The first call into the library checks that the binding and the native
  library were built from the same API, and throws `UniffiInternalError` if not.
- The build hook keys its download cache by the pinned checksum, so a version
  never picks up an engine cached for different archive contents. The cached
  library itself is not re-hashed on every build.
- A download that times out reports the timeout and leaves no partial archive.
- The download uses the HTTP proxy from the environment, falls back to a
  per-project cache when no home directory is set, and extracts with the
  system `tar` on Windows.
- The prebuilt engines are built with the `release-mobile` profile (thin LTO,
  stripped symbols), like the Swift and Kotlin SDKs; the 0.8.0 libraries
  shipped unstripped.

Changed:

- Breaking: `probeShard`, `ShardProbe` and `EdgeShardPresence` are removed.
  `EdgeShard.load(path:, config: null)` answers the same question with its
  own errors, so the answer cannot disagree with what `load` does: it returns
  the shard; throws `UnreadableEdgeException` (new) when data is present but
  will not open — never recreate over it; throws `ShardLockedEdgeException`
  when another handle holds it; and throws `OperationExceptionEdgeException`
  whose `reason` contains `no segments were loaded` when nothing is stored
  there. A `config` that contradicts the stored segments is
  `InvalidArgumentEdgeException`. This is the shape agreed for the shared
  `qdrant-edge-ffi` crate in qdrant/qdrant#10336. Migration:
  `probeShard(...).presence == none` → catch the "no segments were loaded"
  `OperationExceptionEdgeException` from `load`; `unreadable` → catch
  `UnreadableEdgeException`; `loadable` → `load` returns.
- Breaking: `EdgeShard.queryBatch` takes a `QueryBatchRequest`
  (`queryBatch(request: QueryBatchRequest(queries: [...]))`) instead of
  `queryBatch(requests: [...])`, following the FFI change in
  qdrant/qdrant#10641.
- A target with no prebuilt engine now fails the build instead of building an
  app with no engine. 32-bit ARM Android and the x86_64 Apple slices on Apple
  Silicon hosts, which Flutter builds by default, are still skipped with a
  warning.
- The local prebuilt override is the `prebuilt_dir` user-define. The
  `QDRANT_EDGE_PREBUILT_DIR` environment variable it replaces never reached the
  hook.
- `EdgeShard.load` requires an existing directory, as its documentation now
  says; `EdgeShard.create` creates a missing one.
- A failed `updateFromSnapshot` that gets past reading the archive leaves the
  shard unloaded and releases its lock; reopen it with `load`.

Upgrading from 0.8.0-dev.*: those versions never truncated the WAL, and this
version replays it on `load`, so the first `load` after the upgrade can take
longer on a shard written to for a long time. Open shards off the UI isolate;
see "Isolates and threads" in the README.

New from the 0.9.0 engine: BM25 scoring over text indexes (`Bm25Params`,
`TextScoringQuery` and related types) and the `substring` match condition
(`SubstringMatch`).

## 0.8.0-dev.5

Allow `code_assets` 2.x: the constraint widens from `^1.2.0` to
`">=1.2.0 <3.0.0"`. This lets an app resolve when something else in its
dependency graph needs `code_assets ^2.0.0` (for example `native_toolchain_c`
≥0.19.4, `objective_c` ≥9.6.0, `ffigen` ≥22, or `flutter_soloud` 5.x), while
still installing on Flutter 3.44 (where `meta` is pinned to 1.18.0 and pub
picks `code_assets` 1.2.x). No code, API or native-library changes — the
prebuilt binaries remain `edge-dart-native-v0.8.0`.

## 0.8.0-dev.4

**Upgrade if you use 0.8.0-dev.1, 0.8.0-dev.2 or 0.8.0-dev.3.** Those versions
leak native memory on almost every call: the process grows with use and the
memory comes back only when it exits. Stored data is not affected. To fix it,
change the dependency to `qdrant_edge: 0.8.0-dev.4`; no code changes are needed.
The public API and the native libraries (`edge-dart-native-v0.8.0`) are the
same as in 0.8.0-dev.3.

What leaked:

- **Results and typed errors.** Every call that returns a record, list or
  string (`search`, `query`, `queryBatch`, `queryGroups`, `retrieve`,
  `scroll`, `facet`, `info`, `config`, `path`, `snapshotManifest`,
  `probeShard`), and every `EdgeException`, left its serialized result in
  Rust-owned memory that the Dart GC cannot see. The leak grows with the size
  of the result.
- **Arguments.** Every call taking a record, list, string or optional argument
  (requests, points, filters, configs) left the native copy made while
  converting it.
- **Nested enums.** Queries using `SampleScoringQuery`, and text indexes with a
  `stemmer` or `stopwords` setting (when created or read back, for example
  through `info()`), also allocated a small Rust buffer per conversion that was
  never freed.

Measured on macOS arm64, process RSS after 20,000 searches (limit 10, returning
128-dim vectors and 211-byte JSON payloads, 2,000 points, after 2,000
warm-up searches), two runs each:
0.8.0-dev.3 grew by 590,217,216 and 590,299,136 bytes (29,511 and 29,515 bytes
per search); 0.8.0-dev.4 grew by 163,840 and 409,600 bytes.

The fixes come from the uniffi-dart generator (Uniffi-Dart/uniffi-dart#179,
#180 and #182, still open upstream), applied to the generator fork this package
uses; the binding is regenerated with them.

Known issue, not fixed here (present since 0.8.0-dev.1): if converting an
argument throws in Dart, for example `search` with a negative `limit`, the
shard reference taken for that call is never released, so the shard keeps its
file lock after `dispose()` and reopening it throws `ShardLockedEdgeException`
until the process exits. `unload()` still releases the lock.

## 0.8.0-dev.3

Consumer-driven DX fixes for opening, distinguishing, and clearing shards. All
additive — no change to existing `load`/`search`/`update`/filter semantics.

- **`probeShard(path:)`** — classify a directory as `none` / `loadable` /
  `unreadable` (with a reason) *before* opening it, without taking the WAL lock.
  It answers "is there a store here, and would it open?" so consumers no longer
  reverse-engineer the on-disk layout. In particular, a shard whose
  `edge_config.json` is missing but whose segments are intact now reports
  `loadable` (it reopens fine) — removing the "full corpus seen as empty" class
  of bugs.
- **`ShardLockedEdgeException`** — opening a shard another handle already holds
  now throws this distinct, recoverable exception instead of a generic
  `OperationExceptionEdgeException`, so callers can retry or report "already
  open" without substring-matching a message.
- **`EdgeShard.clear()`** — deletes every point in place (the shard stays open,
  its config and indexes intact); the discoverable form of the match-all
  `deletePointsByFilter(Filter())` idiom.
- **Exception guarantee**: `UniffiInternalError` is now exported, so every
  exception crossing the API is catchable by name — an `EdgeException`
  (domain/engine error) or `UniffiInternalError` (a Rust panic or bindings/native
  protocol mismatch). Documented under README "Error handling". The
  previously-unexported top-level `unpackSnapshot` is exported too.
- Re-cut of the `edge-dart-native-v0.8.0` prebuilts to carry the new FFI symbols
  (checksums updated).

## 0.8.0-dev.2

- Fix: normalize the iOS native binary's minimum OS version to 13.0 to avoid
  App Store rejection (ITMS-90208). Flutter's Native Assets wrapper hardcodes
  `MinimumOSVersion 13.0` into the generated `.framework` Info.plist, and App
  Store Connect rejects a nested framework whose binary minos (15.0) exceeds
  its own plist. The prebuilt iOS dylibs now carry minos 13.0.

## 0.8.0

First release of the Qdrant Edge SDK for Dart & Flutter — on-device vector
search with no server and no network, powered by the shared `qdrant-edge-ffi`
Rust crate (the same crate the Swift and Kotlin SDKs bind) through UniFFI.

- **Full shard API**: load/persist, upsert, search, and query (nearest,
  RRF/DBSF fusion, MMR, formula, order-by, sample), retrieve / scroll / count /
  facet / groups, the filter-condition set, config setters, and snapshot
  manifest.
- **Curated public surface** via `package:qdrant_edge/qdrant_edge.dart` — the
  UniFFI plumbing stays out of the semver contract behind an explicit `show`
  list.
- **Native engine as a Native Asset**: the build hook resolves a per-platform,
  SHA256-pinned prebuilt cdylib (Linux x86_64/arm64, Windows x86_64, macOS
  arm64, iOS arm64 device/simulator, Android arm64/x86_64), downloading it from
  the release when no local build is available — so consumers need no Rust
  toolchain.
- Built `--no-default-features` (no `search_matrix`), matching the Swift and
  Kotlin mobile SDKs.
