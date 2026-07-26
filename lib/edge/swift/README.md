# Qdrant Edge — Swift

Swift bindings for [Qdrant Edge](https://qdrant.tech/documentation/edge/edge-quickstart/),
the embeddable vector search engine. Ships as an **XCFramework** with
pre-compiled static libraries for iOS, macOS, and (optionally) tvOS/visionOS.

## Supported slices

| Slice                            | Architectures    | Purpose                              |
|----------------------------------|------------------|--------------------------------------|
| `ios-arm64`                      | arm64            | Physical iOS devices                 |
| `ios-arm64_x86_64-simulator`     | arm64 + x86_64   | iOS Simulator (Apple Silicon/Intel)  |
| `macos-arm64_x86_64`             | arm64 + x86_64   | Native macOS apps                    |
| `tvos-arm64`                     | arm64            | Physical Apple TV                    |
| `tvos-arm64_x86_64-simulator`    | arm64 + x86_64   | tvOS Simulator                       |
| `visionos-arm64`                 | arm64            | Apple Vision Pro                     |
| `visionos-arm64-simulator`       | arm64            | visionOS Simulator                   |

The **shipped package declares only iOS + macOS** (`Package.swift`), and the
default `make build` produces only those three slices. The tvOS/visionOS rows
above are produced by `make build-all` only (Rust tier-3 targets, nightly +
`-Z build-std`) and are **not** declared in `Package.swift` in this release —
add them back once their slices ship. (All targets build on nightly, selected by
`rust-toolchain.toml`, since the workspace uses unstable std features.)

## Quick start

```bash
make setup      # Install Rust, protobuf, cross-compilation targets
make build      # Build the XCFramework (release)
make size       # Show XCFramework size breakdown
```

## Integration

Add the package to your Swift project:

```swift
.package(path: "path/to/lib/edge/swift")
```

Import and use:

```swift
import QdrantEdge

let shard = try EdgeShard.load(path: dataDir, config: config)
```

See `example/` for a complete demo app.

### Distribution

This package lives in a subdirectory of the qdrant monorepo, so **this manifest
is the development/CI manifest** and is consumed via a **local `path:`
dependency** (as shown above). It is intentionally not the public distribution
manifest — here's why, and how public distribution is intended to work.

**The constraint.** SwiftPM resolves a `.package(url:)` dependency only against a
`Package.swift` at a repository **root**. There is no subdirectory selector for
git URLs — the feature request ([swift-package-manager#5768](https://github.com/swiftlang/swift-package-manager/issues/5768))
was closed without implementation. So `.package(url: "…/qdrant")` cannot reach
`lib/edge/swift`, regardless of how the binary target is configured.

**How comparable projects handle it.** Every native-core library in the same
situation — a Swift package that is a *sub-part* of a larger polyglot repo —
ships it from a small, dedicated Swift **release repo** (whose `Package.swift`
is at *its* root), developed from the main monorepo:

| Project | Release repo |
|---|---|
| DuckDB (C++) | `duckdb/duckdb-swift` |
| Turso / libSQL (Rust) | `tursodatabase/libsql-swift` |
| Mozilla app-services (Rust) | `mozilla/rust-components-swift` |
| Matrix (Rust) | `matrix-org/matrix-rust-components-swift` |
| Realm, ObjectBox | dedicated Swift repos |

(Projects that consume from a repo root instead — e.g. `unum-cloud/usearch` —
can do so only because the whole repo *is* the library; that doesn't apply when
the repo root is a large server product.)

**The three options, and the tradeoff.**

1. **Dedicated release repo** (recommended, the industry norm above): a thin
   `qdrant-edge-swift` repo, auto-updated from this monorepo, commits the
   generated bindings and points its XCFramework `binaryTarget` at a
   GitHub-release artifact published here. Cost: one extra (generated) repo.
2. **Orphan branch in `qdrant/qdrant`** (no second repo): a `edge-swift-release`
   branch whose *root* holds the manifest + bindings, consumed via
   `.package(url: "…/qdrant", branch:/exact:)`. Cost: consumers clone the large
   monorepo to resolve, and Swift version tags share the server's tag namespace.
3. **Swift Package Registry** (no git-root constraint): publish versioned
   archives to a registry. Cost: registry infrastructure + consumer opt-in;
   still-nascent ecosystem support.

Option 1 is the plan; standing up the release repo is tracked as follow-up work.
`release-xcframework.sh` already builds and checksums the distributable
artifact, and the `QDRANT_EDGE_RELEASE` url+checksum mode in `Package.swift`
exists for that release repo's manifest and for CI verification.

> **Note for the Kotlin/Android SDK:** this is a Swift-specific constraint.
> Maven/Gradle coordinates are location-independent, so the Android SDK publishes
> its AAR to Maven Central directly from `lib/edge/android/` in this monorepo —
> no separate repo, no root requirement.

## Project layout

```
swift/
├── build-xcframework.sh       Cross-compile Rust + generate Swift bindings
├── demote-ffi-internals.sh    Post-process QdrantEdge.swift (see below)
├── Makefile                   setup / build / build-all / size / clean
├── Package.swift              SPM manifest
├── example/                   Swift example app
└── out/                       Build output (gitignored)
    ├── QdrantEdge.xcframework
    └── swift-bindings/        QdrantEdge.swift + QdrantEdgeFFI.h
```

The Rust crate and `uniffi-bindgen` CLI live under `lib/edge/ffi/`.

## Public API

UniFFI emits `QdrantEdge.swift` with a mix of user-facing domain types
(`EdgeShard`, `Point`, `Query`, `Filter`, …) and FFI plumbing
(`FfiConverter*`, `RustBuffer`, `Uniffi*`, `*_lift`/`*_lower`, …). After
generation, `demote-ffi-internals.sh` rewrites the top-level plumbing
declarations from `public` to `internal`, so `import QdrantEdge` surfaces the
real domain API in Xcode's autocomplete and Quick Help (with the caveat below).

The rewrite is safe because `QdrantEdge.swift` compiles into a single
Swift module; the plumbing is only referenced from within that file, and
`internal` keeps those references valid while hiding them from consumers.

Every public type and method carries doc comments authored in Rust that
UniFFI propagates to Swift Quick Help. ⌥-click in Xcode for summaries,
error notes, and examples.

A handful of UniFFI object-lifecycle internals (`init(unsafeFromHandle:)`,
`init(noHandle:)`, `NoHandle`, `uniffiCloneHandle()`) stay technically `public` — the demote pass only
rewrites top-level declarations, and these are indented members UniFFI requires
for its `FfiConverter` conformance. UniFFI tags them
`@_documentation(visibility: private)`, so they don't appear in autocomplete or
Quick Help; treat them as reserved, not API. Because the public Swift surface is
generated, a UniFFI upgrade can reshape it — treat UniFFI version bumps as semver
events for this package.

## Threading

All `EdgeShard` calls are **synchronous and blocking** — `search`, `query`,
`scroll`, `upsert`, etc. run on the calling thread. **Never call them on the
main thread**; a large search will freeze the UI.

The SDK does not impose a thread for you (you choose where the work runs). The
idiomatic way to run a call off the main thread with Swift concurrency:

```swift
let hits = try await Task.detached(priority: .userInitiated) {
    try shard.search(request: request)
}.value
```

If you wrap the shard in an `actor` (a natural pattern for a database), the
generated value types (`Point`, `Filter`, `SearchRequest`, …) are `Sendable`,
so they cross the actor boundary cleanly under Swift 6 strict concurrency.

## Error handling

Fallible calls throw `EdgeError`, a branchable enum so you can react to the
error category:

- `.ShardClosed` — the shard was unloaded; reopen it via `EdgeShard.load`.
- `.InvalidArgument(reason)` — host-supplied input was invalid (bad UUID,
  out-of-range vector size, unsupported config, …); fix the input and retry.
- `.OperationError(reason)` — any other engine failure (I/O, missing payload
  index, dimension mismatch, …).

(The cases are PascalCase: UniFFI does not lower-case the variants of an
`Error`-typed enum the way it does for a plain `Enum`.)

```swift
do {
    let shard = try EdgeShard.load(path: dataDir, config: config)
    try shard.update(operation: upsert)
} catch EdgeError.ShardClosed {
    // reopen the shard
} catch let EdgeError.InvalidArgument(reason) {
    print("Bad input: \(reason)")
} catch let error as EdgeError {
    print("Engine error: \(error)")
}
```
