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

`make build` covers iOS + macOS. `make build-all` adds tvOS and visionOS;
those are Rust tier-3 targets and require the nightly toolchain.

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
generation, `demote-ffi-internals.sh` rewrites the plumbing declarations
from `public` to `internal`, so `import QdrantEdge` in consumer code only
surfaces the real domain API.

The rewrite is safe because `QdrantEdge.swift` compiles into a single
Swift module; the plumbing is only referenced from within that file, and
`internal` keeps those references valid while hiding them from consumers.

Every public type and method carries doc comments authored in Rust that
UniFFI propagates to Swift Quick Help. ⌥-click in Xcode for summaries,
error notes, and examples.
