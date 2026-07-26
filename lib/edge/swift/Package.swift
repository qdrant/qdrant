// swift-tools-version:5.9
import PackageDescription
import Foundation

// DISTRIBUTION MODEL (why this manifest defaults to a local path)
// ----------------------------------------------------------------
// This manifest is the DEVELOPMENT/CI manifest for the package as it lives in
// the qdrant monorepo at `lib/edge/swift/`. It is NOT the public distribution
// manifest, by necessity, not oversight:
//
// SwiftPM resolves a `.package(url:)` dependency ONLY against a `Package.swift`
// at a repository ROOT (swiftlang/swift-package-manager#5768, closed without
// implementation — there is no `subdir:` for git URLs). A package in a
// subdirectory of a large polyglot monorepo therefore cannot be consumed over
// `.package(url: "…/qdrant")` at all.
//
// Every comparable native-core project in the same position solves this with a
// small, dedicated Swift release repo (whose Package.swift is at ITS root),
// developed from the main monorepo: DuckDB (`duckdb/duckdb-swift`), Turso/libSQL
// (`tursodatabase/libsql-swift`), Mozilla (`mozilla/rust-components-swift`),
// Matrix (`matrix-org/matrix-rust-components-swift`), Realm, ObjectBox. The
// release repo commits the generated bindings and points its XCFramework
// `binaryTarget` at a GitHub-release artifact published from this monorepo.
// (Alternatives that avoid a second repo — an orphan branch in qdrant/qdrant,
// or a Swift Package Registry — carry worse tradeoffs: monorepo-clone bloat +
// tag-namespace collisions, or registry infrastructure. See README "Distribution".)
// Standing up that release repo is tracked as follow-up work.
//
// The two modes below therefore serve: local `path:` for monorepo dev/CI
// (default), and `url:`+`checksum:` for the release repo's manifest + CI
// verification.
//
//   - Local (default): consume `out/QdrantEdge.xcframework` via `path:`. This is
//     what `make build` + `swift test` and the local example use.
//   - Release: when the env vars `QDRANT_EDGE_XCFRAMEWORK_URL` and
//     `QDRANT_EDGE_XCFRAMEWORK_CHECKSUM` are set (by `release-xcframework.sh`,
//     which also patches the committed values below), consume the published zip
//     via `url:` + `checksum:` so the release repo's package is installable from
//     a Git tag.
//
// `release-xcframework.sh` rewrites the two constants below at release time. To
// activate release mode you must ALSO set `QDRANT_EDGE_RELEASE`; the
// `QDRANT_EDGE_XCFRAMEWORK_URL`/`_CHECKSUM` env vars then override the constants
// without editing the file (used by CI/verification).
let releaseURL = "https://github.com/qdrant/qdrant/releases/download/edge-v0.7.2/QdrantEdge.xcframework.zip"
let releaseChecksum = ""  // filled by release-xcframework.sh

let env = ProcessInfo.processInfo.environment
let xcframeworkTarget: Target = {
    let url = env["QDRANT_EDGE_XCFRAMEWORK_URL"] ?? releaseURL
    let checksum = env["QDRANT_EDGE_XCFRAMEWORK_CHECKSUM"] ?? releaseChecksum
    // Release mode is opt-in via QDRANT_EDGE_RELEASE. If it's set we MUST resolve
    // the remote target — fail closed rather than silently fall back to the local
    // build, which would publish/test the wrong (unpinned) artifact undetected.
    if env["QDRANT_EDGE_RELEASE"] != nil {
        guard !checksum.isEmpty else {
            fatalError(
                "QDRANT_EDGE_RELEASE is set but no checksum is available. Run " +
                "release-xcframework.sh first, or set QDRANT_EDGE_XCFRAMEWORK_CHECKSUM."
            )
        }
        return .binaryTarget(
            name: "qdrant_edge_ffiFFI",
            url: url,
            checksum: checksum
        )
    }
    // Default: local build output for development.
    return .binaryTarget(
        name: "qdrant_edge_ffiFFI",
        path: "out/QdrantEdge.xcframework"
    )
}()

let package = Package(
    name: "QdrantEdge",
    // Apple platforms only. tvOS/visionOS are built by `make build-all` (tier-3
    // Rust targets, nightly + -Z build-std) but are NOT declared/shipped in this
    // first release — add them back here once their slices are in the XCFramework.
    //
    // Non-Apple Swift (Linux/Windows, server-side Swift) is intentionally out of
    // scope: an XCFramework is Apple-only, so those need a separate source-target
    // + per-platform Rust cdylib and conditional linking. Deferred until a real
    // consumer appears.
    platforms: [
        .iOS(.v15),
        .macOS(.v13),
    ],
    products: [
        .library(
            // Only the `QdrantEdge` library product is published; the binary FFI
            // target is a dependency of it, not a product. The intended surface
            // is `import QdrantEdge`. SwiftPM does not hard-prevent a consumer
            // from also `import`ing the raw `qdrant_edge_ffiFFI` C module (a
            // C-modulemap binaryTarget can be reachable transitively) — but that
            // module exposes only raw C ABI (no Swift API), and the Swift-level
            // plumbing inside `QdrantEdge.swift` is demoted to `internal`. Treat
            // the raw C module as reserved, same as mozilla/matrix's UniFFI Swift
            // SDKs. (See README "Public API" for the residual reserved members.)
            name: "QdrantEdge",
            targets: ["QdrantEdge"]
        ),
    ],
    targets: [
        .target(
            name: "QdrantEdge",
            dependencies: ["qdrant_edge_ffiFFI"],
            path: "out/swift-bindings",
            sources: ["QdrantEdge.swift"]
        ),
        xcframeworkTarget,
        .testTarget(
            name: "QdrantEdgeTests",
            dependencies: ["QdrantEdge"],
            path: "Tests/QdrantEdgeTests"
        ),
    ]
)
