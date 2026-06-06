// swift-tools-version:5.9
import PackageDescription
import Foundation

// The XCFramework binary target has two modes:
//
//   - Local (default): consume `out/QdrantEdge.xcframework` via `path:`. This is
//     what `make build` + `swift test` and the local example use.
//   - Release: when the env vars `QDRANT_EDGE_XCFRAMEWORK_URL` and
//     `QDRANT_EDGE_XCFRAMEWORK_CHECKSUM` are set (by `release-xcframework.sh`,
//     which also patches the committed values below), consume the published zip
//     via `url:` + `checksum:` so the package is installable from a Git tag.
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
    platforms: [
        .iOS(.v15),
        .macOS(.v13),
        .tvOS(.v15),
        .visionOS(.v1),
    ],
    products: [
        .library(
            // Only `QdrantEdge` is exposed to consumers. The binary FFI target
            // is a private dependency of `QdrantEdge` (linked, not importable),
            // so `import qdrant_edge_ffiFFI` is not available downstream — the
            // demoted C plumbing stays internal.
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
