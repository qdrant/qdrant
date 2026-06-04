// swift-tools-version:5.9
import PackageDescription

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
        .binaryTarget(
            name: "qdrant_edge_ffiFFI",
            path: "out/QdrantEdge.xcframework"
        ),
        .testTarget(
            name: "QdrantEdgeTests",
            dependencies: ["QdrantEdge"],
            path: "Tests/QdrantEdgeTests"
        ),
    ]
)
