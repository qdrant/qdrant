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
            name: "QdrantEdge",
            targets: ["QdrantEdge", "qdrant_edge_ffiFFI"]
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
    ]
)
