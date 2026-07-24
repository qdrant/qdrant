// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "QdrantEdgeExample",
    platforms: [
        .iOS(.v15),
        .macOS(.v13),
        .tvOS(.v15),
        .visionOS(.v1),
    ],
    dependencies: [
        .package(name: "QdrantEdge", path: ".."),
    ],
    targets: [
        .executableTarget(
            name: "QdrantEdgeExample",
            dependencies: ["QdrantEdge"],
            path: "Sources"
        ),
    ]
)
