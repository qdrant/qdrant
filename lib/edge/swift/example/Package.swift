// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "QdrantEdgeExample",
    // iOS + macOS only, matching the QdrantEdge package's shipped slices.
    platforms: [
        .iOS(.v15),
        .macOS(.v13),
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
