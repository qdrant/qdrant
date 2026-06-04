import XCTest
import QdrantEdge

final class QdrantEdgeTests: XCTestCase {

    // Base temp directory unique per test run, cleaned up in tearDown
    private var testDir: URL!

    override func setUp() {
        super.setUp()
        testDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("QdrantEdgeTests-\(UUID().uuidString)")
        try! FileManager.default.createDirectory(at: testDir, withIntermediateDirectories: true)
    }

    override func tearDown() {
        if let dir = testDir, FileManager.default.fileExists(atPath: dir.path) {
            try? FileManager.default.removeItem(at: dir)
        }
        super.tearDown()
    }

    // MARK: - Helpers

    private func makeConfig(size: UInt64 = 4) -> EdgeConfig {
        EdgeConfig(
            vectorData: [
                "": VectorDataConfig(
                    size: size,
                    distance: .dot,
                    quantizationConfig: nil,
                    multivectorConfig: nil,
                    datatype: nil
                ),
            ],
            sparseVectorData: [:]
        )
    }

    // MARK: - testLoadUpsertSearchClose

    /// Loads a fresh shard, upserts 3 points, runs a search, and verifies
    /// that results are returned before closing the shard.
    func testLoadUpsertSearchClose() throws {
        let shardURL = testDir.appendingPathComponent("shard1")
        try FileManager.default.createDirectory(at: shardURL, withIntermediateDirectories: true)
        let shardPath = shardURL.path

        let shard = try EdgeShard.load(path: shardPath, config: makeConfig())

        let upsertOp = try UpdateOperation.upsertPoints(points: [
            Point(
                id: .numId(value: 1),
                vector: .single(values: [1.0, 0.0, 0.0, 0.0]),
                payload: "{\"label\": \"a\"}"
            ),
            Point(
                id: .numId(value: 2),
                vector: .single(values: [0.0, 1.0, 0.0, 0.0]),
                payload: "{\"label\": \"b\"}"
            ),
            Point(
                id: .numId(value: 3),
                vector: .single(values: [0.0, 0.0, 1.0, 0.0]),
                payload: "{\"label\": \"c\"}"
            ),
        ])
        try shard.update(operation: upsertOp)

        let results = try shard.search(request: SearchRequest(
            query: .nearest(vector: [1.0, 0.0, 0.0, 0.0], using: nil),
            limit: 10,
            offset: nil,
            filter: nil,
            params: nil,
            withVector: .bool(enable: false),
            withPayload: .bool(enable: true),
            scoreThreshold: nil
        ))

        XCTAssertFalse(results.isEmpty, "Search should return at least one result")
        XCTAssertEqual(results.count, 3, "All 3 upserted points should be returned")

        // The nearest to [1,0,0,0] should be point 1
        if case let .numId(value) = results.first?.id {
            XCTAssertEqual(value, 1, "Nearest point should be id=1")
        } else {
            XCTFail("First result id should be numId(1)")
        }

        shard.unload()
    }

    // MARK: - testPersistenceAcrossReload

    /// Upserts points, flushes, drops the reference, re-opens the same path
    /// without a config, and verifies the point count is preserved.
    func testPersistenceAcrossReload() throws {
        let shardURL = testDir.appendingPathComponent("shard-persist")
        try FileManager.default.createDirectory(at: shardURL, withIntermediateDirectories: true)
        let shardPath = shardURL.path

        do {
            let shard = try EdgeShard.load(path: shardPath, config: makeConfig())

            let upsertOp = try UpdateOperation.upsertPoints(points: [
                Point(
                    id: .numId(value: 10),
                    vector: .single(values: [1.0, 2.0, 3.0, 4.0]),
                    payload: nil
                ),
                Point(
                    id: .numId(value: 20),
                    vector: .single(values: [4.0, 3.0, 2.0, 1.0]),
                    payload: nil
                ),
            ])
            try shard.update(operation: upsertOp)
            try shard.flush()
            shard.unload()
        }

        // Re-open the same path without supplying a config
        let reopened = try EdgeShard.load(path: shardPath, config: nil)
        let info = try reopened.info()
        reopened.unload()

        XCTAssertEqual(info.pointsCount, 2, "Persisted shard should contain exactly 2 points after reload")
    }

    // MARK: - testInvalidUuidThrowsCatchableError

    /// Passes a syntactically invalid UUID string to deletePoints (a throwing
    /// constructor) and verifies that the error is surfaced as an EdgeError,
    /// not a crash — proving bad input produces a catchable Swift error.
    func testInvalidUuidThrowsCatchableError() throws {
        XCTAssertThrowsError(
            try UpdateOperation.deletePoints(pointIds: [.uuid(value: "not-a-uuid")])
        ) { error in
            XCTAssertTrue(
                error is EdgeError,
                "Expected EdgeError, got \(type(of: error)): \(error)"
            )
            // A malformed UUID is host-supplied bad input → InvalidArgument (C5).
            if case let EdgeError.InvalidArgument(reason) = error {
                XCTAssertFalse(reason.isEmpty, "EdgeError.InvalidArgument should carry a non-empty reason")
            } else {
                XCTFail("Expected EdgeError.InvalidArgument, got \(error)")
            }
        }
    }
}
