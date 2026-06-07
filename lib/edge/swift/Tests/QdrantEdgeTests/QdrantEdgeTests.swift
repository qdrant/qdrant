import XCTest
import QdrantEdge

final class QdrantEdgeTests: XCTestCase {

    // Base temp directory unique per test run, cleaned up in tearDown
    private var testDir: URL!

    override func setUpWithError() throws {
        testDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("QdrantEdgeTests-\(UUID().uuidString)")
        // `try` (not `try!`): a temp-dir creation failure fails THIS test
        // cleanly instead of crashing the whole test runner.
        try FileManager.default.createDirectory(at: testDir, withIntermediateDirectories: true)
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
                    datatype: nil,
                    hnswConfig: nil
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

    // MARK: - Helper: load a fresh shard at a unique subdir, upsert 3 points

    private func loadWithThreePoints(_ name: String) throws -> EdgeShard {
        let shardURL = testDir.appendingPathComponent(name)
        try FileManager.default.createDirectory(at: shardURL, withIntermediateDirectories: true)
        let shard = try EdgeShard.load(path: shardURL.path, config: makeConfig())
        try shard.update(operation: try UpdateOperation.upsertPoints(points: [
            Point(id: .numId(value: 1), vector: .single(values: [1.0, 0.0, 0.0, 0.0]), payload: "{\"label\": \"a\"}"),
            Point(id: .numId(value: 2), vector: .single(values: [0.0, 1.0, 0.0, 0.0]), payload: "{\"label\": \"b\"}"),
            Point(id: .numId(value: 3), vector: .single(values: [0.0, 0.0, 1.0, 0.0]), payload: "{\"label\": \"c\"}"),
        ]))
        return shard
    }

    // MARK: - testDeleteReducesCount

    /// Deletes a point and verifies count drops and the ID is no longer retrievable.
    func testDeleteReducesCount() throws {
        let shard = try loadWithThreePoints("delete")
        defer { shard.unload() }

        try shard.update(operation: try UpdateOperation.deletePoints(pointIds: [.numId(value: 2)]))

        let count = try shard.count(request: CountRequest(filter: nil, exact: true))
        XCTAssertEqual(count, 2, "Count should drop to 2 after deleting one point")

        let got = try shard.retrieve(pointIds: [.numId(value: 2)], withPayload: nil, withVector: nil)
        XCTAssertTrue(got.isEmpty, "Deleted point should not be retrievable")
    }

    // MARK: - testSetPayloadVisibleOnRetrieve

    /// Sets a new payload key and verifies it merges with the original on retrieve.
    func testSetPayloadVisibleOnRetrieve() throws {
        let shard = try loadWithThreePoints("set-payload")
        defer { shard.unload() }

        try shard.update(operation: try UpdateOperation.setPayload(
            pointIds: [.numId(value: 1)],
            payloadJson: "{\"tag\": \"hot\"}"
        ))

        let got = try shard.retrieve(
            pointIds: [.numId(value: 1)],
            withPayload: .bool(enable: true),
            withVector: nil
        )
        XCTAssertEqual(got.count, 1)
        let payload = try XCTUnwrap(got.first?.payload, "Payload should be present after setPayload")
        XCTAssertTrue(payload.contains("\"tag\""), "New key 'tag' should be visible: \(payload)")
        XCTAssertTrue(payload.contains("\"label\""), "Original key 'label' should survive the merge: \(payload)")
    }

    // MARK: - testScrollReturnsAllPoints

    /// Scrolls the shard and verifies all points come back.
    func testScrollReturnsAllPoints() throws {
        let shard = try loadWithThreePoints("scroll")
        defer { shard.unload() }

        let page = try shard.scroll(request: ScrollRequest(
            offset: nil,
            limit: 10,
            filter: nil,
            withPayload: .bool(enable: false),
            withVector: .bool(enable: false),
            orderBy: nil
        ))
        XCTAssertEqual(page.records.count, 3, "Scroll should return all 3 points")
    }

    // MARK: - testQueryReturnsRankedResults

    /// Exercises the query() path (vector scoring) and asserts results return.
    func testQueryReturnsRankedResults() throws {
        let shard = try loadWithThreePoints("query")
        defer { shard.unload() }

        let results = try shard.query(request: QueryRequest(
            limit: 10,
            offset: nil,
            query: .vector(query: .nearest(vector: [1.0, 0.0, 0.0, 0.0], using: nil)),
            prefetches: [],
            withVector: nil,
            withPayload: nil,
            filter: nil,
            scoreThreshold: nil,
            params: nil
        ))
        XCTAssertEqual(results.count, 3, "Query should rank all 3 points")
        if case let .numId(value) = results.first?.id {
            XCTAssertEqual(value, 1, "Nearest to [1,0,0,0] should be id=1")
        } else {
            XCTFail("First query result id should be numId(1)")
        }
    }

    // MARK: - testTurboQuantizationLoadsAndSearches

    /// On-device proof that Turbo quantization works through the Swift bindings.
    func testTurboQuantizationLoadsAndSearches() throws {
        let shardURL = testDir.appendingPathComponent("turbo")
        try FileManager.default.createDirectory(at: shardURL, withIntermediateDirectories: true)

        let config = EdgeConfig(
            vectorData: [
                "": VectorDataConfig(
                    size: 4,
                    distance: .dot,
                    quantizationConfig: .turbo(config: TurboQuantizationParams(alwaysRam: true, bits: .bits4)),
                    multivectorConfig: nil,
                    datatype: nil,
                    hnswConfig: nil
                ),
            ],
            sparseVectorData: [:]
        )

        let shard = try EdgeShard.load(path: shardURL.path, config: config)
        defer { shard.unload() }

        try shard.update(operation: try UpdateOperation.upsertPoints(points: [
            Point(id: .numId(value: 1), vector: .single(values: [1.0, 0.0, 0.0, 0.0]), payload: nil),
        ]))

        let results = try shard.search(request: SearchRequest(
            query: .nearest(vector: [1.0, 0.0, 0.0, 0.0], using: nil),
            limit: 10,
            offset: nil,
            filter: nil,
            params: nil,
            withVector: nil,
            withPayload: nil,
            scoreThreshold: nil
        ))
        XCTAssertFalse(results.isEmpty, "Turbo-quantized search should return a result")
    }

    // MARK: - testHnswOptimizeAndSearch

    /// Loads with an explicit HNSW config, upserts, runs optimize() (builds the
    /// HNSW index from plain segments), and confirms search still works — proving
    /// the HNSW config reaches the optimizer and optimize() works on-device.
    func testHnswOptimizeAndSearch() throws {
        let shardURL = testDir.appendingPathComponent("hnsw-optimize")
        try FileManager.default.createDirectory(at: shardURL, withIntermediateDirectories: true)

        let config = EdgeConfig(
            vectorData: [
                "": VectorDataConfig(
                    size: 4,
                    distance: .dot,
                    quantizationConfig: nil,
                    multivectorConfig: nil,
                    datatype: nil,
                    hnswConfig: HnswIndexConfig(
                        m: 16,
                        efConstruct: 100,
                        fullScanThreshold: 10000,
                        maxIndexingThreads: 1,
                        onDisk: false,
                        payloadM: nil
                    )
                ),
            ],
            sparseVectorData: [:]
        )

        let shard = try EdgeShard.load(path: shardURL.path, config: config)
        defer { shard.unload() }

        try shard.update(operation: try UpdateOperation.upsertPoints(points: [
            Point(id: .numId(value: 1), vector: .single(values: [1.0, 0.0, 0.0, 0.0]), payload: nil),
            Point(id: .numId(value: 2), vector: .single(values: [0.0, 1.0, 0.0, 0.0]), payload: nil),
            Point(id: .numId(value: 3), vector: .single(values: [0.0, 0.0, 1.0, 0.0]), payload: nil),
        ]))

        // Builds the HNSW index; we don't assert the bool (a tiny shard may be
        // already optimal), only that it does not throw and search still works.
        _ = try shard.optimize()

        let results = try shard.search(request: SearchRequest(
            query: .nearest(vector: [1.0, 0.0, 0.0, 0.0], using: nil),
            limit: 3,
            offset: nil,
            filter: nil,
            params: nil,
            withVector: nil,
            withPayload: nil,
            scoreThreshold: nil
        ))
        XCTAssertEqual(results.count, 3, "Search after optimize should return all points")
    }

    // MARK: - testOversizedHnswParamRejected

    /// An absurd HNSW `m` would drive a multi-terabyte allocation at optimize()
    /// and abort the process; load() must reject it as a catchable EdgeError
    /// instead. Reaching the assertion (no abort) is the proof.
    func testOversizedHnswParamRejected() throws {
        let shardURL = testDir.appendingPathComponent("hnsw-oversized")
        try FileManager.default.createDirectory(at: shardURL, withIntermediateDirectories: true)

        let config = EdgeConfig(
            vectorData: [
                "": VectorDataConfig(
                    size: 4,
                    distance: .dot,
                    quantizationConfig: nil,
                    multivectorConfig: nil,
                    datatype: nil,
                    hnswConfig: HnswIndexConfig(
                        m: .max,
                        efConstruct: 100,
                        fullScanThreshold: 10000,
                        maxIndexingThreads: 1,
                        onDisk: false,
                        payloadM: nil
                    )
                ),
            ],
            sparseVectorData: [:]
        )

        XCTAssertThrowsError(try EdgeShard.load(path: shardURL.path, config: config)) { error in
            guard case EdgeError.InvalidArgument = error else {
                return XCTFail("Expected EdgeError.InvalidArgument for oversized HNSW m, got \(error)")
            }
        }
    }
}
