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
            query: .nearest(vector: .dense(values: [1.0, 0.0, 0.0, 0.0]), using: nil),
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

        try shard.unload()
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
            try shard.unload()
        }

        // Re-open the same path without supplying a config
        let reopened = try EdgeShard.load(path: shardPath, config: nil)
        let info = try reopened.info()
        try reopened.unload()

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
        defer { try? shard.unload() }

        try shard.update(operation: try UpdateOperation.deletePoints(pointIds: [.numId(value: 2)]))

        let count = try shard.count(request: CountRequest(filter: nil, exact: true))
        XCTAssertEqual(count, 2, "Count should drop to 2 after deleting one point")

        let got = try shard.retrieve(request: RetrieveRequest(pointIds: [.numId(value: 2)], withPayload: nil, withVector: nil))
        XCTAssertTrue(got.isEmpty, "Deleted point should not be retrievable")
    }

    // MARK: - testSetPayloadVisibleOnRetrieve

    /// Sets a new payload key and verifies it merges with the original on retrieve.
    func testSetPayloadVisibleOnRetrieve() throws {
        let shard = try loadWithThreePoints("set-payload")
        defer { try? shard.unload() }

        try shard.update(operation: try UpdateOperation.setPayload(
            pointIds: [.numId(value: 1)],
            payloadJson: "{\"tag\": \"hot\"}"
        ))

        let got = try shard.retrieve(request: RetrieveRequest(
            pointIds: [.numId(value: 1)],
            withPayload: .bool(enable: true),
            withVector: nil
        ))
        XCTAssertEqual(got.count, 1)
        let payload = try XCTUnwrap(got.first?.payload, "Payload should be present after setPayload")
        XCTAssertTrue(payload.contains("\"tag\""), "New key 'tag' should be visible: \(payload)")
        XCTAssertTrue(payload.contains("\"label\""), "Original key 'label' should survive the merge: \(payload)")
    }

    // MARK: - testScrollReturnsAllPoints

    /// Scrolls the shard and verifies all points come back.
    func testScrollReturnsAllPoints() throws {
        let shard = try loadWithThreePoints("scroll")
        defer { try? shard.unload() }

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
        defer { try? shard.unload() }

        let results = try shard.query(request: QueryRequest(
            limit: 10,
            offset: nil,
            query: .vector(query: .nearest(vector: .dense(values: [1.0, 0.0, 0.0, 0.0]), using: nil)),
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
                    quantizationConfig: .turbo(config: TurboQuantizationParams(memory: .pinned, bits: .bits4)),
                    multivectorConfig: nil,
                    datatype: nil,
                    hnswConfig: nil
                ),
            ],
            sparseVectorData: [:]
        )

        let shard = try EdgeShard.load(path: shardURL.path, config: config)
        defer { try? shard.unload() }

        try shard.update(operation: try UpdateOperation.upsertPoints(points: [
            Point(id: .numId(value: 1), vector: .single(values: [1.0, 0.0, 0.0, 0.0]), payload: nil),
        ]))

        let results = try shard.search(request: SearchRequest(
            query: .nearest(vector: .dense(values: [1.0, 0.0, 0.0, 0.0]), using: nil),
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
                        memory: .pinned,
                        payloadM: nil
                    )
                ),
            ],
            sparseVectorData: [:]
        )

        let shard = try EdgeShard.load(path: shardURL.path, config: config)
        defer { try? shard.unload() }

        try shard.update(operation: try UpdateOperation.upsertPoints(points: [
            Point(id: .numId(value: 1), vector: .single(values: [1.0, 0.0, 0.0, 0.0]), payload: nil),
            Point(id: .numId(value: 2), vector: .single(values: [0.0, 1.0, 0.0, 0.0]), payload: nil),
            Point(id: .numId(value: 3), vector: .single(values: [0.0, 0.0, 1.0, 0.0]), payload: nil),
        ]))

        // Builds the HNSW index; we don't assert the bool (a tiny shard may be
        // already optimal), only that it does not throw and search still works.
        _ = try shard.optimize()

        let results = try shard.search(request: SearchRequest(
            query: .nearest(vector: .dense(values: [1.0, 0.0, 0.0, 0.0]), using: nil),
            limit: 3,
            offset: nil,
            filter: nil,
            params: nil,
            withVector: nil,
            withPayload: nil,
            scoreThreshold: nil
        ))
        XCTAssertEqual(results.count, 3, "Search after optimize should return all points")

        // The HNSW config round-trips honestly through config().
        let readBack = try shard.config()
        let hnsw = try XCTUnwrap(
            readBack.vectorData[""]?.hnswConfig,
            "HNSW config should round-trip through config()"
        )
        XCTAssertEqual(hnsw.m, 16, "round-tripped HNSW m should match")
        XCTAssertEqual(hnsw.efConstruct, 100, "round-tripped HNSW efConstruct should match")
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
                        memory: .pinned,
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

    // MARK: - Parity tests for the expanded FFI surface
    //
    // The tests above cover the original core. These exercise the surface added
    // after the first cut — parameterized payload indexes, order-by/order_value,
    // recommend, grouping, and formula rescoring — proving each is reachable and
    // works through the generated Swift bindings. (search_matrix is deliberately
    // absent: the mobile build drops the `matrix` feature.)

    /// Loads a shard and upserts 3 points carrying an integer `rank` and a
    /// keyword `label`, for the index / order-by / grouping tests.
    private func loadWithRankedPoints(_ name: String) throws -> EdgeShard {
        let shardURL = testDir.appendingPathComponent(name)
        try FileManager.default.createDirectory(at: shardURL, withIntermediateDirectories: true)
        let shard = try EdgeShard.load(path: shardURL.path, config: makeConfig())
        try shard.update(operation: try UpdateOperation.upsertPoints(points: [
            Point(id: .numId(value: 1), vector: .single(values: [1.0, 0.0, 0.0, 0.0]), payload: "{\"rank\": 30, \"label\": \"a\"}"),
            Point(id: .numId(value: 2), vector: .single(values: [0.0, 1.0, 0.0, 0.0]), payload: "{\"rank\": 10, \"label\": \"b\"}"),
            Point(id: .numId(value: 3), vector: .single(values: [0.0, 0.0, 1.0, 0.0]), payload: "{\"rank\": 20, \"label\": \"a\"}"),
        ]))
        return shard
    }

    /// Creates a parameterized integer payload index and confirms info()
    /// reports it back via payloadSchema.
    func testCreateFieldIndexWithParamsAndIntrospect() throws {
        let shard = try loadWithRankedPoints("payload-index")
        defer { try? shard.unload() }

        try shard.update(operation: try UpdateOperation.createFieldIndexWithParams(
            fieldName: "rank",
            params: .integer(config: IntegerIndexParams(lookup: true, range: true))
        ))

        let schema = try shard.info().payloadSchema
        let rankIndex = try XCTUnwrap(schema["rank"], "created payload index should be reported by info()")
        XCTAssertEqual(rankIndex.dataType, .integer, "rank index should report Integer data type")
    }

    /// Scrolls ordered by an indexed integer field and confirms each record
    /// carries its order_value.
    func testOrderByScrollPopulatesOrderValue() throws {
        let shard = try loadWithRankedPoints("order-value")
        defer { try? shard.unload() }

        try shard.update(operation: try UpdateOperation.createFieldIndex(fieldName: "rank", schema: .integer))

        let page = try shard.scroll(request: ScrollRequest(
            offset: nil,
            limit: 10,
            filter: nil,
            withPayload: .bool(enable: false),
            withVector: .bool(enable: false),
            orderBy: OrderBy(key: "rank", direction: .asc, startFrom: nil)
        ))
        XCTAssertEqual(page.records.count, 3, "scroll should return all 3 points")
        // Ascending by rank: the first record is rank 10 (point 2).
        guard case let .int(value) = page.records.first?.orderValue else {
            return XCTFail("order-by scroll should populate an integer order_value, got \(String(describing: page.records.first?.orderValue))")
        }
        XCTAssertEqual(value, 10, "smallest rank should sort first")
    }

    /// A recommendation query (one positive example) returns ranked results.
    func testRecommendReturnsResults() throws {
        let shard = try loadWithThreePoints("recommend")
        defer { try? shard.unload() }

        let results = try shard.query(request: QueryRequest(
            limit: 10,
            offset: nil,
            query: .vector(query: .recommend(
                positives: [.dense(values: [1.0, 0.0, 0.0, 0.0])],
                negatives: [],
                strategy: nil,
                using: nil
            )),
            prefetches: [],
            withVector: nil,
            withPayload: nil,
            filter: nil,
            scoreThreshold: nil,
            params: nil
        ))
        XCTAssertFalse(results.isEmpty, "recommend should return results")
    }

    /// A grouped query returns one group per distinct label value.
    func testQueryGroupsReturnsGroups() throws {
        let shard = try loadWithRankedPoints("grouping")
        defer { try? shard.unload() }

        let groups = try shard.queryGroups(request: GroupRequest(
            query: QueryRequest(
                limit: 10,
                offset: nil,
                query: .vector(query: .nearest(vector: .dense(values: [1.0, 0.0, 0.0, 0.0]), using: nil)),
                prefetches: [],
                withVector: nil,
                withPayload: nil,
                filter: nil,
                scoreThreshold: nil,
                params: nil
            ),
            groupBy: "label",
            groups: 10,
            groupSize: 10
        ))
        // Two distinct labels ("a", "b") -> two groups.
        XCTAssertEqual(groups.count, 2, "grouping by label should yield one group per distinct value")
    }

    /// Formula rescoring: re-rank a prefetch's results by an Expression over
    /// its score. Exercises both the Expression constructors and the
    /// `ScoringQuery.formula` path.
    func testFormulaRescoringQuery() throws {
        let shard = try loadWithThreePoints("formula")
        defer { try? shard.unload() }

        // Trivial but valid formula: re-rank by the prefetch score.
        let expression = Expression.variable(name: "$score")

        let results = try shard.query(request: QueryRequest(
            limit: 10,
            offset: nil,
            query: .formula(expression: expression, defaults: [:]),
            prefetches: [
                Prefetch(
                    limit: 10,
                    query: .vector(query: .nearest(vector: .dense(values: [1.0, 0.0, 0.0, 0.0]), using: nil)),
                    prefetches: [],
                    filter: nil,
                    scoreThreshold: nil,
                    params: nil
                )
            ],
            withVector: nil,
            withPayload: nil,
            filter: nil,
            scoreThreshold: nil,
            params: nil
        ))
        XCTAssertEqual(results.count, 3, "formula rescoring should return all prefetched points")
    }

    // MARK: - Coverage-gap tests (SDK review)
    //
    // The tests below close specific gaps flagged in the SDK review: filtered
    // reads, a *ByFilter update, the ShardClosed / OperationError error variants,
    // a uuid happy path, the vector DECODE path, a handful of untested
    // UpdateOperation constructors, and a concurrency smoke test.

    /// Decodes a returned vector JSON string (the binding surfaces vectors as a
    /// JSON string, not `[Float]`) into its float components. Handles both the
    /// bare-array form and the named `{"field": [...]}` object form.
    private static func extractVectorFloats(_ json: String) throws -> [Float] {
        let data = try XCTUnwrap(json.data(using: .utf8), "vector JSON should be UTF-8")
        let obj = try JSONSerialization.jsonObject(with: data, options: [.fragmentsAllowed])
        if let arr = obj as? [Any] {
            return arr.compactMap { ($0 as? NSNumber)?.floatValue }
        }
        if let dict = obj as? [String: Any], let arr = dict.values.first as? [Any] {
            return arr.compactMap { ($0 as? NSNumber)?.floatValue }
        }
        return []
    }

    // MARK: - testFilteredSearchNarrowsResults

    /// Headline gap: a search with a non-nil `Filter` (keyword match on the
    /// `label` payload key) must narrow the result set vs the unfiltered search.
    func testFilteredSearchNarrowsResults() throws {
        let shard = try loadWithThreePoints("filtered-search")
        defer { try? shard.unload() }

        let unfiltered = try shard.search(request: SearchRequest(
            query: .nearest(vector: .dense(values: [1.0, 1.0, 1.0, 1.0]), using: nil),
            limit: 10, offset: nil, filter: nil, params: nil,
            withVector: nil, withPayload: nil, scoreThreshold: nil
        ))
        XCTAssertEqual(unfiltered.count, 3, "unfiltered search should see all 3 points")

        let labelB = Filter(
            must: [.field(condition: FieldCondition(key: "label", match: .value(value: .string(value: "b"))))],
            should: nil,
            mustNot: nil
        )
        let filtered = try shard.search(request: SearchRequest(
            query: .nearest(vector: .dense(values: [1.0, 1.0, 1.0, 1.0]), using: nil),
            limit: 10, offset: nil, filter: labelB, params: nil,
            withVector: nil, withPayload: nil, scoreThreshold: nil
        ))
        XCTAssertEqual(filtered.count, 1, "filter label=b should match exactly one point")
        if case let .numId(value) = filtered.first?.id {
            XCTAssertEqual(value, 2, "point 2 is the one carrying label=b")
        } else {
            XCTFail("filtered result id should be numId(2)")
        }
    }

    // MARK: - testSetPayloadByFilterAffectsMatchingOnly

    /// A `*ByFilter` update op: `setPayloadByFilter` must mutate only the points
    /// matching the filter, leaving the rest untouched.
    func testSetPayloadByFilterAffectsMatchingOnly() throws {
        let shard = try loadWithThreePoints("set-payload-by-filter")
        defer { try? shard.unload() }

        let labelA = Filter(
            must: [.field(condition: FieldCondition(key: "label", match: .value(value: .string(value: "a"))))],
            should: nil,
            mustNot: nil
        )
        try shard.update(operation: try UpdateOperation.setPayloadByFilter(
            filter: labelA,
            payloadJson: "{\"tag\": \"hot\"}"
        ))

        // Point 1 (label=a) should gain the tag.
        let matched = try shard.retrieve(request: RetrieveRequest(
            pointIds: [.numId(value: 1)], withPayload: .bool(enable: true), withVector: nil
        ))
        let matchedPayload = try XCTUnwrap(matched.first?.payload, "point 1 should have a payload")
        XCTAssertTrue(matchedPayload.contains("\"tag\""), "matching point should gain 'tag': \(matchedPayload)")

        // Point 2 (label=b) should be untouched.
        let unmatched = try shard.retrieve(request: RetrieveRequest(
            pointIds: [.numId(value: 2)], withPayload: .bool(enable: true), withVector: nil
        ))
        let unmatchedPayload = try XCTUnwrap(unmatched.first?.payload, "point 2 should have a payload")
        XCTAssertFalse(unmatchedPayload.contains("\"tag\""), "non-matching point should be untouched: \(unmatchedPayload)")
    }

    // MARK: - testCountAfterUnloadThrowsShardClosed

    /// After `unload()`, an operation on the same handle must throw a catchable
    /// `EdgeError.ShardClosed` — not crash the process.
    func testCountAfterUnloadThrowsShardClosed() throws {
        let shard = try loadWithThreePoints("shard-closed")
        try shard.unload()

        XCTAssertThrowsError(
            try shard.count(request: CountRequest(filter: nil, exact: true))
        ) { error in
            guard case EdgeError.ShardClosed = error else {
                return XCTFail("Expected EdgeError.ShardClosed after unload, got \(error)")
            }
        }
    }

    // MARK: - testDimensionMismatchThrowsOperationError

    /// Upserting a 2-dim vector into a 4-dim field is an engine-level failure
    /// (the FFI boundary can't know the field size), surfaced as the non-
    /// InvalidArgument variant `EdgeError.OperationError`.
    func testDimensionMismatchThrowsOperationError() throws {
        let shard = try loadWithThreePoints("dim-mismatch")
        defer { try? shard.unload() }

        XCTAssertThrowsError(
            try shard.update(operation: try UpdateOperation.upsertPoints(points: [
                Point(id: .numId(value: 99), vector: .single(values: [1.0, 2.0]), payload: nil),
            ]))
        ) { error in
            guard case EdgeError.OperationError = error else {
                return XCTFail("Expected EdgeError.OperationError for dimension mismatch, got \(error)")
            }
        }
    }

    // MARK: - testUuidPointRoundTrips

    /// A point upserted with a valid `.uuid` id round-trips: it is counted and
    /// retrievable by the same uuid, with the id preserved.
    func testUuidPointRoundTrips() throws {
        let shardURL = testDir.appendingPathComponent("uuid-roundtrip")
        try FileManager.default.createDirectory(at: shardURL, withIntermediateDirectories: true)
        let shard = try EdgeShard.load(path: shardURL.path, config: makeConfig())
        defer { try? shard.unload() }

        let uuid = "e9408f2b-b917-4af1-ab75-d97ac6b2c047"
        try shard.update(operation: try UpdateOperation.upsertPoints(points: [
            Point(id: .uuid(value: uuid), vector: .single(values: [1.0, 0.0, 0.0, 0.0]), payload: "{\"label\": \"u\"}"),
        ]))

        let count = try shard.count(request: CountRequest(filter: nil, exact: true))
        XCTAssertEqual(count, 1, "the single uuid point should be counted")

        let got = try shard.retrieve(request: RetrieveRequest(
            pointIds: [.uuid(value: uuid)], withPayload: .bool(enable: true), withVector: nil
        ))
        XCTAssertEqual(got.count, 1, "uuid point should be retrievable by the same uuid")
        if case let .uuid(value) = got.first?.id {
            XCTAssertEqual(value, uuid, "retrieved id should equal the upserted uuid")
        } else {
            XCTFail("retrieved id should be a .uuid")
        }
    }

    // MARK: - testSearchAndRetrieveDecodeUpsertedVector

    /// Vector DECODE path: with `withVector: .bool(enable: true)`, both
    /// `ScoredPoint.vector` and `Record.vector` must carry the upserted values.
    /// The binding returns vectors as a JSON string (not `[Float]`), so we decode
    /// and compare; Dot distance stores vectors unnormalized, so they round-trip
    /// exactly.
    func testSearchAndRetrieveDecodeUpsertedVector() throws {
        let shardURL = testDir.appendingPathComponent("vector-roundtrip")
        try FileManager.default.createDirectory(at: shardURL, withIntermediateDirectories: true)
        let shard = try EdgeShard.load(path: shardURL.path, config: makeConfig())
        defer { try? shard.unload() }

        let upserted: [Float] = [1.0, 2.0, 3.0, 4.0]
        try shard.update(operation: try UpdateOperation.upsertPoints(points: [
            Point(id: .numId(value: 1), vector: .single(values: upserted), payload: nil),
        ]))

        let results = try shard.search(request: SearchRequest(
            query: .nearest(vector: .dense(values: upserted), using: nil),
            limit: 1, offset: nil, filter: nil, params: nil,
            withVector: .bool(enable: true), withPayload: nil, scoreThreshold: nil
        ))
        XCTAssertEqual(results.count, 1)
        let searchVectorJson = try XCTUnwrap(results.first?.vector, "withVector:true should populate ScoredPoint.vector")
        let searchDecoded = try Self.extractVectorFloats(searchVectorJson)
        XCTAssertEqual(searchDecoded.count, 4, "decoded vector should have 4 components: \(searchVectorJson)")
        for (i, expected) in upserted.enumerated() {
            XCTAssertEqual(searchDecoded[i], expected, accuracy: 1e-5, "search component \(i) should round-trip")
        }

        let records = try shard.retrieve(request: RetrieveRequest(
            pointIds: [.numId(value: 1)], withPayload: nil, withVector: .bool(enable: true)
        ))
        let recordVectorJson = try XCTUnwrap(records.first?.vector, "withVector:true should populate Record.vector")
        let recordDecoded = try Self.extractVectorFloats(recordVectorJson)
        XCTAssertEqual(recordDecoded.count, 4, "decoded record vector should have 4 components: \(recordVectorJson)")
        for (i, expected) in upserted.enumerated() {
            XCTAssertEqual(recordDecoded[i], expected, accuracy: 1e-5, "retrieve component \(i) should round-trip")
        }
    }

    // MARK: - testOverwritePayloadReplacesWholePayload

    /// `overwritePayload` replaces the entire payload (unlike `setPayload`, which
    /// merges): the original key must be gone and only the new key present.
    func testOverwritePayloadReplacesWholePayload() throws {
        let shard = try loadWithThreePoints("overwrite-payload")
        defer { try? shard.unload() }

        try shard.update(operation: try UpdateOperation.overwritePayload(
            pointIds: [.numId(value: 1)],
            payloadJson: "{\"tag\": \"hot\"}"
        ))

        let got = try shard.retrieve(request: RetrieveRequest(
            pointIds: [.numId(value: 1)], withPayload: .bool(enable: true), withVector: nil
        ))
        let payload = try XCTUnwrap(got.first?.payload, "payload should be present after overwrite")
        XCTAssertTrue(payload.contains("\"tag\""), "new key should be present: \(payload)")
        XCTAssertFalse(payload.contains("\"label\""), "original key should be gone after overwrite: \(payload)")
    }

    // MARK: - testDeletePayloadRemovesKey

    /// `deletePayload` removes only the named key, leaving other keys intact.
    func testDeletePayloadRemovesKey() throws {
        let shard = try loadWithThreePoints("delete-payload")
        defer { try? shard.unload() }

        // Add a second key so we can prove only the targeted key is removed.
        try shard.update(operation: try UpdateOperation.setPayload(
            pointIds: [.numId(value: 1)],
            payloadJson: "{\"tag\": \"hot\"}"
        ))
        try shard.update(operation: try UpdateOperation.deletePayload(
            pointIds: [.numId(value: 1)],
            keys: ["label"]
        ))

        let got = try shard.retrieve(request: RetrieveRequest(
            pointIds: [.numId(value: 1)], withPayload: .bool(enable: true), withVector: nil
        ))
        let payload = try XCTUnwrap(got.first?.payload, "payload should still be present")
        XCTAssertFalse(payload.contains("\"label\""), "deleted key 'label' should be gone: \(payload)")
        XCTAssertTrue(payload.contains("\"tag\""), "untargeted key 'tag' should survive: \(payload)")
    }

    // MARK: - testClearPayloadEmptiesPayload

    /// `clearPayload` drops all payload keys but keeps the point itself.
    func testClearPayloadEmptiesPayload() throws {
        let shard = try loadWithThreePoints("clear-payload")
        defer { try? shard.unload() }

        try shard.update(operation: try UpdateOperation.clearPayload(pointIds: [.numId(value: 1)]))

        let got = try shard.retrieve(request: RetrieveRequest(
            pointIds: [.numId(value: 1)], withPayload: .bool(enable: true), withVector: nil
        ))
        XCTAssertEqual(got.count, 1, "clearPayload must not delete the point")
        // Empty payload may come back as `nil` or `{}`; either way the key is gone.
        let payload = got.first?.payload ?? "{}"
        XCTAssertFalse(payload.contains("\"label\""), "cleared payload should not contain the original key: \(payload)")
    }

    // MARK: - testCreateDenseVectorAndUpdateVectors

    /// Adds a new named dense vector field (`createDenseVector`), writes it on a
    /// single point (`updateVectors`), and confirms a search on that named field
    /// returns only the point that has the vector.
    func testCreateDenseVectorAndUpdateVectors() throws {
        let shard = try loadWithThreePoints("named-vector")
        defer { try? shard.unload() }

        try shard.update(operation: try UpdateOperation.createDenseVector(
            vectorName: "extra",
            size: 2,
            distance: .dot
        ))
        try shard.update(operation: try UpdateOperation.updateVectors(pointVectors: [
            PointVectors(id: .numId(value: 1), vector: .named(map: ["extra": .dense(values: [5.0, 6.0])])),
        ]))

        let results = try shard.search(request: SearchRequest(
            query: .nearest(vector: .dense(values: [5.0, 6.0]), using: "extra"),
            limit: 10, offset: nil, filter: nil, params: nil,
            withVector: nil, withPayload: nil, scoreThreshold: nil
        ))
        XCTAssertEqual(results.count, 1, "only point 1 has the 'extra' vector")
        if case let .numId(value) = results.first?.id {
            XCTAssertEqual(value, 1, "the point carrying the 'extra' vector is id=1")
        } else {
            XCTFail("named-vector search result id should be numId(1)")
        }
    }

    // MARK: - testConcurrentSearchesAreConsistent

    /// Concurrency smoke test substantiating `@unchecked Sendable`: 8 concurrent
    /// searches against one loaded shard must all succeed and return the same
    /// expected count, with no throw/crash.
    func testConcurrentSearchesAreConsistent() throws {
        let shard = try loadWithThreePoints("concurrency")
        defer { try? shard.unload() }

        let lock = NSLock()
        var counts: [Int] = []
        var failures: [Error] = []

        DispatchQueue.concurrentPerform(iterations: 8) { _ in
            do {
                let results = try shard.search(request: SearchRequest(
                    query: .nearest(vector: .dense(values: [1.0, 0.0, 0.0, 0.0]), using: nil),
                    limit: 10, offset: nil, filter: nil, params: nil,
                    withVector: nil, withPayload: nil, scoreThreshold: nil
                ))
                lock.lock(); counts.append(results.count); lock.unlock()
            } catch {
                lock.lock(); failures.append(error); lock.unlock()
            }
        }

        XCTAssertTrue(failures.isEmpty, "no concurrent search should throw: \(failures)")
        XCTAssertEqual(counts.count, 8, "all 8 concurrent searches should complete")
        XCTAssertTrue(counts.allSatisfy { $0 == 3 }, "every concurrent search should return all 3 points, got \(counts)")
    }

    // MARK: - Full-surface coverage tests (SDK review)
    //
    // The tests below close the remaining public-surface gaps: sparse and
    // multi-vector fields, the discover/context/feedback/fusion/mmr/sample
    // scoring queries, faceting, the create() constructor, the config setters,
    // snapshot manifest + restore, path(), the *ByFilter update family, and
    // index/vector-name deletion. Each drives the real engine through the
    // generated bindings. (search_matrix stays out — the mobile build drops the
    // `matrix` feature.)

    /// A keyword `label` field-condition filter, reused by the *ByFilter tests.
    private func labelFilter(_ value: String) -> Filter {
        Filter(
            must: [.field(condition: FieldCondition(key: "label", match: .value(value: .string(value: value))))],
            should: nil,
            mustNot: nil
        )
    }

    // MARK: - testSparseVectorSearchRanksByOverlap  (gap 1)

    /// A sparse-only shard: upsert two sparse points and query with a sparse
    /// vector. Sparse similarity is the dot of overlapping index weights, so a
    /// query touching an index that only one point carries returns exactly that
    /// point, with a score equal to the overlapping term.
    func testSparseVectorSearchRanksByOverlap() throws {
        let shardURL = testDir.appendingPathComponent("sparse")
        try FileManager.default.createDirectory(at: shardURL, withIntermediateDirectories: true)
        // Empty dense vectorData + one sparse field "sp" (mirrors the FFI test).
        let config = EdgeConfig(vectorData: [:], sparseVectorData: ["sp": SparseVectorDataConfig()])
        let shard = try EdgeShard.load(path: shardURL.path, config: config)
        defer { try? shard.unload() }

        try shard.update(operation: try UpdateOperation.upsertPoints(points: [
            Point(id: .numId(value: 1),
                  vector: .named(map: ["sp": .sparse(vector: SparseVector(indices: [1, 5, 9], values: [0.5, 1.5, 2.5]))]),
                  payload: nil),
            Point(id: .numId(value: 2),
                  vector: .named(map: ["sp": .sparse(vector: SparseVector(indices: [0, 1], values: [1.0, 1.0]))]),
                  payload: nil),
        ]))

        // Query overlaps only index 9, carried by point 1 alone.
        let results = try shard.search(request: SearchRequest(
            query: .nearest(vector: .sparse(vector: SparseVector(indices: [9], values: [1.0])), using: "sp"),
            limit: 10, offset: nil, filter: nil, params: nil,
            withVector: nil, withPayload: nil, scoreThreshold: nil
        ))
        XCTAssertEqual(results.count, 1, "a sparse query touching only index 9 must match exactly point 1")
        if case let .numId(value) = results.first?.id {
            XCTAssertEqual(value, 1, "the only point carrying index 9 is id=1")
        } else {
            XCTFail("sparse search result id should be numId(1)")
        }
        // Dot of the single overlapping term: 2.5 (stored) * 1.0 (query).
        XCTAssertEqual(results.first?.score ?? 0, 2.5, accuracy: 1e-4, "sparse score should be the overlapping-term dot product")
    }

    // MARK: - testMultiVectorMaxSimSearch  (gap 2)

    /// A multi-vector field (MaxSim comparator): upsert two multi-vector points,
    /// query with a single row, and confirm MaxSim ranking (sum over query rows
    /// of the best per-row dot) picks the right winner with the expected score.
    func testMultiVectorMaxSimSearch() throws {
        let shardURL = testDir.appendingPathComponent("multivector")
        try FileManager.default.createDirectory(at: shardURL, withIntermediateDirectories: true)
        let config = EdgeConfig(
            vectorData: ["mv": VectorDataConfig(
                size: 2,
                distance: .dot,
                quantizationConfig: nil,
                multivectorConfig: MultiVectorConfig(comparator: .maxSim),
                datatype: nil,
                hnswConfig: nil
            )],
            sparseVectorData: [:]
        )
        let shard = try EdgeShard.load(path: shardURL.path, config: config)
        defer { try? shard.unload() }

        try shard.update(operation: try UpdateOperation.upsertPoints(points: [
            Point(id: .numId(value: 1), vector: .named(map: ["mv": .multiDense(vectors: [[1.0, 2.0], [3.0, 4.0]])]), payload: nil),
            Point(id: .numId(value: 2), vector: .named(map: ["mv": .multiDense(vectors: [[0.0, 1.0]])]), payload: nil),
        ]))

        let results = try shard.search(request: SearchRequest(
            query: .nearest(vector: .multiDense(vectors: [[3.0, 4.0]]), using: "mv"),
            limit: 10, offset: nil, filter: nil, params: nil,
            withVector: nil, withPayload: nil, scoreThreshold: nil
        ))
        XCTAssertEqual(results.count, 2, "both multi-vector points should be scored")
        if case let .numId(value) = results.first?.id {
            XCTAssertEqual(value, 1, "MaxSim: point 1 (best row dot 25) beats point 2 (4)")
        } else {
            XCTFail("multi-vector search top id should be numId(1)")
        }
        // One query row [3,4]: max(dot([3,4],[1,2])=11, dot([3,4],[3,4])=25) = 25.
        XCTAssertEqual(results.first?.score ?? 0, 25.0, accuracy: 1e-3, "MaxSim score should be the best per-row dot product")
    }

    // MARK: - testDiscoverQueryReturnsResults  (gap 3)

    /// `Query.discover`: a target vector guided by one context pair. Discover's
    /// ranking is context-driven and subtle, so the meaningful assertions are
    /// that it returns a non-empty, in-range result set through the bindings.
    func testDiscoverQueryReturnsResults() throws {
        let shard = try loadWithThreePoints("discover")
        defer { try? shard.unload() }

        let results = try shard.query(request: QueryRequest(
            limit: 10, offset: nil,
            query: .vector(query: .discover(
                target: .dense(values: [1.0, 0.0, 0.0, 0.0]),
                context: [ContextPair(
                    positive: .dense(values: [1.0, 0.0, 0.0, 0.0]),
                    negative: .dense(values: [0.0, 1.0, 0.0, 0.0])
                )],
                using: nil
            )),
            prefetches: [], withVector: nil, withPayload: nil, filter: nil, scoreThreshold: nil, params: nil
        ))
        XCTAssertFalse(results.isEmpty, "discover should return results")
        XCTAssertLessThanOrEqual(results.count, 3, "discover cannot return more than the 3 upserted points")
        for r in results {
            guard case let .numId(v) = r.id, (1...3).contains(v) else {
                return XCTFail("discover returned an unexpected id: \(r.id)")
            }
        }
    }

    // MARK: - testContextQueryReturnsResults  (gap 4)

    /// `Query.context`: rank purely by fit to the context pairs, no target.
    func testContextQueryReturnsResults() throws {
        let shard = try loadWithThreePoints("context")
        defer { try? shard.unload() }

        let results = try shard.query(request: QueryRequest(
            limit: 10, offset: nil,
            query: .vector(query: .context(
                context: [ContextPair(
                    positive: .dense(values: [1.0, 0.0, 0.0, 0.0]),
                    negative: .dense(values: [0.0, 1.0, 0.0, 0.0])
                )],
                using: nil
            )),
            prefetches: [], withVector: nil, withPayload: nil, filter: nil, scoreThreshold: nil, params: nil
        ))
        XCTAssertFalse(results.isEmpty, "context should return results")
        XCTAssertLessThanOrEqual(results.count, 3, "context cannot return more than the 3 upserted points")
    }

    // MARK: - testFeedbackQueryReturnsResults  (gap 5)

    /// `Query.feedback`: fully constructible from the bindings (a target, graded
    /// FeedbackItems, and a/b/c FeedbackCoefficients), so it is exercised for
    /// real rather than smoke-tested. Re-ranking is graded-relevance-driven, so
    /// the assertion is a non-empty, in-range result set.
    func testFeedbackQueryReturnsResults() throws {
        let shard = try loadWithThreePoints("feedback")
        defer { try? shard.unload() }

        let results = try shard.query(request: QueryRequest(
            limit: 10, offset: nil,
            query: .vector(query: .feedback(
                target: .dense(values: [1.0, 0.0, 0.0, 0.0]),
                feedback: [FeedbackItem(vector: .dense(values: [0.0, 1.0, 0.0, 0.0]), score: 1.0)],
                coefficients: FeedbackCoefficients(a: 1.0, b: 1.0, c: 1.0),
                using: nil
            )),
            prefetches: [], withVector: nil, withPayload: nil, filter: nil, scoreThreshold: nil, params: nil
        ))
        XCTAssertFalse(results.isEmpty, "feedback query should return results")
        XCTAssertLessThanOrEqual(results.count, 3, "feedback cannot return more than the 3 upserted points")
    }

    // MARK: - testRrfFusionOverPrefetches  (gap 6)

    /// `ScoringQuery.fusion` (RRF) over two vector prefetches must fuse into a
    /// single ranked set containing every point exactly once, in descending
    /// score order. Mirrors the FFI `rrf_fusion_over_prefetches` test.
    func testRrfFusionOverPrefetches() throws {
        let shard = try loadWithThreePoints("fusion")
        defer { try? shard.unload() }

        let branch: ([Float]) -> Prefetch = { vector in
            Prefetch(
                limit: 3,
                query: .vector(query: .nearest(vector: .dense(values: vector), using: nil)),
                prefetches: [], filter: nil, scoreThreshold: nil, params: nil
            )
        }

        let results = try shard.query(request: QueryRequest(
            limit: 3, offset: nil,
            query: .fusion(fusion: .rrf(k: 60, weights: nil)),
            prefetches: [branch([1.0, 0.0, 0.0, 0.0]), branch([0.0, 0.0, 1.0, 0.0])],
            withVector: nil, withPayload: nil, filter: nil, scoreThreshold: nil, params: nil
        ))
        XCTAssertEqual(results.count, 3, "RRF over two prefetches should fuse to all three points")
        var ids = results.compactMap { p -> UInt64? in
            if case let .numId(v) = p.id { return v } else { return nil }
        }
        ids.sort()
        XCTAssertEqual(ids, [1, 2, 3], "fused result set should contain each point exactly once")
        for i in 1..<results.count {
            XCTAssertGreaterThanOrEqual(results[i - 1].score, results[i].score, "fused scores must be in descending order")
        }
    }

    // MARK: - testMmrQueryReturnsResults / testMmrInvalidLambdaThrows  (gap 7)

    /// `ScoringQuery.mmr`: nearest-neighbor re-ranked for diversity. A valid
    /// lambda in [0,1] returns results.
    func testMmrQueryReturnsResults() throws {
        let shard = try loadWithThreePoints("mmr")
        defer { try? shard.unload() }

        let results = try shard.query(request: QueryRequest(
            limit: 3, offset: nil,
            query: .mmr(vector: .dense(values: [1.0, 0.0, 0.0, 0.0]), using: nil, lambda: 0.5, candidatesLimit: 10),
            prefetches: [], withVector: nil, withPayload: nil, filter: nil, scoreThreshold: nil, params: nil
        ))
        XCTAssertFalse(results.isEmpty, "mmr query should return results")
        XCTAssertLessThanOrEqual(results.count, 3, "mmr cannot return more than the 3 upserted points")
    }

    /// The FFI guards MMR `lambda` into `[0, 1]`: a NaN or out-of-range value
    /// must surface as a catchable `EdgeError.InvalidArgument`, not a panic.
    func testMmrInvalidLambdaThrowsInvalidArgument() throws {
        let shard = try loadWithThreePoints("mmr-bad")
        defer { try? shard.unload() }

        for badLambda: Float in [Float.nan, 2.0, -1.0] {
            XCTAssertThrowsError(try shard.query(request: QueryRequest(
                limit: 3, offset: nil,
                query: .mmr(vector: .dense(values: [1.0, 0.0, 0.0, 0.0]), using: nil, lambda: badLambda, candidatesLimit: 10),
                prefetches: [], withVector: nil, withPayload: nil, filter: nil, scoreThreshold: nil, params: nil
            ))) { error in
                guard case EdgeError.InvalidArgument = error else {
                    return XCTFail("MMR lambda=\(badLambda) should throw InvalidArgument, got \(error)")
                }
            }
        }
    }

    // MARK: - testSampleQueryReturnsLimited  (gap 8)

    /// `ScoringQuery.sample(.random)` returns up to `limit` points at random.
    func testSampleQueryReturnsLimited() throws {
        let shard = try loadWithThreePoints("sample")
        defer { try? shard.unload() }

        let results = try shard.query(request: QueryRequest(
            limit: 2, offset: nil,
            query: .sample(sample: .random),
            prefetches: [], withVector: nil, withPayload: nil, filter: nil, scoreThreshold: nil, params: nil
        ))
        XCTAssertEqual(results.count, 2, "random sample with limit 2 over 3 points returns 2")
        for r in results {
            guard case let .numId(v) = r.id, (1...3).contains(v) else {
                return XCTFail("sample returned an unexpected id: \(r.id)")
            }
        }
    }

    // MARK: - testFacetCountsMatchDistribution  (gap 9)

    /// Facet happy path: index a keyword field, then facet and assert the hit
    /// counts match the upserted distribution (label a×2, b×1, summing to 3).
    func testFacetCountsMatchDistribution() throws {
        let shard = try loadWithRankedPoints("facet") // labels: a, b, a
        defer { try? shard.unload() }

        try shard.update(operation: try UpdateOperation.createFieldIndex(fieldName: "label", schema: .keyword))

        let response = try shard.facet(request: FacetRequest(key: "label", limit: 10, exact: true, filter: nil))
        var counts: [String: UInt64] = [:]
        for hit in response.hits { counts[hit.value] = hit.count }
        XCTAssertEqual(response.hits.count, 2, "two distinct labels -> two facet hits")
        XCTAssertEqual(counts["a"], 2, "label 'a' appears on points 1 and 3")
        XCTAssertEqual(counts["b"], 1, "label 'b' appears on point 2")
        XCTAssertEqual(response.hits.reduce(0) { $0 + $1.count }, 3, "facet counts must sum to all 3 points")
    }

    // MARK: - testCreateSucceedsOnFreshDirAndFailsOnOccupied  (gap 10)

    /// `EdgeShard.create` succeeds on a fresh dir but — unlike `load` — fails
    /// with a catchable `EdgeError.OperationError` on an already-initialized dir.
    func testCreateSucceedsOnFreshDirAndFailsOnOccupied() throws {
        let shardURL = testDir.appendingPathComponent("create-fresh")
        try FileManager.default.createDirectory(at: shardURL, withIntermediateDirectories: true)

        let shard = try EdgeShard.create(path: shardURL.path, config: makeConfig())
        defer { try? shard.unload() }
        try shard.update(operation: try UpdateOperation.upsertPoints(points: [
            Point(id: .numId(value: 1), vector: .single(values: [1.0, 0.0, 0.0, 0.0]), payload: nil),
        ]))
        XCTAssertEqual(try shard.count(request: CountRequest(filter: nil, exact: true)), 1, "create() then upsert should count 1")

        XCTAssertThrowsError(try EdgeShard.create(path: shardURL.path, config: makeConfig())) { error in
            guard case EdgeError.OperationError = error else {
                return XCTFail("create() on an occupied dir should throw OperationError, got \(error)")
            }
        }
    }

    // MARK: - testConfigSettersPersist  (gap 11)

    /// The three config setters apply without throwing, and the per-vector HNSW
    /// change round-trips through `config()`.
    func testConfigSettersPersist() throws {
        let shard = try loadWithThreePoints("config-setters")
        defer { try? shard.unload() }

        try shard.setHnswConfig(hnswConfig: HnswIndexConfig(
            m: 16, efConstruct: 100, fullScanThreshold: 10000, maxIndexingThreads: 1, memory: .cached, payloadM: nil
        ))
        try shard.setVectorHnswConfig(vectorName: "", hnswConfig: HnswIndexConfig(
            m: 8, efConstruct: 64, fullScanThreshold: 10000, maxIndexingThreads: 1, memory: nil, payloadM: nil
        ))
        try shard.setOptimizersConfig(optimizers: OptimizersConfig(
            deletedThreshold: 0.5, vacuumMinVectorNumber: 100, defaultSegmentNumber: 1,
            maxSegmentSizeKb: nil, indexingThresholdKb: 1000, preventUnoptimized: nil
        ))

        // The per-vector HNSW override round-trips through config().
        let hnsw = try XCTUnwrap(
            shard.config().vectorData[""]?.hnswConfig,
            "vector HNSW config should be present after setVectorHnswConfig"
        )
        XCTAssertEqual(hnsw.m, 8, "setVectorHnswConfig should persist m=8")
        XCTAssertEqual(hnsw.efConstruct, 64, "setVectorHnswConfig should persist efConstruct=64")
    }

    // MARK: - testSnapshotManifestAndBadRestore  (gap 12)

    /// `snapshotManifest()` returns a non-empty, valid-JSON manifest, and
    /// `updateFromSnapshot` with a bad path throws a catchable EdgeError while
    /// leaving the shard's data intact (no crash, no half-recovery).
    func testSnapshotManifestAndBadRestore() throws {
        let shard = try loadWithThreePoints("snapshot")
        defer { try? shard.unload() }

        let manifest = try shard.snapshotManifest()
        XCTAssertFalse(manifest.isEmpty, "snapshotManifest should return a non-empty string")
        let data = try XCTUnwrap(manifest.data(using: .utf8), "manifest should be UTF-8")
        XCTAssertNoThrow(
            try JSONSerialization.jsonObject(with: data, options: [.fragmentsAllowed]),
            "snapshot manifest must be valid JSON: \(manifest)"
        )

        XCTAssertThrowsError(
            try shard.updateFromSnapshot(snapshotPath: "/definitely/missing/nope.snapshot", tmpDir: nil)
        ) { error in
            guard case EdgeError.OperationError = error else {
                return XCTFail("a missing snapshot path should throw OperationError, got \(error)")
            }
        }
        XCTAssertEqual(try shard.info().pointsCount, 3, "shard must survive a failed restore with its data intact")
    }

    // MARK: - testPathReturnsLoadedDir  (gap 13)

    /// `path()` returns the directory the shard was loaded from.
    func testPathReturnsLoadedDir() throws {
        let shardURL = testDir.appendingPathComponent("path-check")
        try FileManager.default.createDirectory(at: shardURL, withIntermediateDirectories: true)
        let shard = try EdgeShard.load(path: shardURL.path, config: makeConfig())
        defer { try? shard.unload() }

        XCTAssertEqual(try shard.path(), shardURL.path, "path() should echo the dir the shard was loaded from")
    }

    // MARK: - *ByFilter update family  (gap 14)

    /// `deletePointsByFilter` removes only the points matching the filter.
    func testDeletePointsByFilterRemovesMatchingOnly() throws {
        let shard = try loadWithRankedPoints("del-points-by-filter") // labels: a, b, a
        defer { try? shard.unload() }

        try shard.update(operation: try UpdateOperation.deletePointsByFilter(filter: labelFilter("a")))
        XCTAssertEqual(try shard.count(request: CountRequest(filter: nil, exact: true)), 1, "deleting label=a (2 points) leaves 1")

        let remaining = try shard.scroll(request: ScrollRequest(
            offset: nil, limit: 10, filter: nil,
            withPayload: .bool(enable: false), withVector: .bool(enable: false), orderBy: nil
        ))
        XCTAssertEqual(remaining.records.count, 1)
        if case let .numId(v) = remaining.records.first?.id {
            XCTAssertEqual(v, 2, "only the label=b point (id 2) should survive")
        } else {
            XCTFail("survivor id should be numId(2)")
        }
    }

    /// `deletePayloadByFilter` removes a named key only from matching points.
    func testDeletePayloadByFilterRemovesKeyOnMatchingOnly() throws {
        let shard = try loadWithRankedPoints("del-payload-by-filter")
        defer { try? shard.unload() }

        try shard.update(operation: try UpdateOperation.deletePayloadByFilter(filter: labelFilter("a"), keys: ["rank"]))

        let p1 = try shard.retrieve(request: RetrieveRequest(pointIds: [.numId(value: 1)], withPayload: .bool(enable: true), withVector: nil))
        let p1Payload = try XCTUnwrap(p1.first?.payload)
        XCTAssertFalse(p1Payload.contains("\"rank\""), "matching point 1 should lose 'rank': \(p1Payload)")
        XCTAssertTrue(p1Payload.contains("\"label\""), "matching point 1 keeps 'label': \(p1Payload)")

        let p2 = try shard.retrieve(request: RetrieveRequest(pointIds: [.numId(value: 2)], withPayload: .bool(enable: true), withVector: nil))
        XCTAssertTrue((p2.first?.payload ?? "").contains("\"rank\""), "non-matching point 2 keeps 'rank': \(p2.first?.payload ?? "")")
    }

    /// `clearPayloadByFilter` empties the payload only on matching points.
    func testClearPayloadByFilterEmptiesMatchingOnly() throws {
        let shard = try loadWithRankedPoints("clear-payload-by-filter")
        defer { try? shard.unload() }

        try shard.update(operation: try UpdateOperation.clearPayloadByFilter(filter: labelFilter("a")))

        let p1 = try shard.retrieve(request: RetrieveRequest(pointIds: [.numId(value: 1)], withPayload: .bool(enable: true), withVector: nil))
        let p1Payload = p1.first?.payload ?? "{}"
        XCTAssertFalse(p1Payload.contains("\"label\""), "matching point 1 payload should be cleared: \(p1Payload)")

        let p2 = try shard.retrieve(request: RetrieveRequest(pointIds: [.numId(value: 2)], withPayload: .bool(enable: true), withVector: nil))
        XCTAssertTrue((p2.first?.payload ?? "").contains("\"label\""), "non-matching point 2 keeps its payload: \(p2.first?.payload ?? "")")
    }

    /// `overwritePayloadByFilter` replaces the whole payload of matching points.
    func testOverwritePayloadByFilterReplacesMatchingOnly() throws {
        let shard = try loadWithRankedPoints("overwrite-payload-by-filter")
        defer { try? shard.unload() }

        try shard.update(operation: try UpdateOperation.overwritePayloadByFilter(filter: labelFilter("b"), payloadJson: "{\"tag\": \"hot\"}"))

        // Point 2 (label b) is fully replaced: gains 'tag', loses 'rank'/'label'.
        let p2 = try shard.retrieve(request: RetrieveRequest(pointIds: [.numId(value: 2)], withPayload: .bool(enable: true), withVector: nil))
        let p2Payload = try XCTUnwrap(p2.first?.payload)
        XCTAssertTrue(p2Payload.contains("\"tag\""), "overwrite should set 'tag': \(p2Payload)")
        XCTAssertFalse(p2Payload.contains("\"rank\""), "overwrite should drop the old 'rank' key: \(p2Payload)")

        // Point 1 (label a) untouched.
        let p1 = try shard.retrieve(request: RetrieveRequest(pointIds: [.numId(value: 1)], withPayload: .bool(enable: true), withVector: nil))
        XCTAssertFalse((p1.first?.payload ?? "").contains("\"tag\""), "non-matching point 1 should be untouched: \(p1.first?.payload ?? "")")
    }

    /// `deleteVectorsByFilter` drops a named vector only from matching points.
    func testDeleteVectorsByFilterRemovesNamedVectorOnMatchingOnly() throws {
        let shard = try loadWithRankedPoints("del-vectors-by-filter") // labels: a, b, a
        defer { try? shard.unload() }

        try shard.update(operation: try UpdateOperation.createDenseVector(vectorName: "extra", size: 2, distance: .dot))
        // Give "extra" to point 1 (label a) and point 2 (label b).
        try shard.update(operation: try UpdateOperation.updateVectors(pointVectors: [
            PointVectors(id: .numId(value: 1), vector: .named(map: ["extra": .dense(values: [5.0, 6.0])])),
            PointVectors(id: .numId(value: 2), vector: .named(map: ["extra": .dense(values: [5.0, 6.0])])),
        ]))

        let before = try shard.search(request: SearchRequest(
            query: .nearest(vector: .dense(values: [5.0, 6.0]), using: "extra"),
            limit: 10, offset: nil, filter: nil, params: nil, withVector: nil, withPayload: nil, scoreThreshold: nil
        ))
        XCTAssertEqual(before.count, 2, "points 1 and 2 both carry the 'extra' vector")

        // Delete "extra" from label=a points (1 and 3); only point 1 actually had it.
        try shard.update(operation: try UpdateOperation.deleteVectorsByFilter(filter: labelFilter("a"), vectorNames: ["extra"]))

        let after = try shard.search(request: SearchRequest(
            query: .nearest(vector: .dense(values: [5.0, 6.0]), using: "extra"),
            limit: 10, offset: nil, filter: nil, params: nil, withVector: nil, withPayload: nil, scoreThreshold: nil
        ))
        XCTAssertEqual(after.count, 1, "only point 2 keeps 'extra' after deleting it from label=a")
        if case let .numId(v) = after.first?.id {
            XCTAssertEqual(v, 2, "the surviving 'extra' vector belongs to point 2 (label b)")
        } else {
            XCTFail("remaining 'extra' vector should belong to id 2")
        }
    }

    // MARK: - testDeleteFieldIndexDisablesFacet  (gap 15)

    /// `deleteFieldIndex` removes a payload index: faceting works while it
    /// exists and errors once it is dropped.
    func testDeleteFieldIndexDisablesFacet() throws {
        let shard = try loadWithRankedPoints("delete-field-index")
        defer { try? shard.unload() }

        try shard.update(operation: try UpdateOperation.createFieldIndex(fieldName: "label", schema: .keyword))
        let response = try shard.facet(request: FacetRequest(key: "label", limit: 10, exact: true, filter: nil))
        XCTAssertEqual(response.hits.reduce(0) { $0 + $1.count }, 3, "facet works while the index exists")

        try shard.update(operation: try UpdateOperation.deleteFieldIndex(fieldName: "label"))
        XCTAssertThrowsError(
            try shard.facet(request: FacetRequest(key: "label", limit: 10, exact: true, filter: nil))
        ) { error in
            XCTAssertTrue(error is EdgeError, "faceting a dropped index should throw a catchable EdgeError, got \(type(of: error))")
        }
    }

    // MARK: - testDeleteVectorNameRemovesField  (gap 16)

    /// `deleteVectorName` removes a named dense vector field: config() stops
    /// listing it and searching it fails.
    func testDeleteVectorNameRemovesField() throws {
        let shard = try loadWithThreePoints("delete-vector-name")
        defer { try? shard.unload() }

        try shard.update(operation: try UpdateOperation.createDenseVector(vectorName: "extra", size: 2, distance: .dot))
        XCTAssertNotNil(try shard.config().vectorData["extra"], "created named vector should be listed by config()")

        try shard.update(operation: UpdateOperation.deleteVectorName(vectorName: "extra"))
        XCTAssertNil(try shard.config().vectorData["extra"], "deleted named vector should no longer be listed by config()")

        XCTAssertThrowsError(
            try shard.search(request: SearchRequest(
                query: .nearest(vector: .dense(values: [5.0, 6.0]), using: "extra"),
                limit: 10, offset: nil, filter: nil, params: nil, withVector: nil, withPayload: nil, scoreThreshold: nil
            ))
        ) { error in
            XCTAssertTrue(error is EdgeError, "searching a deleted vector name should throw EdgeError, got \(type(of: error))")
        }
    }

    // MARK: - testDatetimeOrderByScroll  (gap 17)

    /// Order-by scroll on a datetime-typed payload field with a datetime
    /// `StartFrom`. Mirrors the integer order-by test: the inclusive start bound
    /// filters to points at/after it, ascending.
    func testDatetimeOrderByScroll() throws {
        let shardURL = testDir.appendingPathComponent("datetime-orderby")
        try FileManager.default.createDirectory(at: shardURL, withIntermediateDirectories: true)
        let shard = try EdgeShard.load(path: shardURL.path, config: makeConfig())
        defer { try? shard.unload() }

        try shard.update(operation: try UpdateOperation.upsertPoints(points: [
            Point(id: .numId(value: 1), vector: .single(values: [1.0, 0.0, 0.0, 0.0]), payload: "{\"ts\": \"2021-01-01T00:00:00Z\"}"),
            Point(id: .numId(value: 2), vector: .single(values: [0.0, 1.0, 0.0, 0.0]), payload: "{\"ts\": \"2022-01-01T00:00:00Z\"}"),
            Point(id: .numId(value: 3), vector: .single(values: [0.0, 0.0, 1.0, 0.0]), payload: "{\"ts\": \"2023-01-01T00:00:00Z\"}"),
        ]))
        try shard.update(operation: try UpdateOperation.createFieldIndex(fieldName: "ts", schema: .datetime))

        // Ascending by datetime, inclusive start at 2022 -> points 2 and 3.
        let page = try shard.scroll(request: ScrollRequest(
            offset: nil, limit: 10, filter: nil,
            withPayload: .bool(enable: true), withVector: .bool(enable: false),
            orderBy: OrderBy(key: "ts", direction: .asc, startFrom: .datetime(value: "2022-01-01T00:00:00Z"))
        ))
        XCTAssertEqual(page.records.count, 2, "datetime StartFrom (inclusive at 2022) should include only 2022 and 2023")
        let ids = page.records.compactMap { r -> UInt64? in
            if case let .numId(v) = r.id { return v } else { return nil }
        }
        XCTAssertEqual(ids, [2, 3], "ascending datetime order should yield 2022 then 2023")
        XCTAssertTrue((page.records.first?.payload ?? "").contains("2022"), "first record should be the 2022 point: \(page.records.first?.payload ?? "")")
        // order_value is populated; datetime sorts as a numeric timestamp under the hood.
        let orderValue = try XCTUnwrap(page.records.first?.orderValue, "datetime order-by should populate order_value")
        switch orderValue {
        case .int, .float:
            break // accept whichever numeric form the engine surfaces for datetimes
        }
    }
}
