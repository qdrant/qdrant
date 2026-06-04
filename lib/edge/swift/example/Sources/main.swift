import Foundation
import QdrantEdge

// MARK: - Helpers

func toJson(_ dict: [String: Any]) -> String {
    let data = try! JSONSerialization.data(withJSONObject: dict)
    return String(data: data, encoding: .utf8)!
}

func printScoredPoint(_ p: ScoredPoint) {
    let idStr: String
    switch p.id {
    case let .numId(value): idStr = "\(value)"
    case let .uuid(value): idStr = value
    }
    print("  id: \(idStr), version: \(p.version), score: \(p.score)")
    if let payload = p.payload { print("    payload: \(payload)") }
    if let vector = p.vector { print("    vector: \(vector)") }
}

func printRecord(_ r: Record) {
    let idStr: String
    switch r.id {
    case let .numId(value): idStr = "\(value)"
    case let .uuid(value): idStr = value
    }
    print("  id: \(idStr)")
    if let payload = r.payload { print("    payload: \(payload)") }
    if let vector = r.vector { print("    vector: \(vector)") }
}

func pointIdDescription(_ id: PointId) -> String {
    switch id {
    case let .numId(value): return "\(value)"
    case let .uuid(value): return value
    }
}

// MARK: - Setup

let dataDir = FileManager.default.temporaryDirectory
    .appendingPathComponent("qdrant-edge-swift-example")
    .path

// Clean up any previous run
if FileManager.default.fileExists(atPath: dataDir) {
    try FileManager.default.removeItem(atPath: dataDir)
}

try FileManager.default.createDirectory(atPath: dataDir, withIntermediateDirectories: true)

// MARK: - Load shard

print("---- Load shard ----")

let config = EdgeConfig(
    vectorData: [
        "": VectorDataConfig(
            size: 4,
            distance: .dot,
            quantizationConfig: nil,
            multivectorConfig: nil,
            datatype: nil
        ),
    ],
    sparseVectorData: [:]
)

let shard = try EdgeShard.load(path: dataDir, config: config)
print("Shard loaded at: \(dataDir)")

// MARK: - Upsert

print("\n---- Upsert ----")

let randomUuid = UUID().uuidString

let upsertOp = try UpdateOperation.upsertPoints(points: [
    Point(
        id: .numId(value: 1),
        vector: .single(values: [6.0, 9.0, 4.0, 2.0]),
        payload: toJson([
            "null": NSNull(),
            "str": "string",
            "uint": 42,
            "int": -69,
            "float": 4.20,
            "bool": true,
            "obj": [
                "null": NSNull(),
                "str": "string",
                "uint": 42,
                "int": -69,
                "float": 4.20,
                "bool": true,
                "obj": [String: Any](),
                "arr": [Any](),
            ] as [String: Any],
            "arr": [NSNull(), "string", 42, -69, 4.20, true, [String: Any](), [Any]()] as [Any],
        ] as [String: Any])
    ),
    Point(
        id: .uuid(value: "e9408f2b-b917-4af1-ab75-d97ac6b2c047"),
        vector: .single(values: [6.0, 9.0, 3.0, -2.0]),
        payload: toJson([
            "hello": "world",
            "price": 199.99,
        ])
    ),
    Point(
        id: .uuid(value: randomUuid),
        vector: .single(values: [1.0, 6.0, 4.0, 2.0]),
        payload: toJson([
            "hello": "world",
            "price": 999.99,
        ])
    ),
])

try shard.update(operation: upsertOp)
print("Upserted 3 points (ids: 1, e9408f2b-..., \(randomUuid.prefix(8))...)")

// MARK: - Query

print("\n---- Query ----")

let queryResults = try shard.query(request: QueryRequest(
    limit: 10,
    offset: nil,
    query: .vector(query: .nearest(vector: [6.0, 9.0, 4.0, 2.0], using: nil)),
    prefetches: [],
    withVector: .bool(enable: true),
    withPayload: .bool(enable: true),
    filter: nil,
    scoreThreshold: nil,
    params: nil
))

print("Query returned \(queryResults.count) results:")
for point in queryResults {
    printScoredPoint(point)
}

// MARK: - Search

print("\n---- Search ----")

let searchResults = try shard.search(request: SearchRequest(
    query: .nearest(vector: [1.0, 1.0, 1.0, 1.0], using: nil),
    limit: 10,
    offset: nil,
    filter: nil,
    params: nil,
    withVector: .bool(enable: true),
    withPayload: .bool(enable: true),
    scoreThreshold: nil
))

print("Search returned \(searchResults.count) results:")
for point in searchResults {
    printScoredPoint(point)
}

// MARK: - Search + Filter

print("\n---- Search + Filter ----")

let searchFilter = Filter(
    must: [
        .field(condition: FieldCondition(
            key: "hello",
            match: .text(text: "world"),
            range: nil,
            geoBoundingBox: nil,
            geoRadius: nil,
            geoPolygon: nil,
            valuesCount: nil
        )),
        .field(condition: FieldCondition(
            key: "price",
            match: nil,
            range: RangeFloat(gte: 500.0, gt: nil, lte: nil, lt: nil),
            geoBoundingBox: nil,
            geoRadius: nil,
            geoPolygon: nil,
            valuesCount: nil
        )),
    ],
    should: nil,
    mustNot: nil
)

let filteredResults = try shard.search(request: SearchRequest(
    query: .nearest(vector: [1.0, 1.0, 1.0, 1.0], using: nil),
    limit: 10,
    offset: nil,
    filter: searchFilter,
    params: nil,
    withVector: .bool(enable: true),
    withPayload: .bool(enable: true),
    scoreThreshold: nil
))

print("Filtered search returned \(filteredResults.count) results:")
for point in filteredResults {
    printScoredPoint(point)
}

// MARK: - Retrieve

print("\n---- Retrieve ----")

let retrieved = try shard.retrieve(
    pointIds: [.numId(value: 1)],
    withPayload: .bool(enable: true),
    withVector: .bool(enable: true)
)

print("Retrieved \(retrieved.count) records:")
for record in retrieved {
    printRecord(record)
}

// MARK: - Scroll

print("\n---- Scroll ----")

var scrollResponse = try shard.scroll(request: ScrollRequest(
    offset: nil,
    limit: 2,
    filter: nil,
    withPayload: nil,
    withVector: nil,
    orderBy: nil
))

print("Scroll page 1 (\(scrollResponse.records.count) records):")
for record in scrollResponse.records {
    printRecord(record)
}

while let nextOffset = scrollResponse.nextOffset {
    print("--- Next scroll (offset = \(pointIdDescription(nextOffset))) ---")
    scrollResponse = try shard.scroll(request: ScrollRequest(
        offset: nextOffset,
        limit: 2,
        filter: nil,
        withPayload: nil,
        withVector: nil,
        orderBy: nil
    ))
    for record in scrollResponse.records {
        printRecord(record)
    }
}

// MARK: - Count

print("\n---- Count ----")

let count = try shard.count(request: CountRequest(filter: nil, exact: true))
print("Total points count: \(count)")

// MARK: - Facet

print("\n---- Facet (requires payload index) ----")

// Facet requires a payload index on the field being faceted.
// Without a payload index this will fail, which is expected.
do {
    let facetResponse = try shard.facet(request: FacetRequest(
        key: "hello",
        limit: 10,
        exact: false,
        filter: nil
    ))
    print("Facet results (\(facetResponse.hits.count) hits):")
    for hit in facetResponse.hits {
        print("  \(hit.value): \(hit.count)")
    }
} catch {
    print("Facet error (expected without payload index): \(error)")
}

// MARK: - Info

print("\n---- Info ----")

let info = try shard.info()
print("Segments: \(info.segmentsCount), Points: \(info.pointsCount), Indexed vectors: \(info.indexedVectorsCount)")

// MARK: - Close and reopen

print("\n---- Close and reopen shard ----")

shard.unload()
print("Shard closed.")

let reopenedShard = try EdgeShard.load(path: dataDir, config: nil)
let reopenedInfo = try reopenedShard.info()
print("Reopened shard - Segments: \(reopenedInfo.segmentsCount), Points: \(reopenedInfo.pointsCount), Indexed vectors: \(reopenedInfo.indexedVectorsCount)")

reopenedShard.unload()
print("\nDone!")
