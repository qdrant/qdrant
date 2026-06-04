package tech.qdrant.edge.example

import android.app.Activity
import android.os.Bundle
import android.util.Log
import java.io.File
import java.util.UUID

import tech.qdrant.edge.*

private const val TAG = "QdrantEdge"

class MainActivity : Activity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        Thread {
            try {
                runDemo()
            } catch (e: Exception) {
                Log.e(TAG, "Demo failed", e)
            }
        }.start()
    }

    private fun runDemo() {
        val dataDir = File(cacheDir, "qdrant-edge-android-example")
        if (dataDir.exists()) dataDir.deleteRecursively()
        dataDir.mkdirs()

        // ── Load shard ──────────────────────────────────────────────────
        Log.i(TAG, "---- Load shard ----")

        val config = EdgeConfig(
            vectorData = mapOf(
                "" to VectorDataConfig(
                    size = 4uL,
                    distance = Distance.DOT,
                    quantizationConfig = null,
                    multivectorConfig = null,
                    datatype = null,
                )
            ),
            sparseVectorData = emptyMap(),
        )

        val shard = EdgeShard.load(path = dataDir.absolutePath, config = config)
        Log.i(TAG, "Shard loaded at: ${dataDir.absolutePath}")

        // ── Upsert ──────────────────────────────────────────────────────
        Log.i(TAG, "---- Upsert ----")

        val randomUuid = UUID.randomUUID().toString()

        val upsertOp = UpdateOperation.upsertPoints(
            points = listOf(
                Point(
                    id = PointId.NumId(1uL),
                    vector = Vector.Single(listOf(6.0f, 9.0f, 4.0f, 2.0f)),
                    payload = """{"null":null,"str":"string","uint":42,"int":-69,"float":4.2,"bool":true}""",
                ),
                Point(
                    id = PointId.Uuid("e9408f2b-b917-4af1-ab75-d97ac6b2c047"),
                    vector = Vector.Single(listOf(6.0f, 9.0f, 3.0f, -2.0f)),
                    payload = """{"hello":"world","price":199.99}""",
                ),
                Point(
                    id = PointId.Uuid(randomUuid),
                    vector = Vector.Single(listOf(1.0f, 6.0f, 4.0f, 2.0f)),
                    payload = """{"hello":"world","price":999.99}""",
                ),
            ),
        )

        shard.update(operation = upsertOp)
        Log.i(TAG, "Upserted 3 points (ids: 1, e9408f2b-..., ${randomUuid.take(8)}...)")

        // ── Query ───────────────────────────────────────────────────────
        Log.i(TAG, "---- Query ----")

        val queryResults = shard.query(
            request = QueryRequest(
                limit = 10uL,
                offset = null,
                query = ScoringQuery.Vector(Query.Nearest(
                    vector = listOf(6.0f, 9.0f, 4.0f, 2.0f),
                    using = null,
                )),
                prefetches = emptyList(),
                withVector = WithVector.Bool(true),
                withPayload = WithPayload.Bool(true),
                filter = null,
                scoreThreshold = null,
                params = null,
            ),
        )

        Log.i(TAG, "Query returned ${queryResults.size} results:")
        queryResults.forEach { printScoredPoint(it) }

        // ── Search ──────────────────────────────────────────────────────
        Log.i(TAG, "---- Search ----")

        val searchResults = shard.search(
            request = SearchRequest(
                query = Query.Nearest(
                    vector = listOf(1.0f, 1.0f, 1.0f, 1.0f),
                    using = null,
                ),
                limit = 10uL,
                offset = null,
                filter = null,
                params = null,
                withVector = WithVector.Bool(true),
                withPayload = WithPayload.Bool(true),
                scoreThreshold = null,
            ),
        )

        Log.i(TAG, "Search returned ${searchResults.size} results:")
        searchResults.forEach { printScoredPoint(it) }

        // ── Search + Filter ─────────────────────────────────────────────
        Log.i(TAG, "---- Search + Filter ----")

        val searchFilter = Filter(
            must = listOf(
                Condition.Field(
                    FieldCondition(
                        key = "hello",
                        `match` = Match.Text("world"),
                        range = null,
                        geoBoundingBox = null,
                        geoRadius = null,
                        valuesCount = null,
                    )
                ),
                Condition.Field(
                    FieldCondition(
                        key = "price",
                        `match` = null,
                        range = RangeFloat(gte = 500.0, gt = null, lte = null, lt = null),
                        geoBoundingBox = null,
                        geoRadius = null,
                        valuesCount = null,
                    )
                ),
            ),
            should = null,
            mustNot = null,
        )

        val filteredResults = shard.search(
            request = SearchRequest(
                query = Query.Nearest(
                    vector = listOf(1.0f, 1.0f, 1.0f, 1.0f),
                    using = null,
                ),
                limit = 10uL,
                offset = null,
                filter = searchFilter,
                params = null,
                withVector = WithVector.Bool(true),
                withPayload = WithPayload.Bool(true),
                scoreThreshold = null,
            ),
        )

        Log.i(TAG, "Filtered search returned ${filteredResults.size} results:")
        filteredResults.forEach { printScoredPoint(it) }

        // ── Retrieve ────────────────────────────────────────────────────
        Log.i(TAG, "---- Retrieve ----")

        val retrieved = shard.retrieve(
            pointIds = listOf(PointId.NumId(1uL)),
            withPayload = WithPayload.Bool(true),
            withVector = WithVector.Bool(true),
        )

        Log.i(TAG, "Retrieved ${retrieved.size} records:")
        retrieved.forEach { printRecord(it) }

        // ── Scroll ──────────────────────────────────────────────────────
        Log.i(TAG, "---- Scroll ----")

        var scrollResponse = shard.scroll(
            request = ScrollRequest(
                offset = null,
                limit = 2uL,
                filter = null,
                withPayload = null,
                withVector = null,
                orderBy = null,
            ),
        )

        Log.i(TAG, "Scroll page 1 (${scrollResponse.records.size} records):")
        scrollResponse.records.forEach { printRecord(it) }

        while (scrollResponse.nextOffset != null) {
            val nextOffset = scrollResponse.nextOffset!!
            Log.i(TAG, "--- Next scroll (offset = ${pointIdStr(nextOffset)}) ---")
            scrollResponse = shard.scroll(
                request = ScrollRequest(
                    offset = nextOffset,
                    limit = 2uL,
                    filter = null,
                    withPayload = null,
                    withVector = null,
                    orderBy = null,
                ),
            )
            scrollResponse.records.forEach { printRecord(it) }
        }

        // ── Count ───────────────────────────────────────────────────────
        Log.i(TAG, "---- Count ----")

        val count = shard.count(request = CountRequest(filter = null, exact = true))
        Log.i(TAG, "Total points count: $count")

        // ── Facet ───────────────────────────────────────────────────────
        Log.i(TAG, "---- Facet (requires payload index) ----")

        try {
            val facetResponse = shard.facet(
                request = FacetRequest(
                    key = "hello",
                    limit = 10uL,
                    exact = false,
                    filter = null,
                ),
            )
            Log.i(TAG, "Facet results (${facetResponse.hits.size} hits):")
            facetResponse.hits.forEach { hit ->
                Log.i(TAG, "  ${hit.value}: ${hit.count}")
            }
        } catch (e: Exception) {
            Log.i(TAG, "Facet error (expected without payload index): $e")
        }

        // ── Info ────────────────────────────────────────────────────────
        Log.i(TAG, "---- Info ----")

        val info = shard.info()
        Log.i(TAG, "Segments: ${info.segmentsCount}, Points: ${info.pointsCount}, Indexed vectors: ${info.indexedVectorsCount}")

        // ── Close and reopen ────────────────────────────────────────────
        Log.i(TAG, "---- Close and reopen shard ----")

        shard.unload()
        Log.i(TAG, "Shard closed.")

        val reopenedShard = EdgeShard.load(path = dataDir.absolutePath, config = null)
        val reopenedInfo = reopenedShard.info()
        Log.i(TAG, "Reopened shard - Segments: ${reopenedInfo.segmentsCount}, Points: ${reopenedInfo.pointsCount}, Indexed vectors: ${reopenedInfo.indexedVectorsCount}")

        reopenedShard.unload()
        Log.i(TAG, "Done!")
    }

    private fun pointIdStr(id: PointId): String = when (id) {
        is PointId.NumId -> id.`value`.toString()
        is PointId.Uuid -> id.`value`
    }

    private fun printScoredPoint(p: ScoredPoint) {
        val idStr = pointIdStr(p.id)
        Log.i(TAG, "  id: $idStr, version: ${p.version}, score: ${p.score}")
        p.payload?.let { Log.i(TAG, "    payload: $it") }
        p.vector?.let { Log.i(TAG, "    vector: $it") }
    }

    private fun printRecord(r: Record) {
        val idStr = pointIdStr(r.id)
        Log.i(TAG, "  id: $idStr")
        r.payload?.let { Log.i(TAG, "    payload: $it") }
        r.vector?.let { Log.i(TAG, "    vector: $it") }
    }
}
