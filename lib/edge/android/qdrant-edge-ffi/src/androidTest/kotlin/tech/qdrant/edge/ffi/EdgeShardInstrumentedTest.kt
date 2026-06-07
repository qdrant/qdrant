package tech.qdrant.edge.ffi

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import java.io.File

/**
 * Instrumented tests for the Qdrant Edge Android SDK.
 *
 * These tests exercise the real native .so via JNI, verifying the full chain:
 * UniFFI Kotlin bindings → JNA → libqdrant_edge_ffi.so.
 *
 * Note: all imports are from `tech.qdrant.edge.ffi.*` directly (same package
 * as this test). The public `tech.qdrant.edge.*` facade re-exports types via
 * typealias, but a Kotlin typealias does NOT carry a sealed class's nested
 * variants (`PointId.NumId`, `Vector.Single`, `Query.Nearest`), so we use the
 * ffi package throughout — matching the approach used in MainActivity.kt.
 */
@RunWith(AndroidJUnit4::class)
class EdgeShardInstrumentedTest {

    private val context
        get() = InstrumentationRegistry.getInstrumentation().targetContext

    /** Helper: build a fresh temp directory isolated to the given test name. */
    private fun freshDir(name: String): File {
        val dir = File(context.cacheDir, "qdrant-edge-test-$name")
        if (dir.exists()) dir.deleteRecursively()
        dir.mkdirs()
        return dir
    }

    /** Standard 4-dimensional DOT config used across all tests. */
    private fun defaultConfig() = EdgeConfig(
        vectorData = mapOf(
            "" to VectorDataConfig(
                size = 4uL,
                distance = Distance.DOT,
                quantizationConfig = null,
                multivectorConfig = null,
                datatype = null,
                hnswConfig = null,
            )
        ),
        sparseVectorData = emptyMap(),
    )

    // ── Test 1: loadUpsertSearchClose ──────────────────────────────────────

    /**
     * Loads a new shard, upserts 3 points, issues a nearest-neighbour search,
     * asserts at least one result is returned, then unloads the shard.
     *
     * Verifies: EdgeShard.load, shard.update(upsertPoints), shard.search,
     * ScoredPoint.id, shard.unload.
     */
    @Test
    fun loadUpsertSearchClose() {
        val dir = freshDir("load-upsert-search")

        val shard = EdgeShard.load(path = dir.absolutePath, config = defaultConfig())

        val upsertOp = UpdateOperation.upsertPoints(
            points = listOf(
                Point(
                    id = PointId.NumId(1uL),
                    vector = Vector.Single(listOf(6.0f, 9.0f, 4.0f, 2.0f)),
                    payload = """{"tag":"alpha"}""",
                ),
                Point(
                    id = PointId.NumId(2uL),
                    vector = Vector.Single(listOf(1.0f, 1.0f, 1.0f, 1.0f)),
                    payload = """{"tag":"beta"}""",
                ),
                Point(
                    id = PointId.NumId(3uL),
                    vector = Vector.Single(listOf(0.5f, 0.1f, 9.0f, 3.0f)),
                    payload = """{"tag":"gamma"}""",
                ),
            ),
        )
        shard.update(operation = upsertOp)

        val results = shard.search(
            request = SearchRequest(
                query = Query.Nearest(
                    vector = listOf(6.0f, 9.0f, 4.0f, 2.0f),
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

        assertTrue("search should return at least one result", results.isNotEmpty())
        // The point most similar to the query vector [6,9,4,2] should be id=1 (exact match)
        val topId = results.first().id
        assertTrue("top result should be PointId.NumId(1)", topId is PointId.NumId && (topId as PointId.NumId).`value` == 1uL)

        shard.unload()
    }

    // ── Test 2: persistenceAcrossReload ───────────────────────────────────

    /**
     * Upserts points into a shard, flushes, unloads, reopens with config=null
     * (read-existing mode), and asserts the point count is preserved.
     *
     * Verifies: shard.flush, shard.unload, EdgeShard.load(path, null),
     * shard.info().pointsCount.
     */
    @Test
    fun persistenceAcrossReload() {
        val dir = freshDir("persistence")

        val shard = EdgeShard.load(path = dir.absolutePath, config = defaultConfig())

        val upsertOp = UpdateOperation.upsertPoints(
            points = listOf(
                Point(
                    id = PointId.NumId(10uL),
                    vector = Vector.Single(listOf(1.0f, 2.0f, 3.0f, 4.0f)),
                    payload = null,
                ),
                Point(
                    id = PointId.NumId(20uL),
                    vector = Vector.Single(listOf(4.0f, 3.0f, 2.0f, 1.0f)),
                    payload = null,
                ),
            ),
        )
        shard.update(operation = upsertOp)
        shard.flush()
        shard.unload()

        // Reopen without supplying a config (existing shard, reads its own metadata)
        val reopened = EdgeShard.load(path = dir.absolutePath, config = null)
        val info = reopened.info()

        assertEquals("pointsCount should be 2 after reload", 2uL, info.pointsCount)

        reopened.unload()
    }

    // ── Test 3: invalidUuidThrows ──────────────────────────────────────────

    /**
     * Attempts to delete a point using a syntactically invalid UUID string and
     * asserts that an EdgeException is thrown (not a crash / unhandled panic).
     *
     * Verifies: UpdateOperation.deletePoints with PointId.Uuid("not-a-uuid")
     * surfaces as a catchable EdgeException rather than aborting the process.
     */
    @Test
    fun invalidUuidThrows() {
        val dir = freshDir("invalid-uuid")

        val shard = EdgeShard.load(path = dir.absolutePath, config = defaultConfig())

        var exceptionCaught = false
        try {
            val deleteOp = UpdateOperation.deletePoints(
                pointIds = listOf(PointId.Uuid("not-a-uuid")),
            )
            shard.update(operation = deleteOp)
        } catch (e: EdgeException) {
            exceptionCaught = true
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }

        assertTrue(
            "EdgeException should be thrown for invalid UUID, not a process crash",
            exceptionCaught,
        )
    }

    // ── Test 4: turboQuantizationLoadsAndSearches ──────────────────────────

    /**
     * On-device proof that Turbo quantization (exposed for parity with the
     * Python Edge SDK) works through the real JNI -> .so -> UniFFI path: load a
     * Turbo-quantized shard, upsert, search, and assert a result comes back.
     */
    @Test
    fun turboQuantizationLoadsAndSearches() {
        val dir = freshDir("turbo-quant")

        val config = EdgeConfig(
            vectorData = mapOf(
                "" to VectorDataConfig(
                    size = 4uL,
                    distance = Distance.DOT,
                    quantizationConfig = QuantizationConfig.Turbo(
                        config = TurboQuantizationParams(
                            alwaysRam = true,
                            bits = TurboQuantBitSize.BITS4,
                        ),
                    ),
                    multivectorConfig = null,
                    datatype = null,
                    hnswConfig = null,
                )
            ),
            sparseVectorData = emptyMap(),
        )

        val shard = EdgeShard.load(path = dir.absolutePath, config = config)
        try {
            shard.update(
                operation = UpdateOperation.upsertPoints(
                    points = listOf(
                        Point(
                            id = PointId.NumId(1uL),
                            vector = Vector.Single(listOf(6.0f, 9.0f, 4.0f, 2.0f)),
                            payload = null,
                        ),
                    ),
                ),
            )

            val results = shard.search(
                request = SearchRequest(
                    query = Query.Nearest(vector = listOf(6.0f, 9.0f, 4.0f, 2.0f), using = null),
                    limit = 10uL,
                    offset = null,
                    filter = null,
                    params = null,
                    withVector = null,
                    withPayload = null,
                    scoreThreshold = null,
                ),
            )
            assertTrue("Turbo-quantized search should return a result", results.isNotEmpty())
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Helper: load a shard with 3 points (ids 1,2,3) ─────────────────────

    private fun loadWithThreePoints(name: String): EdgeShard {
        val dir = freshDir(name)
        val shard = EdgeShard.load(path = dir.absolutePath, config = defaultConfig())
        shard.update(
            operation = UpdateOperation.upsertPoints(
                points = listOf(
                    Point(PointId.NumId(1uL), Vector.Single(listOf(1.0f, 0.0f, 0.0f, 0.0f)), """{"label":"a"}"""),
                    Point(PointId.NumId(2uL), Vector.Single(listOf(0.0f, 1.0f, 0.0f, 0.0f)), """{"label":"b"}"""),
                    Point(PointId.NumId(3uL), Vector.Single(listOf(0.0f, 0.0f, 1.0f, 0.0f)), """{"label":"c"}"""),
                ),
            ),
        )
        return shard
    }

    // ── Test 5: deleteReducesCount ─────────────────────────────────────────

    @Test
    fun deleteReducesCount() {
        val shard = loadWithThreePoints("delete")
        try {
            shard.update(operation = UpdateOperation.deletePoints(pointIds = listOf(PointId.NumId(2uL))))

            val count = shard.count(request = CountRequest(filter = null, exact = true))
            assertEquals("count should drop to 2 after delete", 2uL, count)

            val got = shard.retrieve(pointIds = listOf(PointId.NumId(2uL)), withPayload = null, withVector = null)
            assertTrue("deleted point should not be retrievable", got.isEmpty())
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 6: setPayloadVisibleOnRetrieve ────────────────────────────────

    @Test
    fun setPayloadVisibleOnRetrieve() {
        val shard = loadWithThreePoints("set-payload")
        try {
            shard.update(
                operation = UpdateOperation.setPayload(
                    pointIds = listOf(PointId.NumId(1uL)),
                    payloadJson = """{"tag":"hot"}""",
                ),
            )

            val got = shard.retrieve(
                pointIds = listOf(PointId.NumId(1uL)),
                withPayload = WithPayload.Bool(true),
                withVector = null,
            )
            assertEquals(1, got.size)
            val payload = got.first().payload ?: ""
            assertTrue("new key 'tag' should be visible: $payload", payload.contains("\"tag\""))
            assertTrue("original 'label' should survive merge: $payload", payload.contains("\"label\""))
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 7: scrollReturnsAllPoints ─────────────────────────────────────

    @Test
    fun scrollReturnsAllPoints() {
        val shard = loadWithThreePoints("scroll")
        try {
            val page = shard.scroll(
                request = ScrollRequest(
                    offset = null,
                    limit = 10uL,
                    filter = null,
                    withPayload = WithPayload.Bool(false),
                    withVector = WithVector.Bool(false),
                    orderBy = null,
                ),
            )
            assertEquals("scroll should return all 3 points", 3, page.records.size)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 8: queryReturnsRankedResults ──────────────────────────────────

    @Test
    fun queryReturnsRankedResults() {
        val shard = loadWithThreePoints("query")
        try {
            val results = shard.query(
                request = QueryRequest(
                    limit = 10uL,
                    offset = null,
                    query = ScoringQuery.Vector(query = Query.Nearest(vector = listOf(1.0f, 0.0f, 0.0f, 0.0f), using = null)),
                    prefetches = emptyList(),
                    withVector = null,
                    withPayload = null,
                    filter = null,
                    scoreThreshold = null,
                    params = null,
                ),
            )
            assertEquals("query should rank all 3 points", 3, results.size)
            val topId = results.first().id
            assertTrue("nearest to [1,0,0,0] should be id=1", topId is PointId.NumId && topId.value == 1uL)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 9: hnswOptimizeAndSearch ──────────────────────────────────────

    /** Loads with an HNSW config, upserts, optimize() (builds HNSW), searches. */
    @Test
    fun hnswOptimizeAndSearch() {
        val dir = freshDir("hnsw-optimize")
        val config = EdgeConfig(
            vectorData = mapOf(
                "" to VectorDataConfig(
                    size = 4uL,
                    distance = Distance.DOT,
                    quantizationConfig = null,
                    multivectorConfig = null,
                    datatype = null,
                    hnswConfig = HnswIndexConfig(
                        m = 16uL,
                        efConstruct = 100uL,
                        fullScanThreshold = 10000uL,
                        maxIndexingThreads = 1uL,
                        onDisk = false,
                        payloadM = null,
                    ),
                )
            ),
            sparseVectorData = emptyMap(),
        )
        val shard = EdgeShard.load(path = dir.absolutePath, config = config)
        try {
            shard.update(
                operation = UpdateOperation.upsertPoints(
                    points = listOf(
                        Point(PointId.NumId(1uL), Vector.Single(listOf(1.0f, 0.0f, 0.0f, 0.0f)), null),
                        Point(PointId.NumId(2uL), Vector.Single(listOf(0.0f, 1.0f, 0.0f, 0.0f)), null),
                        Point(PointId.NumId(3uL), Vector.Single(listOf(0.0f, 0.0f, 1.0f, 0.0f)), null),
                    ),
                ),
            )

            // Builds the HNSW index; don't assert the bool (tiny shard may be
            // already optimal), only that it runs and search still works.
            shard.optimize()

            val results = shard.search(
                request = SearchRequest(
                    query = Query.Nearest(vector = listOf(1.0f, 0.0f, 0.0f, 0.0f), using = null),
                    limit = 3uL,
                    offset = null,
                    filter = null,
                    params = null,
                    withVector = null,
                    withPayload = null,
                    scoreThreshold = null,
                ),
            )
            assertEquals("search after optimize should return all 3 points", 3, results.size)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 10: oversizedHnswParamRejected ────────────────────────────────

    /** An absurd HNSW `m` must be rejected as a catchable EdgeException at load,
     *  not abort the process via a multi-terabyte allocation at optimize(). */
    @Test
    fun oversizedHnswParamRejected() {
        val dir = freshDir("hnsw-oversized")
        val config = EdgeConfig(
            vectorData = mapOf(
                "" to VectorDataConfig(
                    size = 4uL,
                    distance = Distance.DOT,
                    quantizationConfig = null,
                    multivectorConfig = null,
                    datatype = null,
                    hnswConfig = HnswIndexConfig(
                        m = ULong.MAX_VALUE,
                        efConstruct = 100uL,
                        fullScanThreshold = 10000uL,
                        maxIndexingThreads = 1uL,
                        onDisk = false,
                        payloadM = null,
                    ),
                )
            ),
            sparseVectorData = emptyMap(),
        )
        var caught = false
        try {
            EdgeShard.load(path = dir.absolutePath, config = config).unload()
        } catch (e: EdgeException) {
            caught = true
        }
        assertTrue("oversized HNSW m should throw EdgeException, not abort", caught)
    }
}
