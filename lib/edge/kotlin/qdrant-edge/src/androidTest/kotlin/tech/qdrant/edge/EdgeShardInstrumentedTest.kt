package tech.qdrant.edge

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
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
 * The generated bindings live in `tech.qdrant.edge` (this test's own package),
 * so the domain types need no import.
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
                    vector = NamedVector.Dense(listOf(6.0f, 9.0f, 4.0f, 2.0f)),
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
                            memory = Memory.PINNED,
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
                    query = Query.Nearest(vector = NamedVector.Dense(listOf(6.0f, 9.0f, 4.0f, 2.0f)), using = null),
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

            val got = shard.retrieve(
                request = RetrieveRequest(
                    pointIds = listOf(PointId.NumId(2uL)),
                    withPayload = null,
                    withVector = null,
                ),
            )
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
                request = RetrieveRequest(
                    pointIds = listOf(PointId.NumId(1uL)),
                    withPayload = WithPayload.Bool(true),
                    withVector = null,
                ),
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
                    query = ScoringQuery.Vector(query = Query.Nearest(vector = NamedVector.Dense(listOf(1.0f, 0.0f, 0.0f, 0.0f)), using = null)),
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
                        memory = Memory.PINNED,
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
                    query = Query.Nearest(vector = NamedVector.Dense(listOf(1.0f, 0.0f, 0.0f, 0.0f)), using = null),
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

            // The HNSW config round-trips honestly through config().
            val hnsw = shard.config().vectorData[""]?.hnswConfig
            assertTrue("HNSW config should round-trip through config()", hnsw != null)
            assertEquals("round-tripped HNSW m should match", 16uL, hnsw!!.m)
            assertEquals("round-tripped HNSW efConstruct should match", 100uL, hnsw.efConstruct)
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
                        memory = Memory.PINNED,
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

    // ── Helper: load a shard with 3 ranked points (rank 30/10/20, label a/b/a) ──

    private fun loadWithRankedPoints(name: String): EdgeShard {
        val dir = freshDir(name)
        val shard = EdgeShard.load(path = dir.absolutePath, config = defaultConfig())
        shard.update(
            operation = UpdateOperation.upsertPoints(
                points = listOf(
                    Point(PointId.NumId(1uL), Vector.Single(listOf(1.0f, 0.0f, 0.0f, 0.0f)), """{"rank": 30, "label": "a"}"""),
                    Point(PointId.NumId(2uL), Vector.Single(listOf(0.0f, 1.0f, 0.0f, 0.0f)), """{"rank": 10, "label": "b"}"""),
                    Point(PointId.NumId(3uL), Vector.Single(listOf(0.0f, 0.0f, 1.0f, 0.0f)), """{"rank": 20, "label": "a"}"""),
                ),
            ),
        )
        return shard
    }

    /** A keyword `label` field-condition filter, reused by the *ByFilter tests. */
    private fun labelFilter(value: String): Filter = Filter(
        must = listOf(Condition.Field(condition = FieldCondition(key = "label", match = Match.Value(ValueVariants.String(value))))),
        should = null,
        mustNot = null,
    )

    /**
     * Decodes a returned vector JSON string (the binding surfaces vectors as a
     * JSON string, not List<Float>) into its float components. Handles both the
     * bare-array form and the named `{"field": [...]}` object form.
     */
    private fun extractVectorFloats(json: String): List<Float> {
        val trimmed = json.trim()
        val arr: org.json.JSONArray = if (trimmed.startsWith("[")) {
            org.json.JSONArray(trimmed)
        } else {
            val obj = org.json.JSONObject(trimmed)
            val firstKey = obj.keys().next()
            obj.getJSONArray(firstKey)
        }
        return (0 until arr.length()).map { arr.getDouble(it).toFloat() }
    }

    /** Extracts the ULong id of a numeric-id point, or null for a uuid id. */
    private fun numId(id: PointId): ULong? = (id as? PointId.NumId)?.`value`

    // ── Test 11: createFieldIndexWithParamsAndIntrospect ───────────────────

    @Test
    fun createFieldIndexWithParamsAndIntrospect() {
        val shard = loadWithRankedPoints("payload-index")
        try {
            shard.update(
                operation = UpdateOperation.createFieldIndexWithParams(
                    fieldName = "rank",
                    params = PayloadIndexParams.Integer(config = IntegerIndexParams(lookup = true, range = true)),
                ),
            )

            val schema = shard.info().payloadSchema
            val rankIndex = schema["rank"]
            assertNotNull("created payload index should be reported by info()", rankIndex)
            assertEquals("rank index should report Integer data type", PayloadSchemaType.INTEGER, rankIndex!!.dataType)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 12: orderByScrollPopulatesOrderValue ──────────────────────────

    @Test
    fun orderByScrollPopulatesOrderValue() {
        val shard = loadWithRankedPoints("order-value")
        try {
            shard.update(operation = UpdateOperation.createFieldIndex(fieldName = "rank", schema = PayloadSchemaType.INTEGER))

            val page = shard.scroll(
                request = ScrollRequest(
                    offset = null,
                    limit = 10uL,
                    filter = null,
                    withPayload = WithPayload.Bool(false),
                    withVector = WithVector.Bool(false),
                    orderBy = OrderBy(key = "rank", direction = Direction.ASC, startFrom = null),
                ),
            )
            assertEquals("scroll should return all 3 points", 3, page.records.size)
            val first = page.records.first().orderValue
            assertTrue("order-by scroll should populate an integer order_value", first is OrderValue.Int)
            assertEquals("smallest rank should sort first", 10L, (first as OrderValue.Int).`value`)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 13: recommendReturnsResults ───────────────────────────────────

    @Test
    fun recommendReturnsResults() {
        val shard = loadWithThreePoints("recommend")
        try {
            val results = shard.query(
                request = QueryRequest(
                    limit = 10uL,
                    offset = null,
                    query = ScoringQuery.Vector(
                        query = Query.Recommend(
                            positives = listOf(NamedVector.Dense(listOf(1.0f, 0.0f, 0.0f, 0.0f))),
                            negatives = emptyList(),
                            strategy = null,
                            using = null,
                        ),
                    ),
                    prefetches = emptyList(),
                    withVector = null,
                    withPayload = null,
                    filter = null,
                    scoreThreshold = null,
                    params = null,
                ),
            )
            assertTrue("recommend should return results", results.isNotEmpty())
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 14: queryGroupsReturnsGroups ──────────────────────────────────

    @Test
    fun queryGroupsReturnsGroups() {
        val shard = loadWithRankedPoints("grouping")
        try {
            val groups = shard.queryGroups(
                request = GroupRequest(
                    query = QueryRequest(
                        limit = 10uL,
                        offset = null,
                        query = ScoringQuery.Vector(query = Query.Nearest(vector = NamedVector.Dense(listOf(1.0f, 0.0f, 0.0f, 0.0f)), using = null)),
                        prefetches = emptyList(),
                        withVector = null,
                        withPayload = null,
                        filter = null,
                        scoreThreshold = null,
                        params = null,
                    ),
                    groupBy = "label",
                    groups = 10uL,
                    groupSize = 10uL,
                ),
            )
            assertEquals("grouping by label should yield one group per distinct value", 2, groups.size)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 15: formulaRescoringQuery ─────────────────────────────────────

    @Test
    fun formulaRescoringQuery() {
        val shard = loadWithThreePoints("formula")
        try {
            val expression = Expression.variable("\$score")

            val results = shard.query(
                request = QueryRequest(
                    limit = 10uL,
                    offset = null,
                    query = ScoringQuery.Formula(expression = expression, defaults = emptyMap()),
                    prefetches = listOf(
                        Prefetch(
                            limit = 10uL,
                            query = ScoringQuery.Vector(query = Query.Nearest(vector = NamedVector.Dense(listOf(1.0f, 0.0f, 0.0f, 0.0f)), using = null)),
                            prefetches = emptyList(),
                            filter = null,
                            scoreThreshold = null,
                            params = null,
                        ),
                    ),
                    withVector = null,
                    withPayload = null,
                    filter = null,
                    scoreThreshold = null,
                    params = null,
                ),
            )
            assertEquals("formula rescoring should return all prefetched points", 3, results.size)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 16: filteredSearchNarrowsResults ──────────────────────────────

    @Test
    fun filteredSearchNarrowsResults() {
        val shard = loadWithThreePoints("filtered-search")
        try {
            val unfiltered = shard.search(
                request = SearchRequest(
                    query = Query.Nearest(vector = NamedVector.Dense(listOf(1.0f, 1.0f, 1.0f, 1.0f)), using = null),
                    limit = 10uL, offset = null, filter = null, params = null,
                    withVector = null, withPayload = null, scoreThreshold = null,
                ),
            )
            assertEquals("unfiltered search should see all 3 points", 3, unfiltered.size)

            val labelB = Filter(
                must = listOf(Condition.Field(condition = FieldCondition(key = "label", match = Match.Value(ValueVariants.String("b"))))),
                should = null,
                mustNot = null,
            )
            val filtered = shard.search(
                request = SearchRequest(
                    query = Query.Nearest(vector = NamedVector.Dense(listOf(1.0f, 1.0f, 1.0f, 1.0f)), using = null),
                    limit = 10uL, offset = null, filter = labelB, params = null,
                    withVector = null, withPayload = null, scoreThreshold = null,
                ),
            )
            assertEquals("filter label=b should match exactly one point", 1, filtered.size)
            assertEquals("point 2 is the one carrying label=b", 2uL, numId(filtered.first().id))
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 17: setPayloadByFilterAffectsMatchingOnly ─────────────────────

    @Test
    fun setPayloadByFilterAffectsMatchingOnly() {
        val shard = loadWithThreePoints("set-payload-by-filter")
        try {
            shard.update(
                operation = UpdateOperation.setPayloadByFilter(
                    filter = labelFilter("a"),
                    payloadJson = """{"tag": "hot"}""",
                ),
            )

            val matched = shard.retrieve(request = RetrieveRequest(pointIds = listOf(PointId.NumId(1uL)), withPayload = WithPayload.Bool(true), withVector = null))
            val matchedPayload = matched.first().payload ?: ""
            assertTrue("matching point should gain 'tag': $matchedPayload", matchedPayload.contains("\"tag\""))

            val unmatched = shard.retrieve(request = RetrieveRequest(pointIds = listOf(PointId.NumId(2uL)), withPayload = WithPayload.Bool(true), withVector = null))
            val unmatchedPayload = unmatched.first().payload ?: ""
            assertFalse("non-matching point should be untouched: $unmatchedPayload", unmatchedPayload.contains("\"tag\""))
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 18: countAfterUnloadThrowsShardClosed ─────────────────────────

    @Test
    fun countAfterUnloadThrowsShardClosed() {
        val shard = loadWithThreePoints("shard-closed")
        shard.unload()

        var caught = false
        try {
            shard.count(request = CountRequest(filter = null, exact = true))
        } catch (e: EdgeException.ShardClosed) {
            caught = true
        }
        assertTrue("count after unload should throw EdgeException.ShardClosed", caught)
    }

    // ── Test 19: dimensionMismatchThrowsOperationError ─────────────────────

    @Test
    fun dimensionMismatchThrowsOperationError() {
        val shard = loadWithThreePoints("dim-mismatch")
        try {
            var caught = false
            try {
                shard.update(
                    operation = UpdateOperation.upsertPoints(
                        points = listOf(Point(PointId.NumId(99uL), Vector.Single(listOf(1.0f, 2.0f)), null)),
                    ),
                )
            } catch (e: EdgeException.OperationException) {
                caught = true
            }
            assertTrue("dimension mismatch should throw EdgeException.OperationException", caught)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 20: uuidPointRoundTrips ───────────────────────────────────────

    @Test
    fun uuidPointRoundTrips() {
        val dir = freshDir("uuid-roundtrip")
        val shard = EdgeShard.load(path = dir.absolutePath, config = defaultConfig())
        try {
            val uuid = "e9408f2b-b917-4af1-ab75-d97ac6b2c047"
            shard.update(
                operation = UpdateOperation.upsertPoints(
                    points = listOf(Point(PointId.Uuid(uuid), Vector.Single(listOf(1.0f, 0.0f, 0.0f, 0.0f)), """{"label": "u"}""")),
                ),
            )

            assertEquals("the single uuid point should be counted", 1uL, shard.count(request = CountRequest(filter = null, exact = true)))

            val got = shard.retrieve(request = RetrieveRequest(pointIds = listOf(PointId.Uuid(uuid)), withPayload = WithPayload.Bool(true), withVector = null))
            assertEquals("uuid point should be retrievable by the same uuid", 1, got.size)
            val id = got.first().id
            assertTrue("retrieved id should be a uuid", id is PointId.Uuid)
            assertEquals("retrieved id should equal the upserted uuid", uuid, (id as PointId.Uuid).`value`)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 21: searchAndRetrieveDecodeUpsertedVector ─────────────────────

    @Test
    fun searchAndRetrieveDecodeUpsertedVector() {
        val dir = freshDir("vector-roundtrip")
        val shard = EdgeShard.load(path = dir.absolutePath, config = defaultConfig())
        try {
            val upserted = listOf(1.0f, 2.0f, 3.0f, 4.0f)
            shard.update(
                operation = UpdateOperation.upsertPoints(
                    points = listOf(Point(PointId.NumId(1uL), Vector.Single(upserted), null)),
                ),
            )

            val results = shard.search(
                request = SearchRequest(
                    query = Query.Nearest(vector = NamedVector.Dense(upserted), using = null),
                    limit = 1uL, offset = null, filter = null, params = null,
                    withVector = WithVector.Bool(true), withPayload = null, scoreThreshold = null,
                ),
            )
            assertEquals(1, results.size)
            val searchVectorJson = results.first().vector
            assertNotNull("withVector=true should populate ScoredPoint.vector", searchVectorJson)
            val searchDecoded = extractVectorFloats(searchVectorJson!!)
            assertEquals("decoded vector should have 4 components: $searchVectorJson", 4, searchDecoded.size)
            for (i in upserted.indices) {
                assertEquals("search component $i should round-trip", upserted[i], searchDecoded[i], 1e-5f)
            }

            val records = shard.retrieve(request = RetrieveRequest(pointIds = listOf(PointId.NumId(1uL)), withPayload = null, withVector = WithVector.Bool(true)))
            val recordVectorJson = records.first().vector
            assertNotNull("withVector=true should populate Record.vector", recordVectorJson)
            val recordDecoded = extractVectorFloats(recordVectorJson!!)
            assertEquals("decoded record vector should have 4 components: $recordVectorJson", 4, recordDecoded.size)
            for (i in upserted.indices) {
                assertEquals("retrieve component $i should round-trip", upserted[i], recordDecoded[i], 1e-5f)
            }
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 22: overwritePayloadReplacesWholePayload ──────────────────────

    @Test
    fun overwritePayloadReplacesWholePayload() {
        val shard = loadWithThreePoints("overwrite-payload")
        try {
            shard.update(
                operation = UpdateOperation.overwritePayload(
                    pointIds = listOf(PointId.NumId(1uL)),
                    payloadJson = """{"tag": "hot"}""",
                ),
            )

            val got = shard.retrieve(request = RetrieveRequest(pointIds = listOf(PointId.NumId(1uL)), withPayload = WithPayload.Bool(true), withVector = null))
            val payload = got.first().payload ?: ""
            assertTrue("new key should be present: $payload", payload.contains("\"tag\""))
            assertFalse("original key should be gone after overwrite: $payload", payload.contains("\"label\""))
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 23: deletePayloadRemovesKey ───────────────────────────────────

    @Test
    fun deletePayloadRemovesKey() {
        val shard = loadWithThreePoints("delete-payload")
        try {
            shard.update(operation = UpdateOperation.setPayload(pointIds = listOf(PointId.NumId(1uL)), payloadJson = """{"tag": "hot"}"""))
            shard.update(operation = UpdateOperation.deletePayload(pointIds = listOf(PointId.NumId(1uL)), keys = listOf("label")))

            val got = shard.retrieve(request = RetrieveRequest(pointIds = listOf(PointId.NumId(1uL)), withPayload = WithPayload.Bool(true), withVector = null))
            val payload = got.first().payload ?: ""
            assertFalse("deleted key 'label' should be gone: $payload", payload.contains("\"label\""))
            assertTrue("untargeted key 'tag' should survive: $payload", payload.contains("\"tag\""))
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 24: clearPayloadEmptiesPayload ────────────────────────────────

    @Test
    fun clearPayloadEmptiesPayload() {
        val shard = loadWithThreePoints("clear-payload")
        try {
            shard.update(operation = UpdateOperation.clearPayload(pointIds = listOf(PointId.NumId(1uL))))

            val got = shard.retrieve(request = RetrieveRequest(pointIds = listOf(PointId.NumId(1uL)), withPayload = WithPayload.Bool(true), withVector = null))
            assertEquals("clearPayload must not delete the point", 1, got.size)
            val payload = got.first().payload ?: "{}"
            assertFalse("cleared payload should not contain the original key: $payload", payload.contains("\"label\""))
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 25: createDenseVectorAndUpdateVectors ─────────────────────────

    @Test
    fun createDenseVectorAndUpdateVectors() {
        val shard = loadWithThreePoints("named-vector")
        try {
            shard.update(operation = UpdateOperation.createDenseVector(vectorName = "extra", size = 2uL, distance = Distance.DOT))
            shard.update(
                operation = UpdateOperation.updateVectors(
                    pointVectors = listOf(PointVectors(id = PointId.NumId(1uL), vector = Vector.Named(mapOf("extra" to NamedVector.Dense(listOf(5.0f, 6.0f)))))),
                ),
            )

            val results = shard.search(
                request = SearchRequest(
                    query = Query.Nearest(vector = NamedVector.Dense(listOf(5.0f, 6.0f)), using = "extra"),
                    limit = 10uL, offset = null, filter = null, params = null,
                    withVector = null, withPayload = null, scoreThreshold = null,
                ),
            )
            assertEquals("only point 1 has the 'extra' vector", 1, results.size)
            assertEquals("the point carrying the 'extra' vector is id=1", 1uL, numId(results.first().id))
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 26: concurrentSearchesAreConsistent ───────────────────────────

    @Test
    fun concurrentSearchesAreConsistent() {
        val shard = loadWithThreePoints("concurrency")
        try {
            val n = 8
            val pool = java.util.concurrent.Executors.newFixedThreadPool(n)
            val latch = java.util.concurrent.CountDownLatch(n)
            val counts = java.util.Collections.synchronizedList(mutableListOf<Int>())
            val failures = java.util.Collections.synchronizedList(mutableListOf<Throwable>())
            repeat(n) {
                pool.submit {
                    try {
                        val results = shard.search(
                            request = SearchRequest(
                                query = Query.Nearest(vector = NamedVector.Dense(listOf(1.0f, 0.0f, 0.0f, 0.0f)), using = null),
                                limit = 10uL, offset = null, filter = null, params = null,
                                withVector = null, withPayload = null, scoreThreshold = null,
                            ),
                        )
                        counts.add(results.size)
                    } catch (e: Throwable) {
                        failures.add(e)
                    } finally {
                        latch.countDown()
                    }
                }
            }
            latch.await()
            pool.shutdown()

            assertTrue("no concurrent search should throw: $failures", failures.isEmpty())
            assertEquals("all 8 concurrent searches should complete", 8, counts.size)
            assertTrue("every concurrent search should return all 3 points, got $counts", counts.all { it == 3 })
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 27: sparseVectorSearchRanksByOverlap ──────────────────────────

    @Test
    fun sparseVectorSearchRanksByOverlap() {
        val dir = freshDir("sparse")
        val config = EdgeConfig(vectorData = emptyMap(), sparseVectorData = mapOf("sp" to SparseVectorDataConfig()))
        val shard = EdgeShard.load(path = dir.absolutePath, config = config)
        try {
            shard.update(
                operation = UpdateOperation.upsertPoints(
                    points = listOf(
                        Point(PointId.NumId(1uL), Vector.Named(mapOf("sp" to NamedVector.Sparse(SparseVector(indices = listOf(1u, 5u, 9u), values = listOf(0.5f, 1.5f, 2.5f))))), null),
                        Point(PointId.NumId(2uL), Vector.Named(mapOf("sp" to NamedVector.Sparse(SparseVector(indices = listOf(0u, 1u), values = listOf(1.0f, 1.0f))))), null),
                    ),
                ),
            )

            val results = shard.search(
                request = SearchRequest(
                    query = Query.Nearest(vector = NamedVector.Sparse(SparseVector(indices = listOf(9u), values = listOf(1.0f))), using = "sp"),
                    limit = 10uL, offset = null, filter = null, params = null,
                    withVector = null, withPayload = null, scoreThreshold = null,
                ),
            )
            assertEquals("a sparse query touching only index 9 must match exactly point 1", 1, results.size)
            assertEquals("the only point carrying index 9 is id=1", 1uL, numId(results.first().id))
            assertEquals("sparse score should be the overlapping-term dot product", 2.5f, results.first().score, 1e-4f)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 28: multiVectorMaxSimSearch ───────────────────────────────────

    @Test
    fun multiVectorMaxSimSearch() {
        val dir = freshDir("multivector")
        val config = EdgeConfig(
            vectorData = mapOf(
                "mv" to VectorDataConfig(
                    size = 2uL,
                    distance = Distance.DOT,
                    quantizationConfig = null,
                    multivectorConfig = MultiVectorConfig(comparator = MultiVectorComparator.MAX_SIM),
                    datatype = null,
                    hnswConfig = null,
                ),
            ),
            sparseVectorData = emptyMap(),
        )
        val shard = EdgeShard.load(path = dir.absolutePath, config = config)
        try {
            shard.update(
                operation = UpdateOperation.upsertPoints(
                    points = listOf(
                        Point(PointId.NumId(1uL), Vector.Named(mapOf("mv" to NamedVector.MultiDense(listOf(listOf(1.0f, 2.0f), listOf(3.0f, 4.0f))))), null),
                        Point(PointId.NumId(2uL), Vector.Named(mapOf("mv" to NamedVector.MultiDense(listOf(listOf(0.0f, 1.0f))))), null),
                    ),
                ),
            )

            val results = shard.search(
                request = SearchRequest(
                    query = Query.Nearest(vector = NamedVector.MultiDense(listOf(listOf(3.0f, 4.0f))), using = "mv"),
                    limit = 10uL, offset = null, filter = null, params = null,
                    withVector = null, withPayload = null, scoreThreshold = null,
                ),
            )
            assertEquals("both multi-vector points should be scored", 2, results.size)
            assertEquals("MaxSim: point 1 (best row dot 25) beats point 2 (4)", 1uL, numId(results.first().id))
            assertEquals("MaxSim score should be the best per-row dot product", 25.0f, results.first().score, 1e-3f)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 29: discoverQueryReturnsResults ───────────────────────────────

    @Test
    fun discoverQueryReturnsResults() {
        val shard = loadWithThreePoints("discover")
        try {
            val results = shard.query(
                request = QueryRequest(
                    limit = 10uL, offset = null,
                    query = ScoringQuery.Vector(
                        query = Query.Discover(
                            target = NamedVector.Dense(listOf(1.0f, 0.0f, 0.0f, 0.0f)),
                            context = listOf(
                                ContextPair(
                                    positive = NamedVector.Dense(listOf(1.0f, 0.0f, 0.0f, 0.0f)),
                                    negative = NamedVector.Dense(listOf(0.0f, 1.0f, 0.0f, 0.0f)),
                                ),
                            ),
                            using = null,
                        ),
                    ),
                    prefetches = emptyList(), withVector = null, withPayload = null, filter = null, scoreThreshold = null, params = null,
                ),
            )
            assertTrue("discover should return results", results.isNotEmpty())
            assertTrue("discover cannot return more than the 3 upserted points", results.size <= 3)
            for (r in results) {
                val v = numId(r.id)
                assertTrue("discover returned an unexpected id: ${r.id}", v != null && v in 1uL..3uL)
            }
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 30: contextQueryReturnsResults ────────────────────────────────

    @Test
    fun contextQueryReturnsResults() {
        val shard = loadWithThreePoints("context")
        try {
            val results = shard.query(
                request = QueryRequest(
                    limit = 10uL, offset = null,
                    query = ScoringQuery.Vector(
                        query = Query.Context(
                            context = listOf(
                                ContextPair(
                                    positive = NamedVector.Dense(listOf(1.0f, 0.0f, 0.0f, 0.0f)),
                                    negative = NamedVector.Dense(listOf(0.0f, 1.0f, 0.0f, 0.0f)),
                                ),
                            ),
                            using = null,
                        ),
                    ),
                    prefetches = emptyList(), withVector = null, withPayload = null, filter = null, scoreThreshold = null, params = null,
                ),
            )
            assertTrue("context should return results", results.isNotEmpty())
            assertTrue("context cannot return more than the 3 upserted points", results.size <= 3)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 31: feedbackQueryReturnsResults ───────────────────────────────

    @Test
    fun feedbackQueryReturnsResults() {
        val shard = loadWithThreePoints("feedback")
        try {
            val results = shard.query(
                request = QueryRequest(
                    limit = 10uL, offset = null,
                    query = ScoringQuery.Vector(
                        query = Query.Feedback(
                            target = NamedVector.Dense(listOf(1.0f, 0.0f, 0.0f, 0.0f)),
                            feedback = listOf(FeedbackItem(vector = NamedVector.Dense(listOf(0.0f, 1.0f, 0.0f, 0.0f)), score = 1.0f)),
                            coefficients = FeedbackCoefficients(a = 1.0f, b = 1.0f, c = 1.0f),
                            using = null,
                        ),
                    ),
                    prefetches = emptyList(), withVector = null, withPayload = null, filter = null, scoreThreshold = null, params = null,
                ),
            )
            assertTrue("feedback query should return results", results.isNotEmpty())
            assertTrue("feedback cannot return more than the 3 upserted points", results.size <= 3)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 32: rrfFusionOverPrefetches ───────────────────────────────────

    @Test
    fun rrfFusionOverPrefetches() {
        val shard = loadWithThreePoints("fusion")
        try {
            fun branch(vector: List<Float>) = Prefetch(
                limit = 3uL,
                query = ScoringQuery.Vector(query = Query.Nearest(vector = NamedVector.Dense(vector), using = null)),
                prefetches = emptyList(), filter = null, scoreThreshold = null, params = null,
            )

            val results = shard.query(
                request = QueryRequest(
                    limit = 3uL, offset = null,
                    query = ScoringQuery.Fusion(fusion = Fusion.Rrf(k = 60uL, weights = null)),
                    prefetches = listOf(branch(listOf(1.0f, 0.0f, 0.0f, 0.0f)), branch(listOf(0.0f, 0.0f, 1.0f, 0.0f))),
                    withVector = null, withPayload = null, filter = null, scoreThreshold = null, params = null,
                ),
            )
            assertEquals("RRF over two prefetches should fuse to all three points", 3, results.size)
            val ids = results.mapNotNull { numId(it.id) }.sorted()
            assertEquals("fused result set should contain each point exactly once", listOf(1uL, 2uL, 3uL), ids)
            for (i in 1 until results.size) {
                assertTrue("fused scores must be in descending order", results[i - 1].score >= results[i].score)
            }
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 33: mmrQueryReturnsResults ────────────────────────────────────

    @Test
    fun mmrQueryReturnsResults() {
        val shard = loadWithThreePoints("mmr")
        try {
            val results = shard.query(
                request = QueryRequest(
                    limit = 3uL, offset = null,
                    query = ScoringQuery.Mmr(vector = NamedVector.Dense(listOf(1.0f, 0.0f, 0.0f, 0.0f)), using = null, lambda = 0.5f, candidatesLimit = 10uL),
                    prefetches = emptyList(), withVector = null, withPayload = null, filter = null, scoreThreshold = null, params = null,
                ),
            )
            assertTrue("mmr query should return results", results.isNotEmpty())
            assertTrue("mmr cannot return more than the 3 upserted points", results.size <= 3)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 34: mmrInvalidLambdaThrowsInvalidArgument ─────────────────────

    @Test
    fun mmrInvalidLambdaThrowsInvalidArgument() {
        val shard = loadWithThreePoints("mmr-bad")
        try {
            for (badLambda in listOf(Float.NaN, 2.0f, -1.0f)) {
                var caught = false
                try {
                    shard.query(
                        request = QueryRequest(
                            limit = 3uL, offset = null,
                            query = ScoringQuery.Mmr(vector = NamedVector.Dense(listOf(1.0f, 0.0f, 0.0f, 0.0f)), using = null, lambda = badLambda, candidatesLimit = 10uL),
                            prefetches = emptyList(), withVector = null, withPayload = null, filter = null, scoreThreshold = null, params = null,
                        ),
                    )
                } catch (e: EdgeException.InvalidArgument) {
                    caught = true
                }
                assertTrue("MMR lambda=$badLambda should throw InvalidArgument", caught)
            }
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 35: sampleQueryReturnsLimited ─────────────────────────────────

    @Test
    fun sampleQueryReturnsLimited() {
        val shard = loadWithThreePoints("sample")
        try {
            val results = shard.query(
                request = QueryRequest(
                    limit = 2uL, offset = null,
                    query = ScoringQuery.Sample(sample = Sample.RANDOM),
                    prefetches = emptyList(), withVector = null, withPayload = null, filter = null, scoreThreshold = null, params = null,
                ),
            )
            assertEquals("random sample with limit 2 over 3 points returns 2", 2, results.size)
            for (r in results) {
                val v = numId(r.id)
                assertTrue("sample returned an unexpected id: ${r.id}", v != null && v in 1uL..3uL)
            }
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 36: facetCountsMatchDistribution ──────────────────────────────

    @Test
    fun facetCountsMatchDistribution() {
        val shard = loadWithRankedPoints("facet")
        try {
            shard.update(operation = UpdateOperation.createFieldIndex(fieldName = "label", schema = PayloadSchemaType.KEYWORD))

            val response = shard.facet(request = FacetRequest(key = "label", limit = 10uL, exact = true, filter = null))
            val counts = mutableMapOf<String, ULong>()
            for (hit in response.hits) counts[hit.`value`] = hit.count
            assertEquals("two distinct labels -> two facet hits", 2, response.hits.size)
            assertEquals("label 'a' appears on points 1 and 3", 2uL, counts["a"])
            assertEquals("label 'b' appears on point 2", 1uL, counts["b"])
            val total = response.hits.fold(0uL) { acc, h -> acc + h.count }
            assertEquals("facet counts must sum to all 3 points", 3uL, total)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 37: createSucceedsOnFreshDirAndFailsOnOccupied ────────────────

    @Test
    fun createSucceedsOnFreshDirAndFailsOnOccupied() {
        val dir = freshDir("create-fresh")

        val shard = EdgeShard.create(path = dir.absolutePath, config = defaultConfig())
        try {
            shard.update(
                operation = UpdateOperation.upsertPoints(
                    points = listOf(Point(PointId.NumId(1uL), Vector.Single(listOf(1.0f, 0.0f, 0.0f, 0.0f)), null)),
                ),
            )
            assertEquals("create() then upsert should count 1", 1uL, shard.count(request = CountRequest(filter = null, exact = true)))

            var caught = false
            try {
                EdgeShard.create(path = dir.absolutePath, config = defaultConfig())
            } catch (e: EdgeException.OperationException) {
                caught = true
            }
            assertTrue("create() on an occupied dir should throw OperationException", caught)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 38: configSettersPersist ──────────────────────────────────────

    @Test
    fun configSettersPersist() {
        val shard = loadWithThreePoints("config-setters")
        try {
            shard.setHnswConfig(hnswConfig = HnswIndexConfig(m = 16uL, efConstruct = 100uL, fullScanThreshold = 10000uL, maxIndexingThreads = 1uL, memory = Memory.CACHED, payloadM = null))
            shard.setVectorHnswConfig(vectorName = "", hnswConfig = HnswIndexConfig(m = 8uL, efConstruct = 64uL, fullScanThreshold = 10000uL, maxIndexingThreads = 1uL, memory = null, payloadM = null))
            shard.setOptimizersConfig(
                optimizers = OptimizersConfig(
                    deletedThreshold = 0.5, vacuumMinVectorNumber = 100uL, defaultSegmentNumber = 1uL,
                    maxSegmentSizeKb = null, indexingThresholdKb = 1000uL, preventUnoptimized = null,
                ),
            )

            val hnsw = shard.config().vectorData[""]?.hnswConfig
            assertNotNull("vector HNSW config should be present after setVectorHnswConfig", hnsw)
            assertEquals("setVectorHnswConfig should persist m=8", 8uL, hnsw!!.m)
            assertEquals("setVectorHnswConfig should persist efConstruct=64", 64uL, hnsw.efConstruct)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 39: snapshotManifestAndBadRestore ─────────────────────────────

    @Test
    fun snapshotManifestAndBadRestore() {
        val shard = loadWithThreePoints("snapshot")
        try {
            val manifest = shard.snapshotManifest()
            assertTrue("snapshotManifest should return a non-empty string", manifest.isNotEmpty())
            var validJson = true
            try {
                org.json.JSONTokener(manifest).nextValue()
            } catch (e: Exception) {
                validJson = false
            }
            assertTrue("snapshot manifest must be valid JSON: $manifest", validJson)

            var caught = false
            try {
                shard.updateFromSnapshot(snapshotPath = "/definitely/missing/nope.snapshot", tmpDir = null)
            } catch (e: EdgeException.OperationException) {
                caught = true
            }
            assertTrue("a missing snapshot path should throw OperationException", caught)
            assertEquals("shard must survive a failed restore with its data intact", 3uL, shard.info().pointsCount)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 40: pathReturnsLoadedDir ──────────────────────────────────────

    @Test
    fun pathReturnsLoadedDir() {
        val dir = freshDir("path-check")
        val shard = EdgeShard.load(path = dir.absolutePath, config = defaultConfig())
        try {
            assertEquals("path() should echo the dir the shard was loaded from", dir.absolutePath, shard.path())
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 41: deletePointsByFilterRemovesMatchingOnly ───────────────────

    @Test
    fun deletePointsByFilterRemovesMatchingOnly() {
        val shard = loadWithRankedPoints("del-points-by-filter")
        try {
            shard.update(operation = UpdateOperation.deletePointsByFilter(filter = labelFilter("a")))
            assertEquals("deleting label=a (2 points) leaves 1", 1uL, shard.count(request = CountRequest(filter = null, exact = true)))

            val remaining = shard.scroll(
                request = ScrollRequest(
                    offset = null, limit = 10uL, filter = null,
                    withPayload = WithPayload.Bool(false), withVector = WithVector.Bool(false), orderBy = null,
                ),
            )
            assertEquals(1, remaining.records.size)
            assertEquals("only the label=b point (id 2) should survive", 2uL, numId(remaining.records.first().id))
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 42: deletePayloadByFilterRemovesKeyOnMatchingOnly ─────────────

    @Test
    fun deletePayloadByFilterRemovesKeyOnMatchingOnly() {
        val shard = loadWithRankedPoints("del-payload-by-filter")
        try {
            shard.update(operation = UpdateOperation.deletePayloadByFilter(filter = labelFilter("a"), keys = listOf("rank")))

            val p1 = shard.retrieve(request = RetrieveRequest(pointIds = listOf(PointId.NumId(1uL)), withPayload = WithPayload.Bool(true), withVector = null))
            val p1Payload = p1.first().payload ?: ""
            assertFalse("matching point 1 should lose 'rank': $p1Payload", p1Payload.contains("\"rank\""))
            assertTrue("matching point 1 keeps 'label': $p1Payload", p1Payload.contains("\"label\""))

            val p2 = shard.retrieve(request = RetrieveRequest(pointIds = listOf(PointId.NumId(2uL)), withPayload = WithPayload.Bool(true), withVector = null))
            assertTrue("non-matching point 2 keeps 'rank': ${p2.first().payload}", (p2.first().payload ?: "").contains("\"rank\""))
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 43: clearPayloadByFilterEmptiesMatchingOnly ───────────────────

    @Test
    fun clearPayloadByFilterEmptiesMatchingOnly() {
        val shard = loadWithRankedPoints("clear-payload-by-filter")
        try {
            shard.update(operation = UpdateOperation.clearPayloadByFilter(filter = labelFilter("a")))

            val p1 = shard.retrieve(request = RetrieveRequest(pointIds = listOf(PointId.NumId(1uL)), withPayload = WithPayload.Bool(true), withVector = null))
            val p1Payload = p1.first().payload ?: "{}"
            assertFalse("matching point 1 payload should be cleared: $p1Payload", p1Payload.contains("\"label\""))

            val p2 = shard.retrieve(request = RetrieveRequest(pointIds = listOf(PointId.NumId(2uL)), withPayload = WithPayload.Bool(true), withVector = null))
            assertTrue("non-matching point 2 keeps its payload: ${p2.first().payload}", (p2.first().payload ?: "").contains("\"label\""))
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 44: overwritePayloadByFilterReplacesMatchingOnly ──────────────

    @Test
    fun overwritePayloadByFilterReplacesMatchingOnly() {
        val shard = loadWithRankedPoints("overwrite-payload-by-filter")
        try {
            shard.update(operation = UpdateOperation.overwritePayloadByFilter(filter = labelFilter("b"), payloadJson = """{"tag": "hot"}"""))

            val p2 = shard.retrieve(request = RetrieveRequest(pointIds = listOf(PointId.NumId(2uL)), withPayload = WithPayload.Bool(true), withVector = null))
            val p2Payload = p2.first().payload ?: ""
            assertTrue("overwrite should set 'tag': $p2Payload", p2Payload.contains("\"tag\""))
            assertFalse("overwrite should drop the old 'rank' key: $p2Payload", p2Payload.contains("\"rank\""))

            val p1 = shard.retrieve(request = RetrieveRequest(pointIds = listOf(PointId.NumId(1uL)), withPayload = WithPayload.Bool(true), withVector = null))
            assertFalse("non-matching point 1 should be untouched: ${p1.first().payload}", (p1.first().payload ?: "").contains("\"tag\""))
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 45: deleteVectorsByFilterRemovesNamedVectorOnMatchingOnly ─────

    @Test
    fun deleteVectorsByFilterRemovesNamedVectorOnMatchingOnly() {
        val shard = loadWithRankedPoints("del-vectors-by-filter")
        try {
            shard.update(operation = UpdateOperation.createDenseVector(vectorName = "extra", size = 2uL, distance = Distance.DOT))
            shard.update(
                operation = UpdateOperation.updateVectors(
                    pointVectors = listOf(
                        PointVectors(id = PointId.NumId(1uL), vector = Vector.Named(mapOf("extra" to NamedVector.Dense(listOf(5.0f, 6.0f))))),
                        PointVectors(id = PointId.NumId(2uL), vector = Vector.Named(mapOf("extra" to NamedVector.Dense(listOf(5.0f, 6.0f))))),
                    ),
                ),
            )

            val before = shard.search(
                request = SearchRequest(
                    query = Query.Nearest(vector = NamedVector.Dense(listOf(5.0f, 6.0f)), using = "extra"),
                    limit = 10uL, offset = null, filter = null, params = null, withVector = null, withPayload = null, scoreThreshold = null,
                ),
            )
            assertEquals("points 1 and 2 both carry the 'extra' vector", 2, before.size)

            shard.update(operation = UpdateOperation.deleteVectorsByFilter(filter = labelFilter("a"), vectorNames = listOf("extra")))

            val after = shard.search(
                request = SearchRequest(
                    query = Query.Nearest(vector = NamedVector.Dense(listOf(5.0f, 6.0f)), using = "extra"),
                    limit = 10uL, offset = null, filter = null, params = null, withVector = null, withPayload = null, scoreThreshold = null,
                ),
            )
            assertEquals("only point 2 keeps 'extra' after deleting it from label=a", 1, after.size)
            assertEquals("the surviving 'extra' vector belongs to point 2 (label b)", 2uL, numId(after.first().id))
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 46: deleteFieldIndexDisablesFacet ─────────────────────────────

    @Test
    fun deleteFieldIndexDisablesFacet() {
        val shard = loadWithRankedPoints("delete-field-index")
        try {
            shard.update(operation = UpdateOperation.createFieldIndex(fieldName = "label", schema = PayloadSchemaType.KEYWORD))
            val response = shard.facet(request = FacetRequest(key = "label", limit = 10uL, exact = true, filter = null))
            val total = response.hits.fold(0uL) { acc, h -> acc + h.count }
            assertEquals("facet works while the index exists", 3uL, total)

            shard.update(operation = UpdateOperation.deleteFieldIndex(fieldName = "label"))
            var caught = false
            try {
                shard.facet(request = FacetRequest(key = "label", limit = 10uL, exact = true, filter = null))
            } catch (e: EdgeException) {
                caught = true
            }
            assertTrue("faceting a dropped index should throw a catchable EdgeException", caught)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 47: deleteVectorNameRemovesField ──────────────────────────────

    @Test
    fun deleteVectorNameRemovesField() {
        val shard = loadWithThreePoints("delete-vector-name")
        try {
            shard.update(operation = UpdateOperation.createDenseVector(vectorName = "extra", size = 2uL, distance = Distance.DOT))
            assertNotNull("created named vector should be listed by config()", shard.config().vectorData["extra"])

            shard.update(operation = UpdateOperation.deleteVectorName(vectorName = "extra"))
            assertNull("deleted named vector should no longer be listed by config()", shard.config().vectorData["extra"])

            var caught = false
            try {
                shard.search(
                    request = SearchRequest(
                        query = Query.Nearest(vector = NamedVector.Dense(listOf(5.0f, 6.0f)), using = "extra"),
                        limit = 10uL, offset = null, filter = null, params = null, withVector = null, withPayload = null, scoreThreshold = null,
                    ),
                )
            } catch (e: EdgeException) {
                caught = true
            }
            assertTrue("searching a deleted vector name should throw EdgeException", caught)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 48: datetimeOrderByScroll ─────────────────────────────────────

    @Test
    fun datetimeOrderByScroll() {
        val dir = freshDir("datetime-orderby")
        val shard = EdgeShard.load(path = dir.absolutePath, config = defaultConfig())
        try {
            shard.update(
                operation = UpdateOperation.upsertPoints(
                    points = listOf(
                        Point(PointId.NumId(1uL), Vector.Single(listOf(1.0f, 0.0f, 0.0f, 0.0f)), """{"ts": "2021-01-01T00:00:00Z"}"""),
                        Point(PointId.NumId(2uL), Vector.Single(listOf(0.0f, 1.0f, 0.0f, 0.0f)), """{"ts": "2022-01-01T00:00:00Z"}"""),
                        Point(PointId.NumId(3uL), Vector.Single(listOf(0.0f, 0.0f, 1.0f, 0.0f)), """{"ts": "2023-01-01T00:00:00Z"}"""),
                    ),
                ),
            )
            shard.update(operation = UpdateOperation.createFieldIndex(fieldName = "ts", schema = PayloadSchemaType.DATETIME))

            val page = shard.scroll(
                request = ScrollRequest(
                    offset = null, limit = 10uL, filter = null,
                    withPayload = WithPayload.Bool(true), withVector = WithVector.Bool(false),
                    orderBy = OrderBy(key = "ts", direction = Direction.ASC, startFrom = StartFrom.Datetime("2022-01-01T00:00:00Z")),
                ),
            )
            assertEquals("datetime StartFrom (inclusive at 2022) should include only 2022 and 2023", 2, page.records.size)
            val ids = page.records.mapNotNull { numId(it.id) }
            assertEquals("ascending datetime order should yield 2022 then 2023", listOf(2uL, 3uL), ids)
            assertTrue("first record should be the 2022 point", (page.records.first().payload ?: "").contains("2022"))
            val orderValue = page.records.first().orderValue
            assertNotNull("datetime order-by should populate order_value", orderValue)
            assertTrue("datetime order_value should be numeric", orderValue is OrderValue.Int || orderValue is OrderValue.Float)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 49: queryOrderByRanksByPayloadField ───────────────────────────

    @Test
    fun queryOrderByRanksByPayloadField() {
        val shard = loadWithRankedPoints("query-order-by")
        try {
            shard.update(operation = UpdateOperation.createFieldIndex(fieldName = "rank", schema = PayloadSchemaType.INTEGER))

            val results = shard.query(
                request = QueryRequest(
                    limit = 10uL, offset = null,
                    query = ScoringQuery.OrderBy(orderBy = OrderBy(key = "rank", direction = Direction.ASC, startFrom = null)),
                    prefetches = emptyList(), withVector = null, withPayload = null, filter = null, scoreThreshold = null, params = null,
                ),
            )
            assertEquals("order-by query should return all 3 points", 3, results.size)
            val ids = results.mapNotNull { numId(it.id) }
            assertEquals("results must be ranked ascending by the 'rank' payload field", listOf(2uL, 3uL, 1uL), ids)
            val first = results.first().orderValue
            assertTrue("order-by query should populate an integer orderValue", first is OrderValue.Int)
            assertEquals("the smallest rank (10, point 2) sorts first and carries orderValue 10", 10L, (first as OrderValue.Int).`value`)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 50: createSparseVectorAtRuntime ───────────────────────────────

    @Test
    fun createSparseVectorAtRuntime() {
        val shard = loadWithThreePoints("runtime-sparse")
        try {
            shard.update(operation = UpdateOperation.createSparseVector(vectorName = "sp"))

            shard.update(
                operation = UpdateOperation.updateVectors(
                    pointVectors = listOf(
                        PointVectors(id = PointId.NumId(1uL), vector = Vector.Named(mapOf("sp" to NamedVector.Sparse(SparseVector(indices = listOf(1u, 5u, 9u), values = listOf(0.5f, 1.5f, 2.5f)))))),
                        PointVectors(id = PointId.NumId(2uL), vector = Vector.Named(mapOf("sp" to NamedVector.Sparse(SparseVector(indices = listOf(0u, 1u), values = listOf(1.0f, 1.0f)))))),
                    ),
                ),
            )

            val results = shard.search(
                request = SearchRequest(
                    query = Query.Nearest(vector = NamedVector.Sparse(SparseVector(indices = listOf(9u), values = listOf(1.0f))), using = "sp"),
                    limit = 10uL, offset = null, filter = null, params = null,
                    withVector = null, withPayload = null, scoreThreshold = null,
                ),
            )
            assertEquals("the runtime sparse field, queried on index 9, must match exactly point 1", 1, results.size)
            assertEquals("only point 1 carries sparse index 9", 1uL, numId(results.first().id))
            assertEquals("sparse score should be the overlapping-term dot product", 2.5f, results.first().score, 1e-4f)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 51: deleteVectorsByPointIdKeepsPoint ──────────────────────────

    @Test
    fun deleteVectorsByPointIdKeepsPoint() {
        val shard = loadWithThreePoints("del-vectors-by-id")
        try {
            shard.update(operation = UpdateOperation.createDenseVector(vectorName = "extra", size = 2uL, distance = Distance.DOT))
            shard.update(
                operation = UpdateOperation.updateVectors(
                    pointVectors = listOf(
                        PointVectors(id = PointId.NumId(1uL), vector = Vector.Named(mapOf("extra" to NamedVector.Dense(listOf(5.0f, 6.0f))))),
                        PointVectors(id = PointId.NumId(2uL), vector = Vector.Named(mapOf("extra" to NamedVector.Dense(listOf(5.0f, 6.0f))))),
                    ),
                ),
            )

            val before = shard.search(
                request = SearchRequest(
                    query = Query.Nearest(vector = NamedVector.Dense(listOf(5.0f, 6.0f)), using = "extra"),
                    limit = 10uL, offset = null, filter = null, params = null, withVector = null, withPayload = null, scoreThreshold = null,
                ),
            )
            assertEquals("points 1 and 2 both carry the 'extra' vector before deletion", 2, before.size)

            shard.update(operation = UpdateOperation.deleteVectors(pointIds = listOf(PointId.NumId(1uL)), vectorNames = listOf("extra")))

            val after = shard.search(
                request = SearchRequest(
                    query = Query.Nearest(vector = NamedVector.Dense(listOf(5.0f, 6.0f)), using = "extra"),
                    limit = 10uL, offset = null, filter = null, params = null, withVector = null, withPayload = null, scoreThreshold = null,
                ),
            )
            assertEquals("after deleting 'extra' from point 1, only point 2 keeps it", 1, after.size)
            assertEquals("the surviving 'extra' vector belongs to point 2", 2uL, numId(after.first().id))

            assertEquals("deleteVectors must not delete the point", 3uL, shard.count(request = CountRequest(filter = null, exact = true)))
            val got = shard.retrieve(request = RetrieveRequest(pointIds = listOf(PointId.NumId(1uL)), withPayload = WithPayload.Bool(true), withVector = null))
            assertTrue("point 1's payload must survive vector deletion: ${got.first().payload}", (got.first().payload ?: "").contains("\"label\""))
            val defaultSearch = shard.search(
                request = SearchRequest(
                    query = Query.Nearest(vector = NamedVector.Dense(listOf(1.0f, 0.0f, 0.0f, 0.0f)), using = null),
                    limit = 1uL, offset = null, filter = null, params = null, withVector = null, withPayload = null, scoreThreshold = null,
                ),
            )
            assertEquals("point 1's default vector must survive deletion of the named 'extra' vector", 1uL, numId(defaultSearch.first().id))
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 52: conditionalUpsertOverwritesMatchingOnly ───────────────────

    @Test
    fun conditionalUpsertOverwritesMatchingOnly() {
        val shard = loadWithThreePoints("conditional-upsert")
        try {
            shard.update(
                operation = UpdateOperation.upsertPoints(
                    points = listOf(
                        Point(PointId.NumId(1uL), Vector.Single(listOf(1.0f, 0.0f, 0.0f, 0.0f)), """{"label": "a", "touched": "yes"}"""),
                        Point(PointId.NumId(2uL), Vector.Single(listOf(0.0f, 1.0f, 0.0f, 0.0f)), """{"label": "b", "touched": "yes"}"""),
                        Point(PointId.NumId(4uL), Vector.Single(listOf(0.0f, 0.0f, 0.0f, 1.0f)), """{"label": "d"}"""),
                    ),
                    condition = labelFilter("b"),
                ),
            )

            assertEquals("the new point 4 must be inserted", 4uL, shard.count(request = CountRequest(filter = null, exact = true)))

            val p1 = shard.retrieve(request = RetrieveRequest(pointIds = listOf(PointId.NumId(1uL)), withPayload = WithPayload.Bool(true), withVector = null))
            assertFalse("non-matching point 1 must NOT be overwritten: ${p1.first().payload}", (p1.first().payload ?: "").contains("\"touched\""))

            val p2 = shard.retrieve(request = RetrieveRequest(pointIds = listOf(PointId.NumId(2uL)), withPayload = WithPayload.Bool(true), withVector = null))
            assertTrue("matching point 2 must be overwritten: ${p2.first().payload}", (p2.first().payload ?: "").contains("\"touched\""))
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 53: conditionalUpdateVectorsUpdatesMatchingOnly ───────────────

    @Test
    fun conditionalUpdateVectorsUpdatesMatchingOnly() {
        val shard = loadWithThreePoints("conditional-update-vectors")
        try {
            shard.update(
                operation = UpdateOperation.updateVectors(
                    pointVectors = listOf(PointVectors(id = PointId.NumId(1uL), vector = Vector.Single(listOf(0.0f, 0.0f, 0.0f, 9.0f)))),
                    condition = labelFilter("b"),
                ),
            )
            val unchanged = shard.search(
                request = SearchRequest(
                    query = Query.Nearest(vector = NamedVector.Dense(listOf(1.0f, 0.0f, 0.0f, 0.0f)), using = null),
                    limit = 1uL, offset = null, filter = null, params = null, withVector = null, withPayload = null, scoreThreshold = null,
                ),
            )
            assertEquals("point 1 must still top a [1,0,0,0] search — its vector was not updated", 1uL, numId(unchanged.first().id))
            assertEquals("point 1's original vector [1,0,0,0] must be intact (dot == 1.0)", 1.0f, unchanged.first().score, 1e-4f)

            shard.update(
                operation = UpdateOperation.updateVectors(
                    pointVectors = listOf(PointVectors(id = PointId.NumId(1uL), vector = Vector.Single(listOf(0.0f, 0.0f, 0.0f, 9.0f)))),
                    condition = labelFilter("a"),
                ),
            )
            val updated = shard.search(
                request = SearchRequest(
                    query = Query.Nearest(vector = NamedVector.Dense(listOf(0.0f, 0.0f, 0.0f, 9.0f)), using = null),
                    limit = 1uL, offset = null, filter = null, params = null, withVector = null, withPayload = null, scoreThreshold = null,
                ),
            )
            assertEquals("after the matching update, point 1 must top a [0,0,0,9] search", 1uL, numId(updated.first().id))
            assertEquals("point 1's vector must now be [0,0,0,9] (dot == 81.0)", 81.0f, updated.first().score, 1e-3f)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 54: queryGroupsDecodeKeysAndHits ──────────────────────────────

    @Test
    fun queryGroupsDecodeKeysAndHits() {
        val shard = loadWithRankedPoints("grouping-membership")
        try {
            val groups = shard.queryGroups(
                request = GroupRequest(
                    query = QueryRequest(
                        limit = 10uL, offset = null,
                        query = ScoringQuery.Vector(query = Query.Nearest(vector = NamedVector.Dense(listOf(1.0f, 0.0f, 0.0f, 0.0f)), using = null)),
                        prefetches = emptyList(), withVector = null, withPayload = null, filter = null, scoreThreshold = null, params = null,
                    ),
                    groupBy = "label",
                    groups = 10uL,
                    groupSize = 10uL,
                ),
            )
            assertEquals("grouping by label should yield one group per distinct value", 2, groups.size)

            val membership = mutableMapOf<String, Set<ULong>>()
            for (group in groups) {
                val key = group.key
                assertTrue("label group key should decode as GroupId.String, got $key", key is GroupId.String)
                val ids = group.hits.mapNotNull { numId(it.id) }.toSet()
                membership[(key as GroupId.String).`value`] = ids
            }
            assertEquals("group 'a' should contain exactly ids {1,3}", setOf(1uL, 3uL), membership["a"])
            assertEquals("group 'b' should contain exactly id {2}", setOf(2uL), membership["b"])
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 55: recommendSinglePositiveIsNearestNeighbor ──────────────────

    @Test
    fun recommendSinglePositiveIsNearestNeighbor() {
        val shard = loadWithThreePoints("recommend-nearest")
        try {
            val results = shard.query(
                request = QueryRequest(
                    limit = 10uL, offset = null,
                    query = ScoringQuery.Vector(
                        query = Query.Recommend(
                            positives = listOf(NamedVector.Dense(listOf(1.0f, 0.0f, 0.0f, 0.0f))),
                            negatives = emptyList(),
                            strategy = null,
                            using = null,
                        ),
                    ),
                    prefetches = emptyList(), withVector = null, withPayload = null, filter = null, scoreThreshold = null, params = null,
                ),
            )
            assertEquals("recommend should score all 3 points", 3, results.size)
            assertEquals("BestScore against [1,0,0,0] makes point 1 the exact top hit", PointId.NumId(1uL), results.first().id)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 56: dbsfFusionOverPrefetches ──────────────────────────────────

    @Test
    fun dbsfFusionOverPrefetches() {
        val shard = loadWithThreePoints("dbsf-fusion")
        try {
            fun branch(vector: List<Float>) = Prefetch(
                limit = 3uL,
                query = ScoringQuery.Vector(query = Query.Nearest(vector = NamedVector.Dense(vector), using = null)),
                prefetches = emptyList(), filter = null, scoreThreshold = null, params = null,
            )

            val results = shard.query(
                request = QueryRequest(
                    limit = 3uL, offset = null,
                    query = ScoringQuery.Fusion(fusion = Fusion.Dbsf),
                    prefetches = listOf(branch(listOf(1.0f, 0.0f, 0.0f, 0.0f)), branch(listOf(0.0f, 0.0f, 1.0f, 0.0f))),
                    withVector = null, withPayload = null, filter = null, scoreThreshold = null, params = null,
                ),
            )
            assertEquals("DBSF over two prefetches should fuse to all three points", 3, results.size)
            val ids = results.mapNotNull { numId(it.id) }.sorted()
            assertEquals("fused result set should contain each point exactly once", listOf(1uL, 2uL, 3uL), ids)
            for (i in 1 until results.size) {
                assertTrue("fused scores must be in descending order", results[i - 1].score >= results[i].score)
            }
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 57: recommendSumScoresStrategy ────────────────────────────────

    @Test
    fun recommendSumScoresStrategy() {
        val shard = loadWithThreePoints("recommend-sum-scores")
        try {
            val results = shard.query(
                request = QueryRequest(
                    limit = 10uL, offset = null,
                    query = ScoringQuery.Vector(
                        query = Query.Recommend(
                            positives = listOf(NamedVector.Dense(listOf(1.0f, 0.0f, 0.0f, 0.0f))),
                            negatives = emptyList(),
                            strategy = RecommendStrategy.SUM_SCORES,
                            using = null,
                        ),
                    ),
                    prefetches = emptyList(), withVector = null, withPayload = null, filter = null, scoreThreshold = null, params = null,
                ),
            )
            assertEquals("sumScores recommend should score all 3 points", 3, results.size)
            assertEquals("sumScores with positive [1,0,0,0] ranks point 1 first", PointId.NumId(1uL), results.first().id)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 58: unpackSnapshotBadPathThrows ───────────────────────────────

    @Test
    fun unpackSnapshotBadPathThrows() {
        val target = freshDir("unpack-target")
        var caught = false
        try {
            unpackSnapshot(snapshotPath = "/definitely/missing/nope.snapshot", targetPath = target.absolutePath)
        } catch (e: EdgeException.OperationException) {
            caught = true
        }
        assertTrue("a missing snapshot path should throw OperationException", caught)
    }

    // ── Test 59: formulaExpressionBuilderTree ──────────────────────────────

    @Test
    fun formulaExpressionBuilderTree() {
        val shard = loadWithThreePoints("formula-tree")
        try {
            // (($score * 2) / 4) + 1  ==  score/2 + 1  (monotonic increasing in score).
            val expression = Expression.sum(
                listOf(
                    Expression.div(
                        left = Expression.mult(listOf(Expression.variable("\$score"), Expression.constant(2.0f))),
                        right = Expression.constant(4.0f),
                        byZeroDefault = null,
                    ),
                    Expression.constant(1.0f),
                ),
            )

            val results = shard.query(
                request = QueryRequest(
                    limit = 10uL, offset = null,
                    query = ScoringQuery.Formula(expression = expression, defaults = emptyMap()),
                    prefetches = listOf(
                        Prefetch(
                            limit = 10uL,
                            query = ScoringQuery.Vector(query = Query.Nearest(vector = NamedVector.Dense(listOf(1.0f, 0.0f, 0.0f, 0.0f)), using = null)),
                            prefetches = emptyList(), filter = null, scoreThreshold = null, params = null,
                        ),
                    ),
                    withVector = null, withPayload = null, filter = null, scoreThreshold = null, params = null,
                ),
            )
            assertEquals("formula rescoring should return all prefetched points", 3, results.size)
            assertEquals("a monotonic formula preserves the nearest neighbor (point 1) on top", PointId.NumId(1uL), results.first().id)
            assertEquals("score/2 + 1 of point 1's prefetch score (1.0) is 1.5", 1.5f, results.first().score, 1e-4f)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 60: topLevelMultiVectorRoundTrip ──────────────────────────────

    @Test
    fun topLevelMultiVectorRoundTrip() {
        val dir = freshDir("top-level-multivector")
        val config = EdgeConfig(
            vectorData = mapOf(
                "" to VectorDataConfig(
                    size = 2uL,
                    distance = Distance.DOT,
                    quantizationConfig = null,
                    multivectorConfig = MultiVectorConfig(comparator = MultiVectorComparator.MAX_SIM),
                    datatype = null,
                    hnswConfig = null,
                ),
            ),
            sparseVectorData = emptyMap(),
        )
        val shard = EdgeShard.load(path = dir.absolutePath, config = config)
        try {
            shard.update(
                operation = UpdateOperation.upsertPoints(
                    points = listOf(
                        Point(PointId.NumId(1uL), Vector.MultiDense(listOf(listOf(1.0f, 2.0f), listOf(3.0f, 4.0f))), null),
                        Point(PointId.NumId(2uL), Vector.MultiDense(listOf(listOf(0.0f, 1.0f))), null),
                    ),
                ),
            )

            val results = shard.search(
                request = SearchRequest(
                    query = Query.Nearest(vector = NamedVector.MultiDense(listOf(listOf(3.0f, 4.0f))), using = null),
                    limit = 10uL, offset = null, filter = null, params = null,
                    withVector = null, withPayload = null, scoreThreshold = null,
                ),
            )
            assertEquals("both unnamed multi-vector points should be scored", 2, results.size)
            assertEquals("MaxSim: point 1 (best row dot 25) beats point 2 (4)", 1uL, numId(results.first().id))
            assertEquals("MaxSim score should be the best per-row dot product", 25.0f, results.first().score, 1e-3f)
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 61: retrieveMixOfExistingAndMissingIds ────────────────────────

    @Test
    fun retrieveMixOfExistingAndMissingIds() {
        val shard = loadWithThreePoints("retrieve-mix")
        try {
            val got = shard.retrieve(
                request = RetrieveRequest(
                    pointIds = listOf(PointId.NumId(1uL), PointId.NumId(999uL)),
                    withPayload = WithPayload.Bool(true),
                    withVector = null,
                ),
            )
            assertEquals("only the existing id should come back; the missing id is silently omitted", 1, got.size)
            assertEquals("the surviving record should be the existing point 1", 1uL, numId(got.first().id))
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }

    // ── Test 62: emptyShardReadsAreEmpty ───────────────────────────────────

    @Test
    fun emptyShardReadsAreEmpty() {
        val dir = freshDir("empty-shard")
        val shard = EdgeShard.load(path = dir.absolutePath, config = defaultConfig())
        try {
            val search = shard.search(
                request = SearchRequest(
                    query = Query.Nearest(vector = NamedVector.Dense(listOf(1.0f, 0.0f, 0.0f, 0.0f)), using = null),
                    limit = 10uL, offset = null, filter = null, params = null,
                    withVector = null, withPayload = null, scoreThreshold = null,
                ),
            )
            assertTrue("search over an empty shard returns no results", search.isEmpty())

            val page = shard.scroll(
                request = ScrollRequest(
                    offset = null, limit = 10uL, filter = null,
                    withPayload = WithPayload.Bool(false), withVector = WithVector.Bool(false), orderBy = null,
                ),
            )
            assertTrue("scroll over an empty shard returns no records", page.records.isEmpty())
            assertNull("an empty scroll has no next offset", page.nextOffset)

            assertEquals("an empty shard counts zero points", 0uL, shard.count(request = CountRequest(filter = null, exact = true)))
        } finally {
            try { shard.unload() } catch (_: Exception) {}
        }
    }
}
