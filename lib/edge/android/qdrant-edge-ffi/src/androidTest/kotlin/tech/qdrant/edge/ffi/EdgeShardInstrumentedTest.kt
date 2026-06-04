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
}
