@file:Suppress("unused")

package tech.qdrant.edge

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import tech.qdrant.edge.ffi.EdgeShard
import tech.qdrant.edge.ffi.QueryRequest
import tech.qdrant.edge.ffi.Record
import tech.qdrant.edge.ffi.ScoredPoint
import tech.qdrant.edge.ffi.ScrollRequest
import tech.qdrant.edge.ffi.ScrollResponse
import tech.qdrant.edge.ffi.SearchRequest

// Optional `suspend` convenience wrappers for the heavy, blocking EdgeShard
// operations.
//
// UniFFI's threading model is "all calls block; the caller chooses the thread".
// The SDK therefore does NOT impose a dispatcher internally — these wrappers are
// opt-in. They simply move the blocking call onto a background dispatcher within
// the *caller's* coroutine (defaulting to Dispatchers.IO), so the search/query
// doesn't block the main thread. A caller who manages their own thread pool can
// keep calling the plain blocking `search(...)` instead.
//
// Each takes an optional `dispatcher` so the consumer can override the thread
// (e.g. a bounded pool) rather than the SDK deciding for them.

/** Suspends and runs [EdgeShard.search] on [dispatcher] (default [Dispatchers.IO]). */
public suspend fun EdgeShard.searchAsync(
    request: SearchRequest,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): List<ScoredPoint> = withContext(dispatcher) { search(request) }

/** Suspends and runs [EdgeShard.query] on [dispatcher] (default [Dispatchers.IO]). */
public suspend fun EdgeShard.queryAsync(
    request: QueryRequest,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): List<ScoredPoint> = withContext(dispatcher) { query(request) }

/** Suspends and runs [EdgeShard.scroll] on [dispatcher] (default [Dispatchers.IO]). */
public suspend fun EdgeShard.scrollAsync(
    request: ScrollRequest,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): ScrollResponse = withContext(dispatcher) { scroll(request) }

/** Suspends and runs [EdgeShard.retrieve] on [dispatcher] (default [Dispatchers.IO]). */
public suspend fun EdgeShard.retrieveAsync(
    pointIds: List<tech.qdrant.edge.ffi.PointId>,
    withPayload: tech.qdrant.edge.ffi.WithPayload? = null,
    withVector: tech.qdrant.edge.ffi.WithVector? = null,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): List<Record> = withContext(dispatcher) { retrieve(pointIds, withPayload, withVector) }
