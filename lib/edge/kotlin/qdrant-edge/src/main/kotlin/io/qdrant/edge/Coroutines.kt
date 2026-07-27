@file:Suppress("unused")

package io.qdrant.edge

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

// KEEP THIS FILE PLATFORM-AGNOSTIC — no Android imports. It is hand-written
// (only kotlinx.coroutines + the same-package generated types) so it can be
// reused verbatim if a JVM-desktop artifact is added later. The generated
// bindings share this `io.qdrant.edge` package (uniffi.toml), so the domain
// types below (EdgeShard, SearchRequest, …) need no import.
// See the qdrant-edge-kotlin-desktop-decision memory / project ADR.

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
    request: RetrieveRequest,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): List<Record> = withContext(dispatcher) { retrieve(request) }

/**
 * Suspends and runs [EdgeShard.update] on [dispatcher] (default [Dispatchers.IO]).
 *
 * A batch upsert is one of the heaviest write operations; running it here keeps
 * ingest off the main thread.
 */
public suspend fun EdgeShard.updateAsync(
    operation: UpdateOperation,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): Unit = withContext(dispatcher) { update(operation) }

/**
 * Suspends and runs [EdgeShard.optimize] on [dispatcher] (default [Dispatchers.IO]).
 *
 * Building the HNSW index is CPU-bound and can run for a long time on a large
 * shard — the most important call to keep off the main thread.
 */
public suspend fun EdgeShard.optimizeAsync(
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): Boolean = withContext(dispatcher) { optimize() }
