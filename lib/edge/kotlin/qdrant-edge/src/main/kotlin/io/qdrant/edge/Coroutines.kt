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

// ── Coroutine wrapper policy — READ BEFORE ADDING AN OPERATION ────────────────
// UniFFI generates BLOCKING bindings (the Rust FFI is synchronous). These
// hand-written `…Async` wrappers run a blocking EdgeShard call on a background
// dispatcher (default Dispatchers.IO) so it doesn't stall the caller's thread.
// They are opt-in: the plain blocking method stays available for callers that
// manage their own thread pool.
//
// RULE: wrap every operation that can touch disk, compute, or otherwise block.
// The ONLY exceptions are guaranteed-instant in-memory getters, where hopping to
// a dispatcher is pure overhead — those are listed under "NOT wrapped" below.
// There is no middle ground: if an op EVER does I/O (even a "small" fsync,
// config-persist, or manifest read), it is wrapped.
//
// WHEN YOU ADD A NEW FFI OPERATION: wrap it here. Only skip the wrapper if the
// op is a pure in-memory getter, and then add it to "NOT wrapped" with a reason —
// so the wrap/skip line stays legible for the next reader.
//
// NOT wrapped (guaranteed instant — pure in-memory reads, no disk I/O):
//   • info()      — aggregates in-memory segment counters
//   • config()    — returns the in-memory config
//   • path()      — returns the stored path string
// (Everything else touches disk and IS wrapped: unload()/flush() fsync, the
//  set*Config() setters persist to disk, snapshotManifest() reads file metadata.)
// ─────────────────────────────────────────────────────────────────────────────

// ── Reads ─────────────────────────────────────────────────────────────────────

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

/** Suspends and runs [EdgeShard.queryGroups] on [dispatcher] (default [Dispatchers.IO]). */
public suspend fun EdgeShard.queryGroupsAsync(
    request: GroupRequest,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): List<Group> = withContext(dispatcher) { queryGroups(request) }

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

/** Suspends and runs [EdgeShard.facet] on [dispatcher] (default [Dispatchers.IO]). */
public suspend fun EdgeShard.facetAsync(
    request: FacetRequest,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): FacetResponse = withContext(dispatcher) { facet(request) }

/** Suspends and runs [EdgeShard.count] on [dispatcher] (default [Dispatchers.IO]). */
public suspend fun EdgeShard.countAsync(
    request: CountRequest,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): ULong = withContext(dispatcher) { count(request) }

// ── Writes ──────────────────────────────────────────────────────────────────

/**
 * Suspends and runs [EdgeShard.update] on [dispatcher] (default [Dispatchers.IO]).
 *
 * All writes (upsert, delete, payload, index) flow through a single
 * [UpdateOperation]; a batch upsert is one of the heaviest, so this keeps ingest
 * off the main thread.
 */
public suspend fun EdgeShard.updateAsync(
    operation: UpdateOperation,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): Unit = withContext(dispatcher) { update(operation) }

/**
 * Suspends and runs [EdgeShard.updateFromSnapshot] on [dispatcher] (default
 * [Dispatchers.IO]). Restoring a snapshot unpacks and merges an archive — heavy
 * I/O worth keeping off the main thread.
 */
public suspend fun EdgeShard.updateFromSnapshotAsync(
    snapshotPath: String,
    tmpDir: String? = null,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): Unit = withContext(dispatcher) { updateFromSnapshot(snapshotPath, tmpDir) }

// ── Maintenance ───────────────────────────────────────────────────────────────

/**
 * Suspends and runs [EdgeShard.optimize] on [dispatcher] (default [Dispatchers.IO]).
 *
 * Building the HNSW index is CPU-bound and can run for a long time on a large
 * shard — the most important call to keep off the main thread.
 */
public suspend fun EdgeShard.optimizeAsync(
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): Boolean = withContext(dispatcher) { optimize() }

/** Suspends and runs [EdgeShard.flush] (an fsync) on [dispatcher] (default [Dispatchers.IO]). */
public suspend fun EdgeShard.flushAsync(
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): Unit = withContext(dispatcher) { flush() }

// ── Config setters (each persists to disk) ────────────────────────────────────

/** Suspends and runs [EdgeShard.setHnswConfig] on [dispatcher] (default [Dispatchers.IO]). */
public suspend fun EdgeShard.setHnswConfigAsync(
    hnswConfig: HnswIndexConfig,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): Unit = withContext(dispatcher) { setHnswConfig(hnswConfig) }

/** Suspends and runs [EdgeShard.setVectorHnswConfig] on [dispatcher] (default [Dispatchers.IO]). */
public suspend fun EdgeShard.setVectorHnswConfigAsync(
    vectorName: String,
    hnswConfig: HnswIndexConfig,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): Unit = withContext(dispatcher) { setVectorHnswConfig(vectorName, hnswConfig) }

/** Suspends and runs [EdgeShard.setOptimizersConfig] on [dispatcher] (default [Dispatchers.IO]). */
public suspend fun EdgeShard.setOptimizersConfigAsync(
    optimizers: OptimizersConfig,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): Unit = withContext(dispatcher) { setOptimizersConfig(optimizers) }

// ── Lifecycle & snapshot ──────────────────────────────────────────────────────

/**
 * Suspends and runs [EdgeShard.unload] on [dispatcher] (default [Dispatchers.IO]).
 *
 * Unload performs a final fsync before releasing the shard, so it can block on
 * slow storage — keep it off the main thread.
 */
public suspend fun EdgeShard.unloadAsync(
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): Unit = withContext(dispatcher) { unload() }

/** Suspends and runs [EdgeShard.snapshotManifest] on [dispatcher] (default [Dispatchers.IO]). */
public suspend fun EdgeShard.snapshotManifestAsync(
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): String = withContext(dispatcher) { snapshotManifest() }

// ── Open / create (constructors) ──────────────────────────────────────────────
// Opening a shard reads its segments off disk; on a large shard that is a classic
// main-thread stall (ANR) on Android. Wrapped on the companion so
// `EdgeShard.loadAsync(...)` mirrors the blocking `EdgeShard.load(...)`.

/** Suspends and runs [EdgeShard.load] on [dispatcher] (default [Dispatchers.IO]). */
public suspend fun EdgeShard.Companion.loadAsync(
    path: String,
    config: EdgeConfig?,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): EdgeShard = withContext(dispatcher) { EdgeShard.load(path, config) }

/** Suspends and runs [EdgeShard.create] on [dispatcher] (default [Dispatchers.IO]). */
public suspend fun EdgeShard.Companion.createAsync(
    path: String,
    config: EdgeConfig,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): EdgeShard = withContext(dispatcher) { EdgeShard.create(path, config) }

// ── Snapshot utilities (package-level functions — not EdgeShard methods) ───────

/**
 * Suspends and runs [unpackSnapshot] on [dispatcher] (default [Dispatchers.IO]).
 *
 * Unpacking a snapshot archive to disk is heavy I/O. This is a package-level
 * function (no shard involved), so unlike the wrappers above it is not an
 * `EdgeShard` extension.
 */
public suspend fun unpackSnapshotAsync(
    snapshotPath: String,
    targetPath: String,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): Unit = withContext(dispatcher) { unpackSnapshot(snapshotPath, targetPath) }
