import Foundation

// Hand-written async layer for the Qdrant Edge Swift SDK.
//
// UniFFI generates BLOCKING functions (the Rust FFI is synchronous). These
// `…Async` wrappers run the blocking call on a dedicated `DispatchQueue` —
// deliberately NOT the Swift cooperative thread pool, where a long synchronous
// DB call would tie up one of its few threads and can starve structured
// concurrency — then bridge the result back with a checked continuation. They
// are opt-in: the plain blocking method stays available.
//
// RULE: wrap every operation that can touch disk, compute, or block. Skip only
// guaranteed-instant in-memory getters. This mirrors the Kotlin `Coroutines.kt`
// policy 1:1 — keep the two in sync when the FFI surface changes.
//
// WHEN YOU ADD A NEW FFI OPERATION: add its `…Async` wrapper here. Only skip it
// if it is a pure in-memory getter, and record it under "NOT wrapped".
//
// NOT wrapped (guaranteed instant — pure in-memory reads, no disk I/O):
//   • info()   — aggregates in-memory segment counters
//   • config() — returns the in-memory config
//   • path()   — returns the stored path string

/// Dedicated executor for blocking EdgeShard work. Concurrent, so independent
/// reads run in parallel; the engine serializes writes internally. Blocking work
/// runs here rather than on the Swift cooperative pool so it can't starve it.
private let edgeBlockingQueue = DispatchQueue(
    label: "io.qdrant.edge.blocking",
    qos: .userInitiated,
    attributes: .concurrent
)

/// Runs a blocking, throwing call on ``edgeBlockingQueue`` and suspends until it
/// finishes, keeping it off the Swift cooperative thread pool.
private func runBlocking<T: Sendable>(
    _ body: @escaping @Sendable () throws -> T
) async throws -> T {
    try await withCheckedThrowingContinuation { continuation in
        edgeBlockingQueue.async {
            do {
                continuation.resume(returning: try body())
            } catch {
                continuation.resume(throwing: error)
            }
        }
    }
}

// MARK: - Reads

public extension EdgeShard {
    /// Runs ``search(request:)`` on a background queue.
    func searchAsync(request: SearchRequest) async throws -> [ScoredPoint] {
        try await runBlocking { try self.search(request: request) }
    }

    /// Runs ``query(request:)`` on a background queue.
    func queryAsync(request: QueryRequest) async throws -> [ScoredPoint] {
        try await runBlocking { try self.query(request: request) }
    }

    /// Runs ``queryGroups(request:)`` on a background queue.
    func queryGroupsAsync(request: GroupRequest) async throws -> [Group] {
        try await runBlocking { try self.queryGroups(request: request) }
    }

    /// Runs ``scroll(request:)`` on a background queue.
    func scrollAsync(request: ScrollRequest) async throws -> ScrollResponse {
        try await runBlocking { try self.scroll(request: request) }
    }

    /// Runs ``retrieve(request:)`` on a background queue.
    func retrieveAsync(request: RetrieveRequest) async throws -> [Record] {
        try await runBlocking { try self.retrieve(request: request) }
    }

    /// Runs ``facet(request:)`` on a background queue.
    func facetAsync(request: FacetRequest) async throws -> FacetResponse {
        try await runBlocking { try self.facet(request: request) }
    }

    /// Runs ``count(request:)`` on a background queue.
    func countAsync(request: CountRequest) async throws -> UInt64 {
        try await runBlocking { try self.count(request: request) }
    }
}

// MARK: - Writes

public extension EdgeShard {
    /// Runs ``update(operation:)`` on a background queue. All writes (upsert,
    /// delete, payload, index) flow through a single `UpdateOperation`.
    func updateAsync(operation: UpdateOperation) async throws {
        try await runBlocking { try self.update(operation: operation) }
    }

    /// Runs ``updateFromSnapshot(snapshotPath:tmpDir:)`` on a background queue —
    /// restoring a snapshot unpacks and merges an archive (heavy I/O).
    func updateFromSnapshotAsync(snapshotPath: String, tmpDir: String? = nil) async throws {
        try await runBlocking { try self.updateFromSnapshot(snapshotPath: snapshotPath, tmpDir: tmpDir) }
    }
}

// MARK: - Maintenance

public extension EdgeShard {
    /// Runs ``optimize()`` on a background queue. Building the HNSW index is
    /// CPU-bound and can run for a long time — the most important call to offload.
    func optimizeAsync() async throws -> Bool {
        try await runBlocking { try self.optimize() }
    }

    /// Runs ``flush()`` (an fsync) on a background queue.
    func flushAsync() async throws {
        try await runBlocking { try self.flush() }
    }
}

// MARK: - Config setters (each persists to disk)

public extension EdgeShard {
    /// Runs ``setHnswConfig(hnswConfig:)`` on a background queue.
    func setHnswConfigAsync(hnswConfig: HnswIndexConfig) async throws {
        try await runBlocking { try self.setHnswConfig(hnswConfig: hnswConfig) }
    }

    /// Runs ``setVectorHnswConfig(vectorName:hnswConfig:)`` on a background queue.
    func setVectorHnswConfigAsync(vectorName: String, hnswConfig: HnswIndexConfig) async throws {
        try await runBlocking { try self.setVectorHnswConfig(vectorName: vectorName, hnswConfig: hnswConfig) }
    }

    /// Runs ``setOptimizersConfig(optimizers:)`` on a background queue.
    func setOptimizersConfigAsync(optimizers: OptimizersConfig) async throws {
        try await runBlocking { try self.setOptimizersConfig(optimizers: optimizers) }
    }
}

// MARK: - Lifecycle & snapshot

public extension EdgeShard {
    /// Runs ``unload()`` on a background queue — unload performs a final fsync
    /// before releasing the shard, so it can block on slow storage.
    func unloadAsync() async throws {
        try await runBlocking { try self.unload() }
    }

    /// Runs ``snapshotManifest()`` on a background queue.
    func snapshotManifestAsync() async throws -> String {
        try await runBlocking { try self.snapshotManifest() }
    }
}

// MARK: - Open / create (static factories)

public extension EdgeShard {
    /// Opens an existing shard (or creates one from `config`) on a background
    /// queue. Opening a large shard reads its segments from disk — keep it off
    /// the main thread to avoid a UI hang.
    static func loadAsync(path: String, config: EdgeConfig?) async throws -> EdgeShard {
        try await runBlocking { try EdgeShard.load(path: path, config: config) }
    }

    /// Creates a new shard on a background queue.
    static func createAsync(path: String, config: EdgeConfig) async throws -> EdgeShard {
        try await runBlocking { try EdgeShard.create(path: path, config: config) }
    }
}

// MARK: - Snapshot utilities (package-level function — not an EdgeShard method)

/// Runs ``unpackSnapshot(snapshotPath:targetPath:)`` on a background queue.
/// Unpacking a snapshot archive to disk is heavy I/O; this is a package-level
/// function (no shard involved), so unlike the wrappers above it is not an
/// `EdgeShard` method.
public func unpackSnapshotAsync(snapshotPath: String, targetPath: String) async throws {
    try await runBlocking { try unpackSnapshot(snapshotPath: snapshotPath, targetPath: targetPath) }
}
