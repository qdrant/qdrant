# Qdrant Architecture Overview

High-level overview of Qdrant as a vector search engine: how it is deployed, what the main components are, and how they interact.

---

## What is Qdrant

Qdrant is a vector similarity search engine written in Rust. It stores points — each consisting of one or more vectors and an optional JSON payload — and provides fast approximate nearest neighbor (ANN) search with filtering. It can run as a single node or as a distributed cluster with Raft-based consensus.

---

## Deployment Modes

### Single Node (Standalone)

A single `qdrant` binary serves REST (Actix-web) and gRPC (Tonic) APIs. All collections and their data live on the local filesystem. No consensus is used; collection metadata operations execute directly against the `TableOfContent`.

### Distributed Cluster

Multiple Qdrant nodes form a Raft cluster. Each node runs the same binary with `cluster.enabled = true`. Collection metadata operations (create, delete, transfers, etc.) go through Raft consensus. Point-level updates use direct replication between shard replicas — they do NOT go through Raft. Each node may hold a subset of shards for each collection.

---

## Layered Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    API Layer                             │
│   REST (Actix-web)  │  gRPC (Tonic)  │  Internal gRPC   │
├─────────────────────────────────────────────────────────┤
│                  Storage Layer                          │
│   Dispatcher  │  TableOfContent  │  RBAC/Auth  │ Audit  │
├─────────────────────────────────────────────────────────┤
│               Collection Layer                          │
│   Collection  │  ShardReplicaSet  │  Shard Transfers    │
├─────────────────────────────────────────────────────────┤
│            Shard Layer (local engine)                   │
│   SegmentHolder  │  WAL  │  Optimizers  │  ProxySegment │
├─────────────────────────────────────────────────────────┤
│                 Segment Layer                           │
│  Vectors  │  Payloads  │  Indexes  │  ID Tracker        │
└─────────────────────────────────────────────────────────┘
```

### API Layer (`src/actix/`, `src/tonic/`, `lib/api/`)

Two server implementations expose the same operations. REST uses Actix-web with JSON. gRPC uses Tonic with protobuf. Internal gRPC services handle peer-to-peer communication (shard replication, Raft messages, shard transfers). The `api` crate defines shared types and conversions between REST, gRPC, and internal representations.

See: [components/api-layer.md](components/api-layer.md)

### Storage Layer (`lib/storage/`)

The **TableOfContent (ToC)** is the central registry: it owns all `Collection` handles, resolves aliases, manages Tokio runtimes (search, update, general), and in cluster mode holds a link to propose consensus operations. The **Dispatcher** routes collection metadata operations through Raft (cluster) or directly to ToC (standalone). RBAC and audit logging live here.

See: [components/storage.md](components/storage.md), [components/rbac-and-auth.md](components/rbac-and-auth.md)

### Collection Layer (`lib/collection/`)

A **Collection** manages one logical collection. It holds a `ShardHolder` containing `ShardReplicaSet` instances — one per shard. Each replica set has an optional local shard and zero or more remote shard stubs (gRPC clients to other peers). The collection layer handles shard routing (hash ring), replication fan-out, shard transfers, and exposes search/query/update APIs that delegate to shards.

See: [components/collection.md](components/collection.md), [components/replication.md](components/replication.md)

### Shard Layer (`lib/shard/`)

The local storage engine for one shard. A **LocalShard** owns:
- A **WAL** (write-ahead log) for crash recovery
- A **SegmentHolder** managing multiple segments
- An **UpdateHandler** with three worker loops: update, optimization, flush
- **Optimizers** (merge, vacuum, indexing, config mismatch)

The shard layer defines update operations (`CollectionUpdateOperations`) and how they're applied to segments. It also contains `ProxySegment` for concurrent reads during optimization.

See: [components/shard.md](components/shard.md), [components/segment-optimizer.md](components/segment-optimizer.md)

### Segment Layer (`lib/segment/`)

The lowest-level persistent unit. A **Segment** stores:
- **Vectors** in dense, multi-dense, or sparse storage backends (mmap, volatile, RocksDB)
- **Payloads** (JSON metadata per point) in mmap or RocksDB storage
- **Indexes** for vector search (HNSW for dense, inverted index for sparse, plain brute-force)
- **Payload indexes** for filtering (keyword, numeric, geo, full-text, bool, UUID, etc.)
- **ID tracker** mapping external point IDs to internal dense offsets

See: [components/segment.md](components/segment.md)

---

## Crate Dependency Graph

```
qdrant (binary)
├── storage          ← service layer, ToC, Dispatcher, consensus, RBAC
│   ├── collection   ← distributed collections, shards, replication
│   │   ├── shard    ← local shard engine, segment holder, optimizers
│   │   │   ├── segment ← storage unit: vectors, payloads, indexes
│   │   │   │   ├── quantization ← vector compression algorithms
│   │   │   │   ├── sparse       ← sparse vector inverted indexes
│   │   │   │   ├── posting_list ← compressed posting lists
│   │   │   │   ├── gridstore    ← mmap id→blob storage
│   │   │   │   └── gpu          ← optional Vulkan GPU acceleration
│   │   │   └── wal  ← write-ahead log
│   │   └── api      ← REST/gRPC type definitions and conversions
│   └── macros       ← derive macros (Anonymize)
├── edge             ← embedded single-shard runtime
└── trififo          ← seqlock primitive
```

---

## Consensus and Cluster Coordination

Qdrant uses the `raft-rs` library for Raft consensus. Only **collection metadata operations** go through Raft (create/delete collection, aliases, shard transfers, replica state changes). Point updates use **direct replication** between shard replicas with logical clock ordering.

Key components:
- **ConsensusManager** (`lib/storage/`) — implements Raft `Storage`, applies committed entries
- **Consensus thread** (`src/consensus.rs`) — runs `RawNode` loop, exchanges messages with peers
- **OperationSender** — channel from API threads into the consensus pipeline

See: [components/consensus.md](components/consensus.md)

---

## Data Flow Summary

### Write Path (Point Upsert)
```
Client → REST/gRPC → Dispatcher (auth) → Collection (shard routing)
→ ShardReplicaSet (replication fan-out) → LocalShard (WAL write)
→ UpdateWorker (apply to segments) → Segment (vector + payload + id)
→ FlushWorker (periodic persist + WAL ack)
```
See: [execution-paths/upsert.md](execution-paths/upsert.md)

### Read Path (Search)
```
Client → REST/gRPC → Dispatcher (auth) → Collection (shard routing)
→ ShardReplicaSet (prefer local) → SegmentsSearcher (parallel over segments)
→ Segment (filter eval + HNSW/brute-force) → aggregate results
```
See: [execution-paths/search.md](execution-paths/search.md)

### Metadata Path (Create Collection)
```
Client → Dispatcher → [Raft consensus] → ToC.perform_collection_meta_op
→ create directory + config → Collection::new → ShardReplicaSet per shard
```
See: [execution-paths/collection-create.md](execution-paths/collection-create.md)

---

## On-Disk Structure

```
storage/
├── collections/
│   └── <collection_name>/
│       ├── collection_config.json
│       ├── payload_index_schema.json
│       ├── shards/
│       │   └── <shard_id>/
│       │       ├── shard_config.json
│       │       ├── wal/               ← WAL segments
│       │       ├── segments/
│       │       │   └── <uuid>/        ← one Segment
│       │       │       ├── segment.json
│       │       │       ├── version.info
│       │       │       ├── vector_storage[-name]/
│       │       │       ├── vector_index[-name]/
│       │       │       ├── payload_storage/
│       │       │       └── payload_index/
│       │       └── clock_maps/
│       └── snapshots/
├── aliases/
├── raft/                              ← consensus state (cluster mode)
└── .qdrant-initialized
```

---

## Key Design Decisions

1. **Segments as immutable-after-optimization units**: New data goes into appendable segments. Background optimizers merge, index, and compact segments into optimized immutable ones. During optimization, ProxySegments allow concurrent reads.

2. **WAL-first updates**: All point mutations are written to the WAL before being applied to segments. The flush worker periodically persists segment state and acknowledges WAL entries, allowing truncation.

3. **Separation of consensus and data replication**: Raft handles only metadata. Point-level replication uses direct fan-out with logical clocks for ordering, avoiding Raft bottlenecks on the hot data path.

4. **Pluggable storage backends**: Vector storage, payload storage, and indexes each have multiple implementations (mmap, RAM, RocksDB) selected by configuration. This allows tuning for memory vs disk usage.

5. **Trait-based abstraction boundaries**: `SegmentEntry`, `VectorStorage`, `VectorIndex`, `PayloadStorage`, `IdTracker`, `PayloadIndex` — each has a trait that abstracts over implementations, enabling testing and future backends.

---

## Cross-References

Full navigation index: [Navigation.md](Navigation.md)

| Layer | Primary Doc |
|-------|------------|
| API | [components/api-layer.md](components/api-layer.md) |
| Storage/ToC | [components/storage.md](components/storage.md) |
| Collection | [components/collection.md](components/collection.md) |
| Shard | [components/shard.md](components/shard.md) |
| Segment | [components/segment.md](components/segment.md) |
| Consensus | [components/consensus.md](components/consensus.md) |
| Replication | [components/replication.md](components/replication.md) |
