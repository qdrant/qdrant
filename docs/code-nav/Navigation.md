# Qdrant Code Navigation Index

Entry point for developers and AI agents exploring the Qdrant codebase. Each link leads to a focused document (under 500 lines) covering one component, execution path, or utility.

Start with [Architecture.md](Architecture.md) for the big picture.

---

## Components

Core architectural components of Qdrant, from highest to lowest level.

| Document | What it covers |
|----------|---------------|
| [components/storage.md](components/storage.md) | **TableOfContent** (central collection registry), **Dispatcher** (routes meta-ops through consensus or direct), **ConsensusManager**, content manager structure. Crate: `lib/storage/`. |
| [components/collection.md](components/collection.md) | **Collection** struct, shard architecture (**ShardReplicaSet**, **Shard** enum variants), update pipeline (WAL → workers → segments), shard transfer types. Crate: `lib/collection/`. |
| [components/shard.md](components/shard.md) | **SegmentHolder** (manages multiple segments), **LockedSegment**, **ProxySegment**, update operations (**CollectionUpdateOperations**), **SerdeWal**, optimizers trait, search/retrieval over segments. Crate: `lib/shard/`. |
| [components/segment.md](components/segment.md) | **Segment** struct and fields, trait hierarchy (**SegmentEntry** ladder), segment lifecycle (create, load, merge), on-disk layout (`segment.json`, vector/payload directories). Crate: `lib/segment/`. |
| [components/vector-storage.md](components/vector-storage.md) | **VectorStorage** trait and **VectorStorageEnum** (mmap, volatile, appendable mmap, RocksDB, io_uring). Dense, multi-dense, sparse backends. Scoring infrastructure (raw_scorer, query_scorer). Path: `lib/segment/src/vector_storage/`. |
| [components/payload-storage.md](components/payload-storage.md) | **PayloadStorage** trait and backends (mmap via gridstore, RocksDB, in-memory). Condition checking and query checking for filters. Path: `lib/segment/src/payload_storage/`. |
| [components/id-tracker.md](components/id-tracker.md) | **IdTracker** trait: external↔internal ID mapping, per-point versions, soft deletes. Implementations: mutable, immutable, compressed, RocksDB (deprecated). Path: `lib/segment/src/id_tracker/`. |
| [components/indexes.md](components/indexes.md) | **VectorIndex** trait. **HNSW** (graph layers, build, GPU support), **Plain** (brute-force), **Sparse** (inverted index variants). Index selection logic. Paths: `lib/segment/src/index/`, `lib/sparse/src/index/`. |
| [components/payload-indexes.md](components/payload-indexes.md) | **StructPayloadIndex** (per-field index orchestrator), **FieldIndex** types (keyword, numeric, geo, full-text, bool, UUID, datetime, null). Query optimization and filter planning. Path: `lib/segment/src/index/`. |
| [components/quantization.md](components/quantization.md) | **EncodedVectors** trait. Scalar (U8), product (PQ), and binary quantization. Segment integration via QuantizedVectors. Crate: `lib/quantization/`. |
| [components/segment-optimizer.md](components/segment-optimizer.md) | **SegmentOptimizer** trait. Merge, vacuum, indexing, and config-mismatch optimizers. Optimization process (proxy → build → swap). Resource budget. Path: `lib/shard/src/optimizers/`. |
| [components/consensus.md](components/consensus.md) | Raft consensus integration. **ConsensusManager**, consensus thread, proposal flow, persistent state, consensus WAL. Standalone vs cluster differences. Paths: `src/consensus.rs`, `lib/storage/src/content_manager/consensus/`. |
| [components/replication.md](components/replication.md) | **ShardReplicaSet** replication: update fan-out, clock tags, read routing, replica states. Shard transfer methods (stream, snapshot, WAL delta). Recovery via clock maps. Path: `lib/collection/src/shards/replica_set/`. |
| [components/api-layer.md](components/api-layer.md) | REST (Actix) and gRPC (Tonic) servers. **API crate** (shared types, conversions). Request flow from handler to Dispatcher. Paths: `src/actix/`, `src/tonic/`, `lib/api/`. |
| [components/rbac-and-auth.md](components/rbac-and-auth.md) | **Access** model (global vs per-collection), **Auth** struct, audit logging. How RBAC checks integrate with point and meta operations. Path: `lib/storage/src/rbac/`. |

---

## Execution Paths

End-to-end traces of key operations through the codebase, with specific file paths and function names.

| Document | What it traces |
|----------|---------------|
| [execution-paths/upsert.md](execution-paths/upsert.md) | **Point upsert** from REST/gRPC through auth, collection routing, shard replication, WAL write, update worker, segment apply, to flush. Covers wait semantics (Wal/Segment/Visible). |
| [execution-paths/search.md](execution-paths/search.md) | **Search/query** from API through shard routing, SegmentsSearcher (parallel segment search), per-segment filter + HNSW/brute-force, result aggregation, score fusion, and post-processing. |
| [execution-paths/collection-create.md](execution-paths/collection-create.md) | **Collection creation** via Dispatcher → consensus (cluster) or direct (standalone) → ToC → `Collection::new` → shard distribution and ShardReplicaSet setup. |
| [execution-paths/snapshot.md](execution-paths/snapshot.md) | **Snapshot** creation (collection → per-shard → segment tar archives) and recovery (download, extract, rebuild). Snapshot contents. |
| [execution-paths/optimization.md](execution-paths/optimization.md) | **Segment optimization** cycle: trigger (after updates), planning (optimizer selection), execution (proxy wrap → SegmentBuilder → swap), resource budget. |
| [execution-paths/startup.md](execution-paths/startup.md) | **Server startup** sequence: config, persistent state, runtimes, ToC load, consensus setup (cluster), REST/gRPC server launch. |

---

## Utilities

Supporting crates and modules that aren't core components but are important for the system.

| Document | What it covers |
|----------|---------------|
| [utilities/wal.md](utilities/wal.md) | **WAL crate** (`lib/wal/`): directory-based append-only log, segment format (CRC-chained entries), **SerdeWal** typed wrapper, crash recovery. |
| [utilities/gridstore.md](utilities/gridstore.md) | **Gridstore** (`lib/gridstore/`): mmap-backed id→blob storage using pages + tracker + bitmask. Used for payload storage and field indexes. |
| [utilities/sparse-index.md](utilities/sparse-index.md) | **Sparse crate** (`lib/sparse/`): inverted indexes for sparse vectors. InvertedIndex trait, RAM/mmap/compressed implementations, search context. |
| [utilities/posting-list.md](utilities/posting-list.md) | **Posting list** (`lib/posting_list/`): generic compressed posting lists (BitPacker4x, 128-element chunks). Used by full-text index. |
| [utilities/gpu.md](utilities/gpu.md) | **GPU crate** (`lib/gpu/`): Vulkan helpers for GPU-accelerated HNSW build. Feature-gated behind `gpu`. |
| [utilities/edge.md](utilities/edge.md) | **Edge crate** (`lib/edge/`): embedded single-shard runtime for edge/Python deployments. Same segment/WAL stack without clustering. |
| [utilities/common-utils.md](utilities/common-utils.md) | **Shared utilities**: atomic file writes, mmap helpers, memory tracking (cgroup-aware), error types (OperationError), Flusher pattern, flags/bitsets, score fusion. Paths: `lib/common/`, `lib/segment/src/common/`, `lib/segment/src/utils/`. |

---

## Crate Map

Quick reference for all library crates in the workspace.

| Crate | Path | Purpose |
|-------|------|---------|
| `qdrant` (binary) | `src/` | Main server binary: REST/gRPC servers, consensus thread, startup |
| `storage` | `lib/storage/` | Service layer: ToC, Dispatcher, consensus integration, RBAC |
| `collection` | `lib/collection/` | Distributed collections, shards, replication, transfers |
| `shard` | `lib/shard/` | Local shard engine: segments, WAL, optimizers, updates |
| `segment` | `lib/segment/` | Storage unit: vectors, payloads, indexes, ID tracker |
| `api` | `lib/api/` | REST/gRPC type definitions and conversions |
| `wal` | `lib/wal/` | Write-ahead log implementation |
| `gridstore` | `lib/gridstore/` | Mmap-backed id→blob storage |
| `quantization` | `lib/quantization/` | Vector compression (scalar, PQ, binary) |
| `sparse` | `lib/sparse/` | Sparse vector inverted indexes |
| `posting_list` | `lib/posting_list/` | Compressed posting lists |
| `gpu` | `lib/gpu/` | Vulkan GPU helpers (feature-gated) |
| `edge` | `lib/edge/` | Embedded single-shard runtime |
| `macros` | `lib/macros/` | Derive macros (`Anonymize`) |
| `trififo` | `lib/trififo/` | SeqLock synchronization primitive |

---

## How to Use This Documentation

**New developer**: Start with [Architecture.md](Architecture.md) for the big picture, then follow links to specific components you'll be working on.

**AI agent**: Use this file as the index. For any operation, find the relevant execution path document for the end-to-end flow with specific file paths and function names. For understanding a subsystem, read the corresponding component document.

**Finding code**: Each document references specific file paths (`lib/segment/src/segment/mod.rs`), type names (`Segment`, `VectorStorageEnum`), and function names (`upsert_point`, `search_batch`). Use these as grep targets.

**Keeping docs up to date**: Each file is self-contained and under 500 lines. When code changes, update only the affected file. The Navigation.md links and Architecture.md diagrams should rarely need changes unless new major components are added.
