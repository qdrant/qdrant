# Shard crate (`shard`)

Code navigation for the **shard** Rust crate at `lib/shard/`: the local storage engine for a single shard.

---

## Overview

The `shard` crate is the local storage engine for **one shard**. It manages:

- **Multiple segments** grouped in a `SegmentHolder` (appendable vs non-appendable, each wrapped as a `LockedSegment`).
- A **typed write-ahead log** (`SerdeWal`) over the lower-level `wal` crate.
- **Optimizers** that rebuild or merge segments (`SegmentOptimizer` implementations plus `optimize::execute_optimization`).
- **Update operations** (`operations::CollectionUpdateOperations`) and their application across segments (`update.rs`).

Higher layers (for example `lib/collection/`) own replication, routing, and collection-wide policy; this crate focuses on segment lifecycle, WAL records, and per-shard read/update paths.

---

## Crate location

| Path | Role |
|------|------|
| `qdrant/lib/shard/` | Crate root (`Cargo.toml`, `src/`) |
| `qdrant/lib/shard/src/lib.rs` | Public module surface and `pub type PeerId = u64` |

---

## Module map

Top-level modules declared in `lib.rs`:

| Module | Path | One-line description |
|--------|------|---------------------|
| `common` | `src/common/` | Shard-local helpers (e.g. `stopping_guard`). |
| `count` | `src/count.rs` | `CountRequestInternal` (filter, exact vs approximate). |
| `facet` | `src/facet.rs` | `FacetRequestInternal` (payload key, limit, filter, exact). |
| `files` | `src/files/` | `ShardDataFiles` and path helpers (`wal`, `segments`, clock JSON, `applied_seq`). |
| `locked_segment` | `src/locked_segment.rs` | `LockedSegment` enum (`Original` / `Proxy`) over `Arc<RwLock<…>>`. |
| `operation_rate_cost` | `src/operation_rate_cost.rs` | Read cost helpers (e.g. filter condition count). |
| `operations` | `src/operations/` | `CollectionUpdateOperations`, point/vector/payload ops, field indexes, optimization thresholds. |
| `optimize` | `src/optimize.rs` | Generic optimization execution (`OptimizationStrategy`, proxy finalization, disk budget). |
| `optimizers` | `src/optimizers/` | `SegmentOptimizer` trait and concrete optimizers (merge, vacuum, indexing, config mismatch). |
| `payload_index_schema` | `src/payload_index_schema.rs` | Serializable map of payload field → schema. |
| `proxy_segment` | `src/proxy_segment/` | `ProxySegment` and `SegmentEntry` / snapshot glue for optimized segments. |
| `query` | `src/query/` | Internal query types (`QueryEnum`, query context, planned queries, MMR, etc.). |
| `retrieve` | `src/retrieve/` | `retrieve_blocking`, `RecordInternal` for multi-segment point fetch. |
| `scroll` | `src/scroll.rs` | `ScrollRequestInternal` (pagination, filter, payload/vector, `order_by`). |
| `search` | `src/search.rs` | `CoreSearchRequest`, `CoreSearchRequestBatch` (query + filter + limits; API conversions behind `api` feature). |
| `search_result_aggregator` | `src/search_result_aggregator.rs` | Merge top-k scored points across sources; batch + version tracking. |
| `segment_holder` | `src/segment_holder/` | `SegmentHolder`, flush/snapshot/read ordering, appendable routing. |
| `snapshots` | `src/snapshots/` | Snapshot data, manifest, utilities. |
| `tracker` | `src/tracker.rs` | `TrackerLog` / optimizer progress descriptions. |
| `update` | `src/update.rs` | Apply point/vector/payload/field-index ops to `SegmentHolder`. |
| `wal` | `src/wal.rs` | `SerdeWal<R>`, `WalRawRecord`, ack/truncate, retention tuning. |
| `fixtures` | `src/fixtures/` | Test fixtures (`testing` feature). |

Submodules worth noting: `segment_holder` exposes `locked`, `read_points`, internal `flush`, `snapshot`, `tests`.

---

## SegmentHolder

Defined in `segment_holder/mod.rs`. It is the in-memory registry of all segments for the shard.

### What it manages

- **`appendable_segments` / `non_appendable_segments`**: two `BTreeMap<SegmentId, LockedSegment>` instances (sorted IDs). New segments get IDs from `id_source: AtomicUsize` via `generate_new_key`.
- **`failed_operation: BTreeSet<SeqNumberType>`**: seq numbers of operations that could not be fully recovered; used so replay can resume correctly.
- **`optimizer_errors`**: optional string for the first uncorrected optimizer error.
- **`max_persisted_segment_version_overwrite: AtomicU64`**: allows bumping the persisted version for ops that do not touch points but must still be acknowledged in the WAL (see `flush_all` integration).
- **`flush_dependency: Arc<Mutex<TopoSort<SegmentId, SeqNumberType>>>`**: directed dependencies between segments for flush ordering (e.g. copy-on-write from immutable to appendable).
- **`flush_thread`**: mutex holding an optional `JoinHandle` so only one sequential “flush all” runs at a time.
- **`running_optimizations: ProcessCounter`**: count of optimizations in flight.

### Reads vs writes

- **Iteration for generic inspection**: `iter()` walks **appendable first**, then non-appendable (`chain` of the two maps).
- **Reads where point may move from frozen to appendable**: `non_appendable_then_appendable_segments()` (and `read_points` in `read_points.rs`) iterate **non-appendable first, then appendable**, so a concurrent move still yields a consistent view when segments are read-locked in that order.
- **Writes / upserts**: `update.rs` and `SegmentHolder` helpers route new data to **appendable** segments (e.g. `random_appendable_segment`, `smallest_appendable_segment`, `apply_points_to_appendable`-style paths). Points in non-appendable segments are updated in place when possible, or **moved** to an appendable segment with flush dependencies recorded between segment IDs.

`LockedSegmentHolder` (`segment_holder/locked.rs`) wraps `Arc<RwLock<SegmentHolder>>` plus an **`updates_mutex`**: `acquire_updates_lock` returns `UpdatesGuard`, blocking concurrent **updates** during critical sections (e.g. optimization finalization) while still allowing read locks on the holder where the design permits.

---

## LockedSegment

Defined in `locked_segment.rs`.

### Enum

- **`LockedSegment::Original(Arc<RwLock<Segment>>)`** — normal segment.
- **`LockedSegment::Proxy(Arc<RwLock<ProxySegment>>)`** — segment under optimization: frozen wrapped segment plus side effects elsewhere.

### Access

- **`get()`** → `&RwLock<dyn SegmentEntry>` (read/write entry API).
- **`get_read()`** → `&RwLock<dyn ReadSegmentEntry>` (read-oriented trait object).
- **`From<Segment>`** / **`From<ProxySegment>`** construct the corresponding variant with `Arc::new(RwLock::new(...))`.
- **`drop_data`**: tries `Arc::try_unwrap` with a long timeout, then drops underlying segment data; errors if still shared.

`SegmentHolder::iter_original` skips proxy segments and yields only `Original` `Arc<RwLock<Segment>>` references.

---

## ProxySegment

Defined in `proxy_segment/mod.rs`; implements segment traits in `proxy_segment/segment_entry.rs` (and related snapshot types).

### Role

- Wraps a **read-oriented** (“frozen”) segment while optimization runs: **`wrapped_segment: LockedSegment`**.
- Directs **mutations** into a separate **write segment** (not stored inside the `ProxySegment` struct itself; the holder/optimizer wires the temp appendable segment).
- Tracks:
  - **`deleted_points`** (`DeletedPoints` / `AHashMap`) — points hidden from the wrapped segment (may be shared across proxies).
  - **`deleted_mask`** — optional `BitVec` for fast delete checks on plain wrapped segments.
  - **`changed_indexes`** — payload index changes applied on top of the wrapped segment.
  - **`wrapped_config` / `version`** — config and logical version for the proxy.

### Reads

Read paths combine the wrapped segment’s data with proxy overlays (deletes, index changes, writes visible through the entry implementation) so clients see a coherent view while the wrapped segment is not written in place.

### Deferred points

The wrapped appendable segment can contain **deferred** points (visibility controlled by `DeferredBehavior` and segment configuration). The proxy’s `SegmentEntry` implementation adjusts counts and lookups (e.g. `available_point_count_without_deferred`, `point_is_deferred`, `deleted_deferred_count` when deletes target deferred IDs) so search/retrieve semantics stay consistent with the underlying segment’s deferred model.

---

## Update operations

### `CollectionUpdateOperations`

In `operations/mod.rs`, serde untagged enum:

| Variant | Payload module | Role |
|---------|----------------|------|
| `PointOperation` | `point_ops` | Upsert (list / conditional), delete by IDs or filter, sync points. |
| `VectorOperation` | `vector_ops` | Update vectors (optional filter), delete vectors by IDs or filter. |
| `PayloadOperation` | `payload_ops` | Set / overwrite / delete / clear payload (by points or filter). |
| `FieldIndexOperation` | `FieldIndexOperations` | `CreateIndex` / `DeleteIndex` on payload fields. |
| `StagingOperation` | `staging` (feature `staging`) | Test/debug staging hooks. |

`OperationWithClockTag` wraps an operation with optional `ClockTag` (peer clock metadata for distributed ordering). Helper methods on `CollectionUpdateOperations` include `point_ids`, `upsert_point_ids`, `retain_point_ids`, and discriminant checks like `is_upsert_points`.

### `update.rs` processors

- **`process_point_operation`**: dispatches to `upsert_points`, `conditional_upsert`, `delete_points`, `delete_points_by_filter`, `sync_points`, etc.
- **`process_vector_operation`**: `update_vectors_conditional`, `delete_vectors`, `delete_vectors_by_filter`.
- **`process_payload_operation`**: set/overwrite/delete/clear payload, by point list or filter where applicable.
- **`process_field_index_operation`**: `create_field_index` or `delete_field_index` across segments.

Updates work on **`&SegmentHolder`**, use **`HardwareCounterCell`** for metrics, and chunk large upserts (`UPDATE_OP_CHUNK_SIZE` = 32) to avoid starving concurrent readers.

---

## Optimizers

### `SegmentOptimizer` trait

In `optimizers/segment_optimizer.rs`. Common contract for all optimizers:

- Paths: `segments_path`, `temp_path`; target config via `segment_optimizer_config`, `hnsw_global_config`, `threshold_config`.
- **`plan_optimizations(&self, planner: &mut OptimizationPlanner)`** — enqueue candidate segment sets.
- **`optimized_segment_builder`**, **`temp_segment`**, plus hooks integrated with **`crate::optimize::execute_optimization`** through **`OptimizationStrategy`** (`ShardOptimizationStrategy` implements `create_segment_builder` / `create_temp_segment` by delegating to the optimizer).

Concrete optimizers are **`Sync`** objects used as `Optimizer = dyn SegmentOptimizer + Sync + Send`.

### Four optimizer types

| Type | File | Purpose |
|------|------|---------|
| **Merge** | `merge_optimizer.rs` | Reduce segment count by batching mergeable segments up to size thresholds (documented greedy batching in module docs). |
| **Vacuum** | `vacuum_optimizer.rs` | Rebuild segments with high soft-delete “litter” to reclaim space and refresh indexes. |
| **Indexing** | `indexing_optimizer.rs` | Build indexes for large segments that are still unindexed / below mmap thresholds (CPU-heavy work same pipeline as rebuild). |
| **Config mismatch** | `config_mismatch_optimizer.rs` | Detect segments whose on-disk indexes or storage (e.g. HNSW params, on-disk flags) disagree with current collection config; rebuild to align. |

Shared configuration lives in `optimizers/config.rs`; thresholds in `operations::optimization::OptimizerThresholds`.

---

## SerdeWal

In `wal.rs`.

- **`SerdeWal<R>`** wraps **`wal::Wal`** with **`WalOptions`** and a logical **`first_index`** (optional), persisted in **`first-index`** JSON (`WalState { ack_index }`) so acknowledged prefix truncation aligns with Qdrant’s logical WAL.
- **`WalRawRecord<R>`** holds serialized bytes; **`new`** uses **serde_cbor**; **`deserialize`** tries CBOR then falls back to **rmp_serde** for older records.
- **Read API**: `read`, `read_range`, `read_all` (with/without acknowledged entries); `len` subtracts logically truncated prefix via `truncated_prefix_entries_num`.
- **`ack`**: `prefix_truncate` on the underlying WAL, then advances/persists `first_index` monotonically.
- **Retention for transfers**: **`set_extended_retention`** multiplies closed-segment retention by **`INCREASED_RETENTION_FACTOR`** (10); **`set_normal_retention`** restores configured retention. Extends recoverable history for shard transfer scenarios.
- **`drop_from`**: truncate from a given index (debug-asserts index ≥ logical `first_index`).

---

## Search and retrieval

### `search.rs`

Defines **`CoreSearchRequest`** (deprecated comment: prefer `ShardQueryRequest` path for new universal query work) with **`QueryEnum`**, filter, `SearchParams`, limit/offset, payload/vector options, score threshold. **`CoreSearchRequestBatch`** batches multiple requests. With the **`api`** feature, conversions exist from REST/gRPC types. **`search_rate_cost`** combines query cost with filter condition count.

**Multi-segment search execution** is not fully implemented inside `search.rs`; the **`collection`** crate’s **`SegmentsSearcher`** (`lib/collection/src/collection_manager/segments_searcher.rs`) runs searches per segment in parallel, uses **`shard::query`** for query context, and merges with **`shard::search_result_aggregator::BatchResultAggregator`**.

### `scroll.rs`, `count.rs`, `facet.rs`

These modules define **request structs** (`ScrollRequestInternal`, `CountRequestInternal`, `FacetRequestInternal`) used by API and collection layers. They do not themselves iterate `SegmentHolder`; execution happens in **`segment`** / **`collection`** code paths that receive these types.

### `retrieve/`

- **`retrieve_blocking.rs`**: **`retrieve_blocking`** takes **`LockedSegmentHolder`**, resolves latest point versions, calls **`SegmentHolder::read_points_locked`** (non-appendable-then-appendable order), then **`ReadSegmentEntry::retrieve`** with **`DeferredBehavior`** and hardware counters. Builds **`AHashMap<PointIdType, RecordInternal>`**.
- **`record_internal.rs`**: Internal record representation for retrieved points.

---

## Cross-references

| Related area | Where to look |
|--------------|----------------|
| Single-segment storage and indexes | [`segment` crate](./segment.md) — `Segment`, `SegmentEntry`, vector/payload indexes. |
| Collection-wide shards, consensus, remote search | `lib/collection/` — `local_shard`, `segments_searcher`, `shard_trait`. |
| Raw WAL segments on disk | `wal` crate (dependency of `shard`). |
| Distributed operation ordering | `operations::ClockTag`, `PeerId` in `lib.rs`. |
| Optimization telemetry | `tracker::TrackerLog`, `OperationDurationsAggregator` in optimizer structs. |
| Shard directory layout | `files::ShardDataFiles`, `WAL_PATH`, `SEGMENTS_PATH`. |
