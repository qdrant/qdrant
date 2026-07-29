# Vector indexes (`lib/segment`, `lib/sparse`)

Code navigation for **vector indexes** used during similarity search: dense **HNSW**, dense **plain (full scan)**, and **sparse inverted** indexes. Dense index orchestration lives under [`lib/segment/src/index/`](../../../lib/segment/src/index/); posting-list primitives for sparse vectors live in [`lib/sparse/src/index/`](../../../lib/sparse/src/index/).

---

## Overview

Vector indexes **accelerate similarity search** over internal point offsets. Qdrant uses:

- **HNSW** (Hierarchical Navigable Small World) for **dense** vectors when the segment is configured with `Indexes::Hnsw` — approximate graph search with tunable recall/latency.
- **Inverted indexes** for **sparse** vectors (`SparseVectorIndex` over implementations of `InvertedIndex` from the `sparse` crate).
- A **plain** index (`PlainVectorIndex`) that performs **brute-force** scoring over available vectors — used when the collection/segment is configured with `Indexes::Plain`, and also as the execution path when estimated scan cost is below planner thresholds.

---

## Location

| Path | Role |
|------|------|
| [`lib/segment/src/index/mod.rs`](../../../lib/segment/src/index/mod.rs) | Re-exports index submodules; `pub use vector_index_base::*` |
| [`lib/segment/src/index/vector_index_base.rs`](../../../lib/segment/src/index/vector_index_base.rs) | `VectorIndex` trait, `VectorIndexEnum` |
| [`lib/segment/src/index/hnsw_index/`](../../../lib/segment/src/index/hnsw_index/) | HNSW graph, build, search, GPU hooks |
| [`lib/segment/src/index/plain_vector_index.rs`](../../../lib/segment/src/index/plain_vector_index.rs) | Full-scan dense search |
| [`lib/segment/src/index/sparse_index/`](../../../lib/segment/src/index/sparse_index/) | `SparseVectorIndex`, sparse config |
| [`lib/sparse/src/index/mod.rs`](../../../lib/sparse/src/index/mod.rs) | Sparse index modules: `inverted_index`, `posting_list`, `search_context`, … |
| [`lib/segment/src/segment_constructor/segment_constructor_base.rs`](../../../lib/segment/src/segment_constructor/segment_constructor_base.rs) | `open_vector_index`, `build_vector_index`, `create_sparse_vector_index` |
| [`lib/segment/src/types.rs`](../../../lib/segment/src/types.rs) | `Indexes`, `HnswConfig`, `HnswGlobalConfig` |

---

## VectorIndex trait

Defined in [`vector_index_base.rs`](../../../lib/segment/src/index/vector_index_base.rs).

**Note:** **Construction / building** of an index is **not** part of this trait. Dense HNSW is built via `HNSWIndex::build` (see below); segments wire indices through [`open_vector_index`](../../../lib/segment/src/segment_constructor/segment_constructor_base.rs) / [`build_vector_index`](../../../lib/segment/src/segment_constructor/segment_constructor_base.rs).

| Method | Purpose |
|--------|---------|
| `search` | Batch query: `&[&QueryVector]`, optional `Filter`, `top`, optional `SearchParams`, `VectorQueryContext` → `Vec<Vec<ScoredPointOffset>>` |
| `get_telemetry_data` | Search-time telemetry (`TelemetryDetail` → `VectorIndexSearchesTelemetry`) |
| `files` | On-disk paths owned by the index |
| `immutable_files` | Default empty; mmap / immutable assets |
| `indexed_vector_count` | Count of vectors currently represented in the index |
| `size_of_searchable_vectors_in_bytes` | Total size of searchable vector data (bytes) |
| `update_vector` | Incremental update for one internal id: insert/update vector or remove from index when `vector` is `None` (see doc comment on trait for semantics vs storage deletion) |

---

## VectorIndexEnum

Also in [`vector_index_base.rs`](../../../lib/segment/src/index/vector_index_base.rs). This is the **segment-facing** wrapper; `impl VectorIndex for VectorIndexEnum` dispatches every trait method to the active variant.

| Variant | Inner type |
|---------|------------|
| `Plain` | `PlainVectorIndex` |
| `Hnsw` | `HNSWIndex` |
| `SparseRam` | `SparseVectorIndex<InvertedIndexRam>` |
| `SparseImmutableRam` | `SparseVectorIndex<InvertedIndexImmutableRam>` |
| `SparseMmap` | `SparseVectorIndex<InvertedIndexMmap>` |
| `SparseCompressedImmutableRamF32` / `F16` / `U8` | `SparseVectorIndex<InvertedIndexCompressedImmutableRam<…>>` |
| `SparseCompressedMmapF32` / `F16` / `U8` | `SparseVectorIndex<InvertedIndexCompressedMmap<…>>` |

Helpers on the enum include `is_index()` (plain → `false`; all others → `true`), `is_on_disk()`, `populate()`, `clear_cache()`, `fill_idf_statistics` (sparse only), `indexed_vectors()`.

---

## HNSW index

Primary type: [`HNSWIndex`](../../../lib/segment/src/index/hnsw_index/hnsw.rs) in `hnsw.rs`. Holds `GraphLayers`, `HnswGraphConfig`, shared `IdTracker` / `VectorStorage` / optional `QuantizedVectors` / `StructPayloadIndex`, paths, telemetry, on-disk flag.

### Architecture

- **Graph layers:** [`graph_layers.rs`](../../../lib/segment/src/index/hnsw_index/graph_layers.rs) implements layered link structure and **search-on-level** variants (module docs list `search_on_level`, ACORN variant `search_on_level_acorn`, entry search `search_entry_on_level`, and **inline vector** variants when `HnswConfig::inline_storage` is used). Constants include `HNSW_GRAPH_FILE`, `HNSW_LINKS_FILE`, compressed links filename.
- **Entry points:** [`entry_points.rs`](../../../lib/segment/src/index/hnsw_index/entry_points.rs) — `EntryPoint` (id + level), `EntryPoints` with primary list and `extra_entry_points` queue; `new_point` integrates a filter predicate so deleted/unavailable points are not used as checkpoints.
- **Level generation:** [`GraphLayersBuilder`](../../../lib/segment/src/index/hnsw_index/graph_layers_builder.rs) assigns per-point levels (random layer sampling; reuse from migrated old graph when present). [`HnswM`](../../../lib/segment/src/index/hnsw_index/mod.rs) encodes `m` / `m0` and `level_m`.
- **Search context:** [`search_context.rs`](../../../lib/segment/src/index/hnsw_index/search_context.rs) — `SearchContext` with `nearest` (`FixedLengthPriorityQueue`) and `candidates` (`BinaryHeap`), `ef`-sized beam, `process_candidate` / `lower_bound`.
- **Links / storage:** [`graph_links.rs`](../../../lib/segment/src/index/hnsw_index/graph_links.rs), [`point_scorer.rs`](../../../lib/segment/src/index/hnsw_index/point_scorer.rs) for filtered batch search integration with vector storage scorers.

### Key files

| File | Role |
|------|------|
| `hnsw.rs` | `HNSWIndex::open` / `build`, search, telemetry, GPU orchestration |
| `graph_layers.rs` | Persisted graph + search algorithms |
| `graph_layers_builder.rs` | Mutable graph construction |
| `entry_points.rs` | Multi-level entry point bookkeeping |
| `search_context.rs` | Greedy beam state during search |
| `graph_layers_healer.rs` | Migrate/heal graph when reusing old index |
| `build_condition_checker.rs`, `build_cache.rs` | Build-time filtering / caching |
| `config.rs` | `HnswGraphConfig` persisted as `hnsw_config.json` |

### Build process

[`HNSWIndex::build`](../../../lib/segment/src/index/hnsw_index/hnsw.rs):

1. If graph files already exist, logs and delegates to `open` (debug assertion).
2. Derives **`full_scan_threshold` in vector count** from `HnswConfig::full_scan_threshold` (KB) and average vector size in storage.
3. Writes [`HnswGraphConfig`](../../../lib/segment/src/index/hnsw_index/config.rs) (`m`, `m0 = 2*m`, `ef_construct`, `ef`, thresholds, threading, optional payload M).
4. Optionally picks an **`OldIndexCandidate`** from `build_args.old_indices` to reuse levels / heal graph (`GraphLayersHealer`).
5. Instantiates **`GraphLayersBuilder`** with `HnswM`, `ef_construct`, computed `num_entries` from `full_scan_threshold`.
6. Builds a **rayon** thread pool sized from the resource permit; on Linux may lower thread priority.
7. Assigns levels per non-deleted internal id (from old graph or random).
8. **GPU (`feature = "gpu"`):** may upload vectors and call `build_main_graph_on_gpu`; on success replaces the builder state and skips CPU main-graph build for that phase.
9. **CPU path:** first `SINGLE_THREADED_HNSW_BUILD_THRESHOLD` inserts single-threaded, then parallel insertion with `FilteredScorer` / progress / stop checks; payload-index **additional HNSW links** per indexed field when `enable_hnsw` on schema (see `additional_links_params`).
10. Finalizes graph, saves files, constructs `HNSWIndex`.

`VectorIndexBuildArgs` in [`segment_constructor_base.rs`](../../../lib/segment/src/segment_constructor/segment_constructor_base.rs) supplies `permit`, `old_indices`, optional `LockedGpuDevice`, `rng`, `stopped`, `HnswGlobalConfig`, `FeatureFlags`, `ProgressTracker`.

### Configuration

- **API / collection type:** [`HnswConfig`](../../../lib/segment/src/types.rs) — `m`, `ef_construct`, `full_scan_threshold` (KB; serde alias `full_scan_threshold_kb`), `max_indexing_threads` (0 = auto), `on_disk`, `payload_m`, `inline_storage`. Method `mismatch_requires_rebuild` documents which changes force rebuild.
- **On-disk graph snapshot:** [`HnswGraphConfig`](../../../lib/segment/src/index/hnsw_index/config.rs) — includes resolved `full_scan_threshold` as **vector count**, `m0`, optional `indexed_vector_count`, payload `m0`, file `hnsw_config.json`.
- **Global:** [`HnswGlobalConfig`](../../../lib/segment/src/types.rs) — e.g. `healing_threshold` for HNSW healing during build/migration.

Thread count helper: [`get_num_indexing_threads`](../../../lib/segment/src/index/hnsw_index/mod.rs) in `hnsw_index/mod.rs`.

---

## Plain index

[`PlainVectorIndex`](../../../lib/segment/src/index/plain_vector_index.rs) holds the same shared segment handles as HNSW (id tracker, vector storage, optional quantized vectors, payload index) but **no graph**. Search uses [`BatchFilteredSearcher`](../../../lib/segment/src/index/hnsw_index/point_scorer.rs) over full storage with optional quantization path.

**When it is used**

- Segment `VectorDataConfig.index` is [`Indexes::Plain`](../../../lib/segment/src/types.rs): constructor always wraps `PlainVectorIndex::new` in [`open_vector_index`](../../../lib/segment/src/segment_constructor/segment_constructor_base.rs) / [`build_vector_index`](../../../lib/segment/src/segment_constructor/segment_constructor_base.rs).

**Planner threshold:** `is_small_enough_for_unindexed_search` compares estimated scan size (using payload cardinality when filtered) to `search_optimized_threshold_kb` (bytes threshold derived in caller). If `SearchParams::indexed_only` is true and the segment is **not** “small enough”, plain search returns **empty** results per query vector (fast path for “index-only” semantics on unindexed segments).

---

## Sparse vector index

[`SparseVectorIndex<T: InvertedIndex>`](../../../lib/segment/src/index/sparse_index/sparse_vector_index.rs) in `sparse_index/sparse_vector_index.rs` combines:

- `SparseIndexConfig` ([`sparse_index_config.rs`](../../../lib/segment/src/index/sparse_index/sparse_index_config.rs)) — `SparseIndexType` (`MutableRam`, `ImmutableRam`, `Mmap`), optional `full_scan_threshold`, `datatype`.
- Pluggable **`T: InvertedIndex`** from [`lib/sparse/src/index/inverted_index/mod.rs`](../../../lib/sparse/src/index/inverted_index/mod.rs).

**`InvertedIndex` trait** (sparse crate): `open` / `save`, `get` posting list iterator, `len`, `posting_list_len`, `files` / `immutable_files`, `remove` / `upsert`, `from_ram_index`, `vector_count`, `is_on_disk`, associated `StorageVersion`, etc.

**Implementations** (submodules of `inverted_index/`):

- **RAM:** `InvertedIndexRam`, `InvertedIndexImmutableRam`
- **Mmap:** `InvertedIndexMmap`
- **Compressed:** `InvertedIndexCompressedImmutableRam`, `InvertedIndexCompressedMmap` (parameterized by element type: `f32`, `f16`, `QuantizedU8`)

[`create_sparse_vector_index`](../../../lib/segment/src/segment_constructor/segment_constructor_base.rs) maps `(SparseIndexType, datatype, USE_COMPRESSED)` to the correct `VectorIndexEnum` variant; non-compressed `Float16`/`Uint8` without compression is rejected with validation error.

---

## Index selection

### Segment constructor (dense)

[`open_vector_index`](../../../lib/segment/src/segment_constructor/segment_constructor_base.rs) / [`build_vector_index`](../../../lib/segment/src/segment_constructor/segment_constructor_base.rs):

- `Indexes::Plain` → `VectorIndexEnum::Plain(PlainVectorIndex::new(...))`
- `Indexes::Hnsw` → `HNSWIndex::open` or `HNSWIndex::build(...)` with `HnswIndexOpenArgs`

Sparse vectors use **`create_sparse_vector_index`** with `SparseVectorIndexOpenArgs` (path, trackers, config, progress, deferred id).

The **collection-level** choice of plain vs HNSW is stored in segment **`VectorDataConfig`** (from collection params); the segment crate applies it when building/opening the segment.

### Indexing optimizer

[`IndexingOptimizer`](../../../lib/shard/src/optimizers/indexing_optimizer.rs) (`lib/shard`, re-exported from collection tests) decides **when** to run an optimization pass that can **build** indexes or migrate storage:

- **Dense:** For each configured vector, if `index.is_indexed()` is false and **storage size ≥ `indexing_threshold_kb`** (converted to bytes), or mmap/on-disk migration is needed, or the segment has **deferred points**, optimization is required.
- **Sparse:** If storage is “big” (indexing or mmap threshold) and the sparse index is **not yet immutable** (`index_type.is_immutable()`), optimization is required.

`plan_optimizations` prioritizes **largest unindexed** segments, may batch two segments when under `max_segment_size_kb`, and can pair with small indexed segments for reindexing — see comments in the optimizer.

Actual **rebuild** of HNSW after config changes may also involve [`ConfigMismatchOptimizer`](../../../lib/collection/src/collection_manager/optimizers/config_mismatch_optimizer.rs) (collection layer) when `HnswConfig::mismatch_requires_rebuild` applies.

---

## Cross-references

| Topic | Where to read |
|------|----------------|
| Vector storage / scorers | [`docs/code-nav/components/vector-storage.md`](vector-storage.md), [`raw_scorer`](../../../lib/segment/src/vector_storage/raw_scorer.rs) |
| Segment lifecycle | [`docs/code-nav/components/segment.md`](segment.md), [`segment_constructor_base.rs`](../../../lib/segment/src/segment_constructor/segment_constructor_base.rs) |
| Payload indexes (filterable HNSW on fields) | [`field_index/`](../../../lib/segment/src/index/field_index/), `hnsw.rs` additional links build |
| Query planning / full scan vs index | [`query_estimator.rs`](../../../lib/segment/src/index/query_estimator.rs), [`query_optimization.rs`](../../../lib/segment/src/index/query_optimization.rs), HNSW `full_scan_threshold` in types + graph config |
| GPU HNSW | [`hnsw_index/gpu/`](../../../lib/segment/src/index/hnsw_index/gpu/) (behind `feature = "gpu"`), stubs in `hnsw_index/mod.rs` when disabled |
| Sparse search context | [`lib/sparse/src/index/search_context.rs`](../../../lib/sparse/src/index/search_context.rs) |
