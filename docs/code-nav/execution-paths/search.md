# Search / query execution path

## Overview

This note traces how a vector **search** (legacy REST `/points/search` and gRPC core search) and the **universal query** (REST `/points/query`) reach segment-level vector indexes and return scored points. Both paths converge on the same building blocks below the collection: **shard replica read routing**, **local shard execution**, **`SegmentsSearcher`** over locked segments, **`Segment::search_batch`** → **`VectorIndex::search`** (HNSW or plain), then **merging and post-processing** (top‑K across segments/shards, optional fusion, MMR, payload/vector fill).

The **universal query** API is the unified surface for prefetches, fusion, formula rescoring, and scroll sources; it is compiled into a **`PlannedQuery`** that still executes core vector work via **`CoreSearchRequestBatch`** and **`LocalShard::do_search`**.

## Path Summary

1. **HTTP / gRPC** — Search: `search_points` → `do_core_search_points`. Query: `query_points` → REST conversion → `TableOfContent::query_batch`.
2. **TOC** — Auth + `get_collection`; delegate to `Collection::core_search_batch` or `Collection::query_batch`.
3. **Collection (search)** — `select_shards`, concurrent `ShardReplicaSet::core_search`, `merge_from_shards`.
4. **Collection (query)** — Vector resolution for references, `do_query_batch` → `batch_query_shards_concurrently` → `ShardReplicaSet::query_batch`; merge intermediates; `intermediates_to_final_list`.
5. **Replica set** — `execute_and_resolve_read_operation`: prefer local when `local_only`, else fan-out / consistency resolution.
6. **Local shard (search)** — `core_search` → `do_search` → `SegmentsSearcher::prepare_query_context` + `SegmentsSearcher::search`.
7. **Local shard (query)** — `query_batch` → `PlannedQuery::try_from` → `do_planned_query` (runs `do_search` for batched core searches + scrolls, then `resolve_plan`).
8. **SegmentsSearcher** — Parallel `search_in_segment` per segment (blocking pool), probabilistic sampling + optional second pass, `BatchResultAggregator::into_topk`.
9. **Segment** — `search_batch` → `vector_index.search` → `process_search_result` (retrieve payload/vector, build `ScoredPoint`).
10. **HNSW** — `HNSWIndex::search` chooses plain vs graph vs ACORN-style paths; graph path uses `search_vectors_with_graph` / `search_with_graph`.
11. **Aggregation / post** — Segment batch merge, shard merge, query fusion (`rrf_scoring`, `score_fusion`), MMR, offset/limit, optional second-phase payload retrieve.

## Detailed Walkthrough

### 1. API layer

**REST search** — `qdrant/src/actix/api/search_api.rs`: handler `search_points` (`#[post("/collections/{collection_name}/points/search")]`). Flow: unwrap `SearchRequest` → `check_strict_mode` → build `ShardSelectorInternal` from `shard_key` → `get_request_hardware_counter` → `do_core_search_points` from `qdrant/src/common/query.rs` with `dispatcher.toc(&auth, &pass)`, consistency, timeout, and HW accumulator.

**REST query** — `qdrant/src/actix/api/query_api.rs`: `query_points` (`#[post("/collections/{collection_name}/points/query")]`). Flow: `convert_query_request_from_rest` (inference) → `check_strict_mode` → `TableOfContent::query_batch` with `vec![(request, shard_selection)]`. Batch endpoint `query_points_batch` builds a vector of `(CollectionQueryRequest, ShardSelectorInternal)` and calls `query_batch` similarly.

**Unified semantics** — New work should target the **query** path: `CollectionQueryRequest` / `ShardQueryRequest` compile to `PlannedQuery` and reuse **`CoreSearchRequestBatch`** for actual nearest / recommend / etc. operations inside the shard. Legacy **search** maps REST `SearchRequestInternal` into `CoreSearchRequest` via `From` in `qdrant/lib/shard/src/search.rs`.

### 2. Storage / collection routing

**Core search batch** — `qdrant/lib/storage/src/content_manager/toc/point_ops.rs`: `TableOfContent::core_search_batch` loops `auth.check_point_op(collection_name, request, "core_search_batch")`, `get_collection(&collection_pass)`, then `collection.core_search_batch(...)`.

**Query batch** — same file: `TableOfContent::query_batch` uses `auth.check_point_op(..., "query_batch")`, `get_collection`, then `collection.query_batch(requests, |name| self.get_collection_opt(name), ...)`.

**Collection entry** — `qdrant/lib/collection/src/collection/search.rs`: `Collection::core_search_batch` may split “metadata-heavy” work into a first pass without payload/vector and `fill_search_result_with_payload`; otherwise calls `do_core_search_batch`. That function uses `shards_holder.read().await`, `select_shards(shard_selection)?`, and for each target shard calls `shard.core_search(request.clone(), ...)`, optionally tagging `shard_key` on points, then `merge_from_shards`.

**Query entry** — `qdrant/lib/collection/src/collection/query.rs`: public `Collection::query_batch` resolves referenced vectors (`build_vector_resolver_queries`, `resolve_referenced_vectors_batch`) then dispatches inner batch logic. `do_query_batch` mirrors the two-phase payload optimization; `do_query_batch_impl` calls `batch_query_shards_concurrently`, then per-request `merge_intermediate_results_from_shards` and `intermediates_to_final_list`.

### 3. Shard-level search

**Replica set** — `qdrant/lib/collection/src/shards/replica_set/read_ops.rs`: `ShardReplicaSet::core_search` wraps `shard.core_search(request, &search_runtime, ...)` in `execute_and_resolve_read_operation`. `ShardReplicaSet::query_batch` does the same for `shard.query_batch`.

**Read resolution** — `qdrant/lib/collection/src/shards/replica_set/execute_read_operation.rs`: `execute_and_resolve_read_operation` documents the policy: if `local_only` (e.g. shard id in selector), `execute_local_read_operation`; else combine local + remote replicas per `ReadConsistency` (`All`, `Majority`, `Quorum`, `Factor`) and resolve. `execute_read_operation` (without Resolve) pops a single response when not local-only.

**Trait** — `qdrant/lib/collection/src/shards/shard_trait.rs`: `ShardOperation::core_search` / `query_batch` are implemented by `LocalShard`, `RemoteShard`, proxy shards, etc.

**Local shard: core search** — `qdrant/lib/collection/src/shards/local_shard/shard_ops.rs`: `LocalShard::core_search` applies read rate limiting (`check_read_rate_limiter`), default search timeout, then `do_search`.

**Local shard: query** — same file: `query_batch` builds `PlannedQuery::try_from(requests)`, rate-limits, then `do_planned_query` in `qdrant/lib/collection/src/shards/local_shard/query.rs`.

**Planned query** — `LocalShard::do_planned_query`: concurrently `do_search(CoreSearchRequestBatch { searches: request.searches })` and `query_scroll_batch` for scroll prefetches; then `resolve_plan` for each `RootPlan` to assemble `ShardQueryResponse` (merges, rescoring, formula stages may call `do_search` again — see `rescoring_core_search_request` in that file).

### 4. Segments searcher

**Types** — `CoreSearchRequest` / batch types are defined in `qdrant/lib/shard/src/search.rs` (`CoreSearchRequest`, `CoreSearchRequestBatch`).

**Local shard orchestration** — `qdrant/lib/collection/src/shards/local_shard/search.rs`: `LocalShard::do_search` optionally chunks large batches (`CHUNK_SIZE`) for parallelism; `do_search_impl` calls `SegmentsSearcher::prepare_query_context` then `SegmentsSearcher::search` with `sampling_enabled: true`, applies distance `postprocess_score` for `QueryEnum::Nearest`.

**SegmentsSearcher** — `qdrant/lib/collection/src/collection_manager/segments_searcher.rs`:

- `prepare_query_context` — `init_query_context` / `fill_query_context` (blocking task) over `LockedSegmentHolder`.
- `search` — collects `non_appendable_then_appendable_segments`, optionally enables probabilistic sampling (`find_search_sampling_over_point_distribution` / `sampling_limit` in `execute_batch_search`), spawns one `spawn_blocking` task per segment running `search_in_segment`, awaits `execute_searches` (`FuturesUnordered`), `process_search_result_step1` + `BatchResultAggregator`, optionally re-runs unsampled searches for under-filled batches, returns `result_aggregator.into_topk()`.
- `search_in_segment` — batches consecutive requests with identical `BatchSearchParams` (vector name, filter, payload flags, top, HNSW params); calls `execute_batch_search`.
- `execute_batch_search` — `read_segment.search_batch(...)` on `ReadSegmentEntry` with computed `top` (sampled or full).

### 5. Segment search

**Entry point** — `qdrant/lib/segment/src/segment/entry.rs` (`impl ReadSegmentEntry for Segment`): `search_batch` validates query vectors, looks up `vector_data[vector_name]`, calls `vector_data.vector_index.borrow().search(query_vectors, filter, top, params, &vector_query_context)`, then maps each internal hit list through `Segment::process_search_result`.

**Post-process** — `qdrant/lib/segment/src/segment/search.rs`: `process_search_result` maps internal offsets to external ids via `id_tracker`, `retrieve`s payload/vectors, builds `ScoredPoint` (version, score, etc.). The test-oriented `search` forwards to `search_batch`.

**Index abstraction** — `VectorIndex::search` (trait in `qdrant/lib/segment/src/index/mod.rs` usage from segment) returns `Vec<Vec<ScoredPointOffset>>` per query vector in the batch.

### 6. HNSW search

**Implementation** — `qdrant/lib/segment/src/index/hnsw_index/hnsw.rs`, `impl VectorIndex for HNSWIndex`:

- **`fn search`** (~line 1405) — Early exit if `top == 0`. Detects disabled graph (`m == 0` and no `payload_m`). Handles `exact` by adjusting quantization in params. **Unfiltered**: chooses plain vs graph from `exact`, disabled HNSW, or `available_vector_count() < full_scan_threshold`; plain path uses `search_plain_unfiltered_batched`, graph path `search_vectors_with_graph`. **Filtered**: exact or disabled → `search_vectors_plain`; else may use graph with filter / ACORN heuristics (see `search_with_graph` and `SearchAlgorithm::Acorn`).
- **`search_vectors_with_graph`** — Per vector, dispatches `discover_search_with_graph` for discover queries or **`search_with_graph`** for others.
- **`search_with_graph`** — Computes `ef` from `SearchParams` or config, optional ACORN when filter selectivity is low enough, optional “graph with quantized link vectors” fast path, else **`regular_search()`** (full graph traversal with scoring against `vector_storage` / payload filter).

Plain / brute-force batched paths: `search_plain_iterator_batched`, `search_plain_batched`, `search_plain_unfiltered_batched`, `search_vectors_plain`.

### 7. Result aggregation

**Across segments (same shard)** — `BatchResultAggregator` (`qdrant/lib/shard/src/search_result_aggregator.rs`, used from `segments_searcher.rs`) merges per-batch results; `into_topk` applies global ordering after optional resampling correction.

**Across shards (core search)** — `qdrant/lib/collection/src/collection/search.rs`: `merge_from_shards` uses distance order (`Order::LargeBetter` / `SmallBetter`) and `kmerge_by` over shard vectors, deduplicates by point id (`AHashSet`), applies `offset`/`limit` for client requests.

**Across shards (query)** — `qdrant/lib/collection/src/collection/query.rs`: `batch_query_shards_concurrently` may shrink per-shard limits via `modify_shard_query_for_undersampling_limits` + `find_search_sampling_over_point_distribution`. `merge_intermediate_results_from_shards` combines shard-local `ShardQueryResponse` pieces before `intermediates_to_final_list`.

### 8. Post-processing

**Payload / vector second phase** — `Collection::core_search_batch` / `do_query_batch` (both in their respective modules) can run searches without payload/vector, then `fill_search_result_with_payload` (`search.rs`) to `retrieve` by ids and merge.

**Fusion / hybrid** — `intermediates_to_final_list` (`query.rs`): root `ScoringQuery::Fusion` applies `rrf_scoring` (`segment::common::reciprocal_rank_fusion`) for RRF or `score_fusion(..., ScoreFusion::dbsf())` for DBSF; optional `score_threshold`.

**MMR** — `ScoringQuery::Mmr` → `mmr_from_points_with_vector` (`qdrant/lib/collection/src/collection/mmr.rs`), with vector stripping per `with_vector` selector.

**Offset / limit** — `intermediates_to_final_list` ends with `skip(offset).take(limit)` for the final list.

**Slow query telemetry** — `Collection::post_process_if_slow_request` (`search.rs`, also called from `query.rs`) publishes `SlowQueryEvent` when duration exceeds threshold.

**Shard query fill** — `LocalShard::fill_with_payload_or_vectors` (`local_shard/query.rs`) can enrich `ShardQueryResponse` after planned resolution.

## Cross-references

| Area | Primary symbols |
|------|-----------------|
| REST search | `search_points`, `qdrant/src/actix/api/search_api.rs` |
| REST query | `query_points`, `query_points_batch`, `qdrant/src/actix/api/query_api.rs` |
| TOC | `core_search_batch`, `query_batch`, `qdrant/lib/storage/src/content_manager/toc/point_ops.rs` |
| Common glue | `do_core_search_points`, `do_core_search_batch_points`, `qdrant/src/common/query.rs` |
| Collection search | `core_search_batch`, `do_core_search_batch`, `merge_from_shards`, `qdrant/lib/collection/src/collection/search.rs` |
| Collection query | `query_batch`, `do_query_batch`, `do_query_batch_impl`, `batch_query_shards_concurrently`, `intermediates_to_final_list`, `qdrant/lib/collection/src/collection/query.rs` |
| Replica reads | `ShardReplicaSet::core_search`, `query_batch`, `execute_and_resolve_read_operation`, `qdrant/lib/collection/src/shards/replica_set/` |
| Local shard | `core_search`, `query_batch`, `do_planned_query`, `qdrant/lib/collection/src/shards/local_shard/shard_ops.rs`, `local_shard/query.rs`, `local_shard/search.rs` |
| Segment parallel search | `SegmentsSearcher::search`, `search_in_segment`, `execute_batch_search`, `qdrant/lib/collection/src/collection_manager/segments_searcher.rs` |
| Request types | `CoreSearchRequest`, `CoreSearchRequestBatch`, `qdrant/lib/shard/src/search.rs` |
| Segment API | `ReadSegmentEntry::search_batch`, `process_search_result`, `qdrant/lib/segment/src/segment/entry.rs`, `segment/search.rs` |
| HNSW | `HNSWIndex::search`, `search_vectors_with_graph`, `search_with_graph`, `qdrant/lib/segment/src/index/hnsw_index/hnsw.rs` |
| Planned universal query | `PlannedQuery`, `shard::query::planned_query`, `qdrant/lib/collection/src/shards/local_shard/query.rs` |

Related component notes under `qdrant/docs/code-nav/components/`: `api-layer.md`, `collection.md`, `shard.md`, `segment.md`, `indexes.md`.
