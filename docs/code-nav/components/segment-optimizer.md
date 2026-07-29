# Segment optimizers

## Overview

Segment optimizers run in the background to keep shard storage efficient and aligned with collection configuration. They merge small segments, reclaim space from soft-deleted points and vectors, build HNSW (and related) indexes when data grows past thresholds, and rebuild segments when stored layout or parameters no longer match the desired config (payload storage, dense/sparse vector storage, HNSW, quantization, etc.).

## Location

Primary implementation: **`lib/shard/src/optimizers/`**

- `segment_optimizer.rs` — `SegmentOptimizer` trait, shared `optimized_segment_builder` logic, `OptimizationPlanner`, and `plan_optimizations` orchestration.
- `merge_optimizer.rs`, `vacuum_optimizer.rs`, `indexing_optimizer.rs`, `config_mismatch_optimizer.rs` — Concrete optimizers.
- `config.rs` — `SegmentOptimizerConfig` and related inputs.

Execution of a planned optimization (proxies, copy, build, swap) lives in **`lib/shard/src/optimize.rs`** (`execute_optimization`, `build_new_segment`, `optimize_segment_propagate_changes`, `finish_optimization`).

The collection layer constructs optimizer instances and drives when they run (see **`lib/collection/src/update_handler.rs`**, **`lib/collection/src/optimizers_builder.rs`**).

## SegmentOptimizer Trait

`SegmentOptimizer` (`segment_optimizer.rs`) is the common interface for all optimizers:

- **`name`** — Static optimizer label (for logs and telemetry).
- **`segments_path` / `temp_path`** — Where live segments live and where rebuilt segments are built.
- **`segment_optimizer_config`** — Target `SegmentOptimizerConfig` (dense/sparse vector params, payload storage, HNSW per vector, quantization, etc.).
- **`hnsw_global_config`** — Global HNSW settings passed into `SegmentBuilder`.
- **`threshold_config`** — `OptimizerThresholds` (indexing and mmap thresholds in KB, deferred internal id, etc.).
- **`num_indexing_threads`** — Defaults via `max_num_indexing_threads`, using per-vector `max_indexing_threads` from HNSW config.
- **`plan_optimizations(&self, planner: &mut OptimizationPlanner)`** — The extension point: each optimizer inspects remaining segments and calls `OptimizationPlanner::plan` with batches of `SegmentId`s to merge/rebuild. There is no separate `check_condition` method; eligibility is entirely inside `plan_optimizations` (and private helpers). Vacuum, for example, ranks segments by “littered” ratios and schedules the worst; merge uses size-based batching (see optimizer sections below).
- **`optimized_segment_builder`** — Builds a `SegmentBuilder` in `temp_path` with vector/sparse/payload layout derived from merged input sizes and thresholds (enables HNSW + `quantization_config` when over indexing threshold, adjusts mmap vs RAM, sparse index type, etc.).
- **`temp_segment`** — Creates a plain segment for copy-on-write writes during optimization when needed.
- **`optimize`** — Runs `execute_optimization` with paths, `ResourcePermit`, `ResourceBudget`, stop flag, and progress tracker.

`OptimizationPlanner` tracks `remaining` segments, `running` optimizations, and `scheduled` batches (each optionally tied to an optimizer handle for execution). **`plan_optimizations(segments, optimizers)`** runs each optimizer in order so they can consume the planner’s remaining set.

## Optimizer Types

### Merge Optimizer

**`MergeOptimizer`** reduces segment count toward a configured target. It sorts mergeable segments by size and greedily forms batches up to a max-bytes threshold so that each merge actually decreases segment count (documented invariant: merge at least three segments in one batch, or merge two batches, to avoid a no-op count reduction). May leave a new appendable segment when the previous appendable segment was merged.

### Vacuum Optimizer

**`VacuumOptimizer`** targets segments with high ratios of soft-deleted points or, for indexed named vectors, high deletion ratios at the vector-index level. It uses `deleted_threshold`, `min_vectors_number`, and threshold config; only “big” and “littered” segments qualify. Rebuilding compacts storage and refreshes indexes fragmented by deletes.

### Indexing Optimizer

**`IndexingOptimizer`** selects segments that are large enough (per `indexing_threshold_kb` / `memmap_threshold_kb`) but still missing indexes, mmap layout, or need deferred-point handling—so expensive HNSW construction happens in the same rebuild pipeline as other optimizers. It iterates named dense vectors and compares segment `vector_data` against thresholds and `SegmentOptimizerConfig` (`on_disk`, index state).

### Config Mismatch Optimizer

**`ConfigMismatchOptimizer`** compares each segment’s actual `SegmentConfig` to the desired `SegmentOptimizerConfig`: payload storage on disk, dense and sparse vector storage and index types, HNSW parameters (`global_hnsw_config` vs segment), quantization settings, etc. Any mismatch schedules a rebuild so on-disk state matches collection configuration.

## Optimization Process

End-to-end flow in **`execute_optimization`** (`optimize.rs`):

1. **Plan** (before `execute_optimization`): shard/collection code obtains `(optimizer, segment_ids)` batches from `plan_optimizations`.
2. **Lock and validate** inputs; optionally **add a temporary COW segment** if all appendable segments are in the batch (so writes still have a target).
3. **Replace** each input segment in the holder with a **`ProxySegment`** wrapping the original, replicating field indexes onto the temp segment when present; increment **`running_optimizations`**.
4. **Slow path — `optimize_segment_propagate_changes`**: `build_new_segment` creates a `SegmentBuilder` via `OptimizationStrategy` (`ShardOptimizationStrategy` delegates to the optimizer’s `optimized_segment_builder`), **`update`** copies data from wrapped segments, applies **proxy index changes**, **populates** vector storages, then **`build`** runs indexing under a CPU permit from **`ResourceBudget`**. Deletes and ordered index changes from proxies are applied to the new segment.
5. **Fast path — `finish_optimization`**: swap proxies for the new segment (and unwind temp/COW state), decrement running counter, handle cancellation/errors via **`unwrap_proxy`** rollback.

So: **select segments → wrap with proxies (and optional temp segment) → build new segment off-thread → finalize swap under write lock.**

## Triggering

The collection **update handler** runs an **optimizer worker** (`UpdateWorkers::optimization_worker_fn`) that listens on a channel fed by the **update worker** after operations complete. Signals include **`OptimizerSignal::Operation`**, **`Nop`** (explicit wake), and **`Stop`**. The worker calls **`plan_optimizations`** on the current segment holder; if work exists, it spawns constrained tasks that invoke each optimizer’s **`optimize`**. **`OptimizerSignal::Nop`** exists specifically to nudge optimization without tying it to a WAL sequence number.

A **`has_triggered_optimizers`** flag tracks whether any optimization has been scheduled since startup (used for readiness / gating logic). **`optimization_finished`** watch channel connects completion back into the update path.

Periodic **flush** (`flush_worker_fn`) is separate but shares the same handler infrastructure (WAL truncation, segment flush interval).

## Resource Budget

**`ResourceBudget`** and **`ResourcePermit`** (`common::budget`) limit concurrent **IO** and **CPU** used by optimizations. `execute_optimization` receives a permit acquired against the shard’s budget; during `build_new_segment`, after data copy, the code **`replace_with`** on the budget to shift from IO-oriented permits to **`indexing_permit`** for `SegmentBuilder::build` (commented in `optimize.rs` as balancing IO vs CPU when multiple jobs run).

**`max_optimization_threads`** on `UpdateHandler` caps how many optimization jobs may run concurrently at the collection layer; **`optimizer_resource_budget`** is the global CPU budget shared across those jobs.

## Cross-References

- **`ProxySegment`** — `lib/shard/src/proxy_segment/`; records deletes and index mutations during optimization.
- **`SegmentBuilder`** — `lib/segment/src/segment_constructor/segment_builder.rs`; materializes merged segments and builds indexes.
- **`LockedSegmentHolder` / `SegmentHolder`** — `lib/shard/src/segment_holder/`; segment registry, running optimization counter, optimizer error reporting.
- **`OptimizerThresholds`** — `lib/shard/src/operations/optimization/`; threshold values passed into each optimizer.
- **Quantization** — Optimizer’s `optimized_segment_builder` attaches `quantization_config` when crossing indexing threshold; see `docs/code-nav/components/quantization.md`.
- **Updates** — `lib/shard/src/update.rs` documents interaction with deferred points and deduplication during optimization.
