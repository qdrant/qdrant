# Segment optimization execution path

## Overview

Segment optimization runs in the background to keep storage layout healthy: merging small segments, applying indexing thresholds, reclaiming deleted data, and aligning on-disk structure with collection configuration. After updates, the **optimization worker** receives signals, checks whether work is allowed (capacity, WAL recovery, resource budget, concurrent job limits), then **`plan_optimizations`** proposes batches of segment IDs per optimizer. Each job runs **`shard::optimize::execute_optimization`**, which wraps sources in **`ProxySegment`**, builds a new segment (often under a temp path via **`SegmentBuilder`**), then **`finish_optimization`** swaps the result into **`SegmentHolder`** and drops proxy data.

Primary code: `qdrant/lib/collection/src/update_workers/optimization_worker.rs`, `qdrant/lib/shard/src/optimize.rs`, `qdrant/lib/shard/src/optimizers/segment_optimizer.rs`, `qdrant/lib/shard/src/proxy_segment/mod.rs`.

## Trigger

**Signals.** `OptimizerSignal` is defined in `qdrant/lib/collection/src/update_handler.rs` (`Operation(SeqNumberType)`, `Nop`, `Stop`). The optimization worker (`optimization_worker_fn` in `qdrant/lib/collection/src/update_workers/optimization_worker.rs`) loops on an async channel receiving these signals (with a periodic timeout for handle cleanup).

**After updates.** On a successful update, the update worker (`qdrant/lib/collection/src/update_workers/update_worker.rs`) sends `OptimizerSignal::Operation(op_num)` to the optimizer channel. `UpdateSignal::Nop` forwards as `OptimizerSignal::Nop`.

**Re-entrancy.** When an optimization task finishes, its completion callback notifies watchers and `try_send(OptimizerSignal::Nop)` so the worker schedules again. If the cleanup interval removes a finished handle without a new signal, the worker explicitly triggers optimizers to avoid getting stuck (see comment referencing pull/5111 in `optimization_worker.rs`). If the IO **ResourceBudget** is exhausted, a helper task re-sends when budget is available.

**Stop.** `OptimizerSignal::Stop` or a closed channel ends the optimization worker loop.

## Planning

**Entry point.** `plan_optimizations` in `qdrant/lib/shard/src/optimizers/segment_optimizer.rs` takes a read view of `SegmentHolder` and a slice of optimizers. It builds an **`OptimizationPlanner`** seeded with `segments.running_optimizations.count()` and `segments.iter_original()`.

**Planner state.** `OptimizationPlanner` holds `remaining: BTreeMap<SegmentId, &Segment>` (segments still available to schedule), `scheduled` batches, and `running` (expected in-flight optimizations). Each `plan(batch)` removes those IDs from `remaining` and appends `(optimizer, batch)` to `scheduled`.

**Per-optimizer proposals.** For each optimizer in array order, the planner sets `optimizer` and calls `SegmentOptimizer::plan_optimizations(&mut planner)`. Concrete logic lives in merge / indexing / vacuum / config-mismatch implementations under `qdrant/lib/shard/src/optimizers/` (and thin re-exports under `qdrant/lib/collection/src/collection_manager/optimizers/`).

**Concurrency limits (collection layer).** `launch_optimization` in `optimization_worker.rs` iterates the scheduled list until `max_optimization_threads` (as `limit`) is reached. Each started job acquires a **ResourcePermit** from `optimizer_resource_budget` via `try_acquire(0, desired_io)` where `desired_io` equals `optimizer.num_indexing_threads()`. If no permit is available, remaining jobs are postponed; an empty batch may still invoke the callback so the worker retries.

## Execution

### 1. Proxy segment creation

In `execute_optimization` (`qdrant/lib/shard/src/optimize.rs`):

- Input segments must be **`LockedSegment::Original`**; otherwise optimization returns early with zero points.
- Optional **COW / temp appendable segment**: if every appendable segment is among the inputs, `create_temp_segment` adds a writable segment for ongoing updates (`OptimizationStrategy::create_temp_segment`, implemented via the shard optimizer’s `temp_segment` in `segment_optimizer.rs`).
- For each input segment, **`ProxySegment::new`** (`qdrant/lib/shard/src/proxy_segment/mod.rs`) wraps the read-only source. Proxies record deletes, index changes, and deferred points in structures separate from the wrapped segment so reads can continue while the build runs.
- Under a write lock on the segment holder, originals are **`replace`**d by **`LockedSegment::Proxy`**, proxy segment IDs are recorded, optional temp segment is **`add_new_locked`**, and **`running_optimizations`** is incremented.

**`unwrap_proxy`** (`optimize.rs`) restores originals if build fails before swap.

### 2. Segment build

**`optimize_segment_propagate_changes`** calls **`build_new_segment`**, which uses **`OptimizationStrategy::create_segment_builder`** (shard: **`ShardOptimizationStrategy`** → `SegmentOptimizer::optimized_segment_builder`) to construct **`SegmentBuilder`** with vector/payload config derived from thresholds (`segment_optimizer.rs`). The builder merges source segments on disk; **`SegmentBuilder::build`** runs in the slow path with **`ResourceBudget`** / permit handoff for indexing IO vs CPU (`optimize.rs` comments and `replace_with` on the permit).

Deletes and payload index changes observed on proxies are applied to the built segment (again ordering index changes before point deletes where relevant).

**Disk check.** `check_segments_size` estimates input size and temp path free space before starting.

### 3. Swap

**`finish_optimization`** (`optimize.rs`):

- Takes upgradable read on segment holder, **`acquire_updates_lock`** to block conflicting updates.
- Applies accumulated **proxy index changes** and **deletes** onto the optimized segment.
- Upgrades to write lock and **`swap_new(optimized_segment, proxy_ids)`** on `SegmentHolder`, replacing proxies with the new segment; may **`remove_segment_if_not_needed`** for the temp COW id.
- Handles **deferred points** via **`deduplicate_points`** in chunks when needed.
- Drops locks, drops **`locked_proxies`**, then **`drop_data`** on old proxy segments.

On failure after build, **`unwrap_proxy`** restores the previous layout.

## Optimizer selection

**Configured order** is fixed when optimizers are constructed in **`build_optimizers`** (`qdrant/lib/collection/src/optimizers_builder.rs`):

1. **`MergeOptimizer`** — reduces segment count according to `get_number_segments()` and merge thresholds.
2. **`IndexingOptimizer`** — promotes plain segments toward HNSW / mmap / sparse index modes when size crosses indexing and memmap thresholds (see `optimized_segment_builder` in `segment_optimizer.rs`).
3. **`VacuumOptimizer`** — reclaims space from heavy deletes (`deleted_threshold`, `vacuum_min_vector_number`).
4. **`ConfigMismatchOptimizer`** — rebuilds segments whose on-disk config disagrees with collection config / HNSW settings.

Because **`plan_optimizations`** runs optimizers **in that vector order** on a single shared **`OptimizationPlanner`**, earlier optimizers consume segments from **`remaining`** first; later optimizers only see what is still unscheduled. This is **merge → indexing → vacuum → config mismatch**, not “vacuum before indexing.”

## Cross-references

| Topic | Location |
|--------|-----------|
| Optimization worker loop, permits, handles | `qdrant/lib/collection/src/update_workers/optimization_worker.rs` |
| `OptimizerSignal`, `UpdateHandler::run_workers` | `qdrant/lib/collection/src/update_handler.rs` |
| Post-update optimizer notify | `qdrant/lib/collection/src/update_workers/update_worker.rs` |
| `plan_optimizations`, `OptimizationPlanner`, `SegmentOptimizer::optimize` → `execute_optimization` | `qdrant/lib/shard/src/optimizers/segment_optimizer.rs` |
| `execute_optimization`, `finish_optimization`, `unwrap_proxy`, `proxy_deleted_points` | `qdrant/lib/shard/src/optimize.rs` |
| Proxy read/write behavior | `qdrant/lib/shard/src/proxy_segment/mod.rs` |
| Optimizer list construction | `qdrant/lib/collection/src/optimizers_builder.rs` |
| Segment holder swap / locking | `qdrant/lib/shard/src/segment_holder/` (used from `optimize.rs`) |
| Resource budgeting | `common::budget::ResourceBudget` (passed from `UpdateHandler`) |
