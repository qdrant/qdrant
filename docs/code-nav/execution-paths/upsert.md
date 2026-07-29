# Point upsert: execution path

## Overview

This note traces how a **point upsert** flows from an external API (REST or gRPC) through storage and collection layers, replication, the per-shard WAL/update queue, the blocking update worker, segment-level point operations, and finally asynchronous persistence via the flush worker (segment flush + WAL acknowledge). The default REST path is a plain `PointOperations::UpsertPoints`; conditional paths (`update_filter`, `update_mode`) wrap the same downstream machinery with `UpsertPointsConditional`.

## Path Summary

1. **REST** — `upsert_points` → `do_upsert_points` → `update` (`src/actix/api/update_api.rs`, `src/common/update.rs`).
2. **gRPC** — `PointsService::upsert` → `update_common::upsert` → same `do_upsert_points` / `update` (`src/tonic/api/points_api.rs`, `src/tonic/api/update_common.rs`, `src/common/update.rs`).
3. **Strict mode + TOC** — `CheckedTocProvider::check_strict_mode` inside `do_upsert_points`; then `TableOfContent::update` with RBAC (`src/common/update.rs`, `lib/storage/src/content_manager/toc/point_ops.rs`).
4. **Collection routing** — `Collection::update_from_client` splits work by shard (`split_by_shard`, `split_by_mode`) and calls `ShardReplicaSet::update_with_consistency` per shard (`lib/collection/src/collection/point_ops.rs`).
5. **Replication** — `ShardReplicaSet::update_with_consistency` picks a leader, then `update` → `update_impl`: stamps `ClockTag`, runs local `ShardOperation::update` and remote `update` futures, enforces `write_consistency_factor` (`lib/collection/src/shards/replica_set/update.rs`).
6. **Local shard** — `LocalShard::update` (`shard_ops.rs`): disk check, `RecoverableWal::lock_and_write`, enqueue `UpdateSignal::Operation` (`lib/collection/src/shards/local_shard/shard_ops.rs`).
7. **Update worker** — `UpdateWorkers::update_worker_fn` dequeues, optionally rehydrates op from WAL, `spawn_blocking` → `update_worker_internal` → `CollectionUpdater::update` (`lib/collection/src/update_workers/update_worker.rs`, `lib/collection/src/collection_manager/collection_updater.rs`).
8. **Segment path** — `process_point_operation` → `upsert_points` → `apply_points_with_conditional_move` / `smallest_appendable_segment` → `upsert_with_payload` → `SegmentEntry::upsert_point` (`lib/shard/src/update.rs`, `lib/segment/src/segment/entry.rs`).
9. **Durability after apply** — `AppliedSeqHandler::update`; optimizer signal; client callback with `UpdateStatus` (`lib/collection/src/update_workers/update_worker.rs`).
10. **Flush / WAL ack** — `UpdateWorkers::flush_worker_fn` on an interval: WAL flush, `SegmentHolder::flush_all`, `wal.ack`, clock persistence (`lib/collection/src/update_workers/flush_workers.rs`, `lib/collection/src/update_handler.rs`).

## Detailed Walkthrough

### 1. API Layer

**REST** — Handler `upsert_points` (`PUT /collections/{collection_name}/points`) builds `InternalUpdateParams::default()`, reads `UpdateParams` (`wait`, `ordering`, `timeout`), and calls `do_upsert_points` with `StrictModeCheckedTocProvider::new(&dispatcher)` (`src/actix/api/update_api.rs`).

**gRPC** — `PointsService::upsert` validates the request, extracts auth and inference keys, converts the proto body to `PointInsertOperations::PointsList` (including `shard_key`, `update_filter`, `update_mode`), and calls `do_upsert_points` with `UpdateParams::from_grpc` (`src/tonic/api/points_api.rs`, `src/tonic/api/update_common.rs`).

**Common processing** — `do_upsert_points`:

- Obtains `TableOfContent` via `toc_provider.check_strict_mode(&operation, &collection_name, …)` (strict mode + collection access).
- Normalizes inference (batch/list), producing `PointInsertOperationsInternal` and optional `UpdateMode` / `Filter`.
- Maps to `CollectionUpdateOperations::PointOperation`: either `PointOperations::UpsertPoints`, `UpsertPointsConditional`, etc. (`src/common/update.rs`).

**Dispatch to storage** — `update(toc, collection_name, operation, internal_params, params, shard_key, auth, hw_measurement_acc)` resolves `WaitUntil` from `internal_params.wait_override` or `WaitUntil::from(params.wait)`, builds `ShardSelectorInternal` via `get_shard_selector_for_update`, wraps `OperationWithClockTag::new`, and calls `toc.update(…)` (`src/common/update.rs`).

### 2. Storage Layer (TableOfContent)

`TableOfContent::update` (`lib/storage/src/content_manager/toc/point_ops.rs`):

- `auth.check_point_op(collection_name, &operation.operation, operation.operation.operation_name())`.
- `get_collection(&collection_pass).await?`.
- Optional cluster-wide update rate limiting when the selector is not a direct shard id.
- Branches on `shard_selector`: `Empty` / `All` / `ShardKey(s)` / `ShardKeyWithFallback` / `ShardId`:
  - Client-origin updates use `Collection::update_from_client` (possibly `_update_shard_keys` for multiple keys).
  - `ShardId` uses `Collection::update_from_peer` (internal/forwarded path with clock tag already set).

### 3. Collection Layer

`Collection::update_from_client` (`lib/collection/src/collection/point_ops.rs`):

- Reads `shards_holder`, then on the update runtime:
  - `shard_holder.split_by_shard(operation, &shard_keys_selection)?` — hash-ring / key routing to per-shard operations.
  - `split_by_mode(shard_id, operation)` — separates `update_all` vs `update_only_existing` batches.
- For each shard, `shard.update_with_consistency(operation, wait, timeout, ordering, update_only_existing, hw_acc)` in a `FuturesUnordered` pipeline.
- Aggregates per-shard `UpdateResult` (status priority, operation id max, timeout handling).

### 4. Replication Layer

`ShardReplicaSet::update_with_consistency` (`lib/collection/src/shards/replica_set/update.rs`):

- `leader_peer_for_update(ordering)` (`Weak` → this peer; `Medium` / `Strong` → highest alive / highest replica id).
- If this peer is leader: optional `write_ordering_lock` for strong/medium, then private `update`.
- Else: `forward_update` to the leader.

Private `update` acquires a `ClockGuard` (`get_clock`), then loops up to `UPDATE_MAX_CLOCK_REJECTED_RETRIES` calling `update_impl`.

`update_impl`:

- Builds `ClockTag::new(this_peer_id, clock.id(), tick)` and `OperationWithClockTag`.
- Schedules **local** `local.get().update(operation, local_wait, …)` when updatable (`Listener` forces `WaitUntil::Wal` for local apply).
- Schedules **remote** `remote.update(operation, wait, …)` for each updatable peer.
- Collects results (optional concurrency cap from `shared_storage_config`), advances clock if a replica echoes a newer tick, applies `write_consistency_factor`, handles partial failures / deactivation.

### 5. Local Shard

`LocalShard::update` (`lib/collection/src/shards/local_shard/shard_ops.rs`):

- If `wait.needs_callback()`, creates a `oneshot` for `InternalUpdateResult`.
- Disk full check via `disk_usage_watcher`.
- Under `update_lock` (read): reserves space on `update_sender`, then **`wal.lock_and_write(&mut operation).await`** — assigns `operation_id` (op seq), persists to WAL; on `WalError::ClockRejected` returns `UpdateStatus::ClockRejected`.
- Optionally drops in-RAM operation payload if the pending queue exceeds `DEFAULT_UPDATE_QUEUE_RAM_BUFFER`; otherwise embeds `Box::new(operation.operation)`.
- Sends `UpdateSignal::Operation(OperationData { op_num, operation, sender, wait_for_deferred: wait.wait_for_deferred(), hw_measurements })`.
- Awaits callback (if any) with timeout handling → `UpdateStatus::Completed` / `WaitTimeout` / `Acknowledged`.

### 6. Update Worker

`UpdateWorkers::update_worker_fn` (`lib/collection/src/update_workers/update_worker.rs`):

- On `UpdateSignal::Operation`, may deserialize the op from WAL if `operation` is `None`.
- `spawn_blocking` → `update_worker_internal(collection_name, operation, op_num, wait, …)` where `wait` is `sender.is_some()` (i.e. client is waiting for segment-level completion).
- **`update_worker_internal`**: if `wait`, **`wal.blocking_lock().flush()`** before applying; then **`CollectionUpdater::update`** (`segments`, `op_num`, `operation`, locks, `update_tracker`, HW counter).
- On success: notify optimizer (`OptimizerSignal::Operation(op_num)`), `applied_seq_handler.update(op_num)`, optional `wait_for_deferred_points_ready` when `prevent_unoptimized` + visible wait, then `send_feedback` with `InternalUpdateResult`.

`CollectionUpdater::update` (`lib/collection/src/collection_manager/collection_updater.rs`):

- `block_in_place`: `update_operation_lock` write, `update_tracker.update()`, `segments.acquire_updates_lock()`, `segments.read()`.
- Dispatches `CollectionUpdateOperations::PointOperation` → **`process_point_operation`** (`shard::update`).

### 7. Segment Layer

`process_point_operation` (`lib/shard/src/update.rs`):

- `PointOperations::UpsertPoints` → **`upsert_points`**.

`upsert_points`:

- Chunks point ids (`UPDATE_OP_CHUNK_SIZE`).
- For each chunk: **`segments.apply_points_with_conditional_move`** to update existing points in writable segments via closure calling **`upsert_with_payload`**.
- Inserts IDs not updated into **`segments.smallest_appendable_segment()`** (appendable segment), again via **`upsert_with_payload`**.

`upsert_with_payload`:

- **`segment.upsert_point(op_num, point_id, vectors, hw_counter)`** then payload: `set_full_payload` or `clear_payload`.

`Segment::upsert_point` (`lib/segment/src/segment/entry.rs`, `SegmentEntry` impl):

- Asserts appendable for inserts, validates vectors, uses **`handle_point_version_and_failure`**: if id exists → **`replace_all_vectors`**, else **`insert_new_vectors`**.

### 8. Storage Write

Within `Segment` (invoked from `SegmentEntry` methods): vector storages are updated for the internal id, payload storage reflects `set_full_payload` / `clear_payload`, and the **id tracker** maps external id ↔ internal id and versions. Payload / vector indexes are updated according to segment configuration (details live beside `insert_new_vectors` / `replace_all_vectors` in `lib/segment`).

### 9. Flush

`UpdateHandler::run_workers` starts **`UpdateWorkers::flush_worker_fn`** alongside the update and optimizer workers (`lib/collection/src/update_handler.rs`).

`flush_worker_internal` (`lib/collection/src/update_workers/flush_workers.rs`), on each interval:

1. `wal.blocking_lock().flush_async()` (join).
2. `segments.read().flush_all(false, false)` → **`confirmed_version`** (capped by minimum failed op if any).
3. `clocks.store_if_changed(&shard_path)`.
4. `wal.blocking_lock().ack(ack)` with `ack = confirmed_version.min(wal_keep_from - 1)` so retained WAL entries (e.g. queue proxy) are not truncated.

Comments in `LocalShard` recovery (`lib/collection/src/shards/local_shard/mod.rs`) describe the invariant: **after segments flush, the WAL can be truncated/acknowledged up to the last applied+flushed operation** (tracked via `SerdeWal` / WAL layer).

## Wait Semantics

`WaitUntil` (`lib/collection/src/shards/shard_trait.rs`):

| Variant | `needs_callback` | `wait_for_deferred` | Meaning |
|--------|------------------|---------------------|---------|
| **Wal** | false | false | Return after WAL append + queue signal; no segment-completion callback. REST `wait=false` maps here via `From<bool>`. |
| **Segment** | true | false | Wait until the update worker finishes applying to segments (callback). WAL is explicitly flushed in `update_worker_internal` when this callback exists. |
| **Visible** | true | true | Same callback path as Segment, plus the worker may block on deferred-point optimization when `prevent_unoptimized` is enabled, so search-visible semantics hold. REST `wait=true` maps here. |

gRPC can set `WaitUntil` explicitly (`InternalUpdateParams::wait_override` / proto `WaitUntil`).

**Local vs replication:** `ReplicaState::Listener` downgrades local apply wait to **`WaitUntil::Wal`** inside `update_impl` so listeners do not block the leader on full segment wait.

## Cross-References

- API surface: `docs/code-nav/components/api-layer.md`
- Collection / shard concepts: `docs/code-nav/components/collection.md`, `docs/code-nav/components/shard.md`
- Segment internals: `docs/code-nav/components/segment.md`, `docs/code-nav/components/vector-storage.md`, `docs/code-nav/components/payload-storage.md`, `docs/code-nav/components/id-tracker.md`
- Replication: `docs/code-nav/components/replication.md`
- RBAC: `docs/code-nav/components/rbac-and-auth.md`
