# Collection crate (`collection`)

Code navigation for the **`collection`** Rust crate at `lib/collection/`: the layer that owns a **distributed collection** (sharding, replication, transfers, optimizers, and the update pipeline). A [`Collection`](../../../lib/collection/src/collection/mod.rs) is the user-facing unit that maps to one or more [`ShardReplicaSet`](../../../lib/collection/src/shards/replica_set/mod.rs)s.

---

## Overview

The collection crate manages a distributed collection of points. It handles sharding, replication, shard transfers, optimizers, and the update pipeline. A `Collection` is the user-facing unit that maps to one or more shards, each represented by a `ShardReplicaSet` that may hold a local [`Shard`](../../../lib/collection/src/shards/shard.rs) variant and remote [`RemoteShard`](../../../lib/collection/src/shards/remote_shard/) gRPC clients. Persistent shard data on disk is ultimately split into segments (see the [`segment`](./segment.md) crate); this crate orchestrates how updates and searches reach those segments across peers.

---

## Crate location

| Path | Role |
|------|------|
| `qdrant/lib/collection/` | Crate root (`Cargo.toml`, `src/`) |
| `qdrant/lib/collection/src/lib.rs` | Public module surface |

---

## Module map

Top-level modules from [`lib.rs`](../../../lib/collection/src/lib.rs) (declaration order). Paths are under `lib/collection/src/` unless noted.

| Module | Path | Description |
|--------|------|-------------|
| `collection` | `collection/` | `Collection` struct and collection-level ops: search, updates, snapshots, shard transfer hooks, resharding, telemetry wiring. |
| `collection_manager` | `collection_manager/` | `CollectionUpdater`, `SegmentsSearcher`, segment holder, optimizers. |
| `collection_state` | `collection_state.rs` | Serializable collection/shard state types used with consensus and recovery. |
| `common` | `common/` | Shared helpers: batching, size stats, file/snapshot utilities, `IsReady`, stoppable tasks, etc. |
| `config` | `config.rs` | Internal collection configuration (`CollectionConfigInternal`, sharding method). |
| `discovery` | `discovery.rs` | Discover-query orchestration on `Collection` (context pairs, lookup collection). |
| `grouping` | `grouping.rs` | Group-by / aggregation style flows over collection search. |
| `hash_ring` | `hash_ring.rs` | Consistent hashing router for custom shard keys (`HashRingRouter`, shard ID sets). |
| `lookup` | `lookup.rs` | Lookup collection / cross-collection vector resolution helpers. |
| `operations` | `operations/` | Request/response types, conversions, `SplitByShard`, cluster ops, snapshot ops, universal query types. |
| `optimizers_builder` | `optimizers_builder.rs` | Builds optimizer instances from config for a shard. |
| `problems` | `problems/` | Diagnostics such as unindexed field reporting (`UnindexedField`). |
| `recommendations` | `recommendations.rs` | Recommend-query orchestration on `Collection`. |
| `shards` | `shards/` | Shard types, replica set, holder, transfers, channel service, proxy shards, resharding. |
| `telemetry` | `telemetry.rs` | Collection-level telemetry aggregation and gRPC mapping. |
| `update_handler` | `update_handler.rs` | **Private** module: `UpdateHandler`, `UpdateSignal`, `OperationData`, optimizer/flush/update worker handles. |
| `wal_delta` | `wal_delta.rs` | WAL delta recovery types used with local shards and transfers. |
| `events` | `events.rs` | Lightweight event structs (`CollectionDeletedEvent`, `SlowQueryEvent`, `IndexCreatedEvent`). |
| `profiling` | `profiling/` | Request logging / profiling hooks used from update path. |
| `update_workers` | `update_workers/` | `UpdateWorkers` impls: `update_worker_fn`, optimization worker, flush workers, applied sequence handling. |
| `tests` | `tests/` | **Test-only** (`#[cfg(test)]`) integration tests. |

---

## The `Collection` struct

Defined in [`collection/mod.rs`](../../../lib/collection/src/collection/mod.rs) (struct starts ~line 64).

| Field / group | Type (conceptual) | Role |
|---------------|-------------------|------|
| `id` | `CollectionId` (`String`) | Collection name. |
| `shards_holder` | `SharedShardHolder` | Shared [`ShardHolder`](../../../lib/collection/src/shards/shard_holder/mod.rs): all `ShardReplicaSet` instances and shard-key mapping. |
| `collection_config` | `Arc<RwLock<CollectionConfigInternal>>` | Live collection parameters (vectors, optimizers, etc.). |
| `shared_storage_config` | `Arc<SharedStorageConfig>` | Storage-wide options (e.g. default transfer method). |
| `payload_index_schema` | `Arc<SaveOnDisk<PayloadIndexSchema>>` | On-disk payload index schema for the collection. |
| `optimizers_overwrite` | `Option<OptimizersConfigDiff>` | Optional optimizer config overlay at creation. |
| `this_peer_id` | `PeerId` | Current node in cluster. |
| `path` / `snapshots_path` | `PathBuf` | Collection data and snapshot directories. |
| `channel_service` | `ChannelService` | gRPC channel factory for talking to remote peers. |
| `transfer_tasks` | `Mutex<TransferTasksPool>` | In-flight shard transfer tasks for this collection. |
| `request_shard_transfer_cb` | `RequestShardTransfer` | Callback to ask consensus/cluster to start a transfer. |
| `notify_peer_failure_cb` | `ChangePeerFromState` | Replica failure notification upward. |
| `abort_shard_transfer_cb` | `AbortShardTransfer` | Cancels transfers from replica-set logic. |
| `is_initialized` | `Arc<IsReady>` | One-way flag: all shards activated after first init. |
| `update_runtime` / `search_runtime` | `Handle` | Tokio handles for update vs search work. |
| `optimizer_resource_budget` | `ResourceBudget` | Global CPU budget for optimization. |
| `collection_stats_cache` | `CollectionSizeStatsCache` | Cached size statistics. |
| `shard_clean_tasks` | `ShardCleanTasks` | Background shard cleanup. |

**Responsibilities:** construct and hold all replica sets from [`CollectionShardDistribution`](../../../lib/collection/src/shards/collection_shard_distribution.rs); route operations through the shard holder and hash ring; drive shard transfer lifecycle via [`TransferTasksPool`](../../../lib/collection/src/shards/transfer/transfer_tasks_pool.rs); expose collection-level APIs (query, recommend, discover, clustering, snapshots) implemented under `collection/` submodules (`search.rs`, `point_ops.rs`, `shard_transfer.rs`, etc.).

---

## Shard architecture

### `ShardReplicaSet`

[`shards/replica_set/mod.rs`](../../../lib/collection/src/shards/replica_set/mod.rs): one logical shard’s replicas on this peer.

- **`local`:** `RwLock<Option<Shard>>` — may be `Local` or a **proxy** variant during transfers.
- **`remotes`:** `RwLock<Vec<RemoteShard>>` — outbound gRPC clients to peers hosting other replicas.
- **`replica_state`:** `Arc<SaveOnDisk<ReplicaSetState>>` — persisted peer → [`ReplicaState`](../../../lib/collection/src/shards/replica_set/replica_set_state.rs) map.
- **`locally_disabled_peers`:** peers treated as dead locally until consensus catches up.
- **`clock_set`:** [`ClockSet`](../../../lib/collection/src/shards/replica_set/clock_set.rs) for tagging operations with logical clocks.
- **`write_ordering_lock`:** serializes writes when ordering is required.

**Behavior (from doc comment):** keep replica state consistent; prefer the local shard for reads when possible; apply updates to all replicas and surface errors if any replica fails.

**Replica state machine:** ASCII diagram in `replica_set/mod.rs` (~lines 55–88) summarizes transitions among `Initializing`, `Active`, `Listener`, `Partial`, `Dead`, and transfer-related paths. `ReplicaState` enum also includes `PartialSnapshot`, `Recovery`, `Resharding`, `ReshardingScaleDown`, `ActiveRead`, `ManualRecovery`, etc. — see `replica_set_state.rs`.

### `Shard` enum

[`shards/shard.rs`](../../../lib/collection/src/shards/shard.rs):

| Variant | Wrapped type | Role |
|---------|--------------|------|
| `Local` | `LocalShard` | Full local data: WAL, segments, update pipeline. |
| `Proxy` | `ProxyShard` | Proxies to remote shard (transfer / routing). |
| `ForwardProxy` | `ForwardProxyShard` | Forwards updates while wrapping a local shard. |
| `QueueProxy` | `QueueProxyShard` | Queues updates during transfer; interacts with WAL retention (`wal_keep_from` in `UpdateHandler`). |
| `Dummy` | `DummyShard` | Placeholder when shard not loaded. |

`Shard::get()` returns `&(dyn ShardOperation + Sync + Send)` for dynamic dispatch.

### `LocalShard`

[`shards/local_shard/mod.rs`](../../../lib/collection/src/shards/local_shard/mod.rs) (`LocalShard` ~lines 99–139):

- **`segments`:** `LockedSegmentHolder` — all [`Segment`](./segment.md) handles for this shard.
- **`wal`:** `RecoverableWal` — write-ahead log (`crate::wal_delta::RecoverableWal`).
- **`update_handler`:** `Arc<Mutex<UpdateHandler>>` — owns optimizers list, WAL handle, segment holder reference, and worker join handles.
- **`update_sender`:** `ArcSwap<Sender<UpdateSignal>>` — channel into the update worker.
- **`update_tracker`:** `UpdateTracker` — tracks in-flight updates for wait/visibility.
- **`optimizers` / `optimizers_log` / `total_optimized_points`:** optimization bookkeeping.
- **`update_operation_lock` / scroll coordination:** `RwLock` pairs with `UpdateHandler`’s `scroll_read_lock` for consistent scroll/snapshot sections.

### `RemoteShard`

Module [`shards/remote_shard/`](../../../lib/collection/src/shards/remote_shard/): gRPC client implementation calling peer collection APIs for the same logical shard (updates, reads, transfers).

### `ShardOperation` trait

[`shards/shard_trait.rs`](../../../lib/collection/src/shards/shard_trait.rs): async trait implemented by local and proxy shards. Core methods include `update` (with [`WaitUntil`](../../../lib/collection/src/shards/shard_trait.rs): `Wal` / `Segment` / `Visible`), `scroll_by`, search/count/query helpers, etc. `WaitUntil` maps user `wait` semantics to how long the collection waits before acknowledging.

---

## Update pipeline

**High-level path**

1. **`Collection`** receives an operation and uses the [`ShardHolder`](../../../lib/collection/src/shards/shard_holder/mod.rs) to split work by shard via [`SplitByShard`](../../../lib/collection/src/operations/mod.rs) and [`HashRingRouter`](../../../lib/collection/src/hash_ring.rs) (`split_by_shard` in `shard_holder/mod.rs`).
2. Each target **`ShardReplicaSet`** executes updates on local and remote replicas; local path uses the active **`Shard`** (usually `Local` or a proxy during transfer).
3. **`LocalShard`** appends to the WAL, assigns sequence numbers, and sends [`UpdateSignal::Operation(OperationData)`](../../../lib/collection/src/update_handler.rs) on the update channel (`OperationData` carries `op_num`, optional in-memory `operation` or reload-from-WAL, feedback `oneshot`, `wait_for_deferred`, hardware metrics).
4. **`UpdateHandler`** (via [`UpdateWorkers::update_worker_fn`](../../../lib/collection/src/update_workers/update_worker.rs)) consumes signals: on `Operation`, it may flush WAL when waiting, then runs [`CollectionUpdater::update`](../../../lib/collection/src/collection_manager/collection_updater.rs) on the **`LockedSegmentHolder`** under the update locks.
5. After a successful apply, an [`OptimizerSignal::Operation(op_num)`](../../../lib/collection/src/update_handler.rs) is sent to the **optimization worker**, which schedules segment optimizers (`plan_optimizations`, `SegmentOptimizer` trait objects).
6. **`flush_worker`** periodically flushes segments and truncates the WAL subject to `wal_keep_from` and retention (see `UpdateHandler` fields `flush_interval_sec`, `wal_keep_from`).

**Workers (types in `update_handler.rs`)**

| Worker | Field | Role |
|--------|-------|------|
| Update | `update_worker` | WAL + `CollectionUpdater` application loop (`update_workers/update_worker.rs`). |
| Optimization | `optimizer_worker` | Reacts to `OptimizerSignal`, runs merge/index optimizers with concurrency limits (`max_optimization_threads`, `optimization_handles`). |
| Flush | `flush_worker` | Periodic flush + WAL truncation (`update_workers/flush_workers.rs`). |

**Signals:** `UpdateSignal` also has `Nop` (poke optimizers) and `Plunger` (barrier / drain). `OptimizerSignal` supports `Stop` for shutdown.

---

## Shard transfers

Types live under [`shards/transfer/`](../../../lib/collection/src/shards/transfer/). [`ShardTransferMethod`](../../../lib/collection/src/shards/transfer/mod.rs) (documented ~lines 275–295):

| Variant | Meaning |
|---------|---------|
| `StreamRecords` | Stream all shard records in batches until complete. |
| `Snapshot` | Snapshot shard, transfer archive, restore on receiver. |
| `WalDelta` | Transfer difference via WAL delta when possible. |
| `ReshardingStreamRecords` | Streaming transfer used in resharding migrations. |

[`ShardTransfer`](../../../lib/collection/src/shards/transfer/mod.rs) carries source/target peer IDs, shard id, optional method, etc. [`transfer/driver.rs`](../../../lib/collection/src/shards/transfer/driver.rs) dispatches by method. **Proxy shards** (`Proxy`, `ForwardProxy`, `QueueProxy`) temporarily replace or wrap `LocalShard` so traffic and replication stay consistent while data moves; `QueueProxy` ties into extended WAL retention in `UpdateHandler`.

Collection-level orchestration: [`collection/shard_transfer.rs`](../../../lib/collection/src/collection/shard_transfer.rs), [`shards/replica_set/shard_transfer.rs`](../../../lib/collection/src/shards/replica_set/shard_transfer.rs).

---

## Key submodules

### `operations/`

[`operations/mod.rs`](../../../lib/collection/src/operations/mod.rs): **`SplitByShard`** trait (`split_by_shard` → **`OperationToShard<O>`** (`ByShard` vs `ToAll`)); impls for **`CollectionUpdateOperations`** and point/payload/vector op enums (`point_ops.rs`, `payload_ops.rs`, `vector_ops.rs`). Houses **`types`**, **`cluster_ops`**, **`conversions`**, **`universal_query`**, snapshot and config diff types, etc.

### `collection_manager/`

[`collection_manager/mod.rs`](../../../lib/collection/src/collection_manager/mod.rs): **`collection_updater`** (apply ops to segments), **`segments_searcher`**, **`holders`** (e.g. **`SegmentHolder`**), **`optimizers`** (segment optimizer traits, tracker log).

### `shards/`

[`shards/mod.rs`](../../../lib/collection/src/shards/mod.rs): **`channel_service`**, **`shard_holder`**, **`replica_set`**, **`local_shard`**, **`remote_shard`**, **`transfer`**, **`proxy_shard`**, **`forward_proxy_shard`**, **`queue_proxy_shard`**, **`dummy_shard`**, **`resharding`**, **`update_tracker`**, **`shard_config`**, helpers like **`shard_path`**.

### `update_workers/`

[`update_workers/mod.rs`](../../../lib/collection/src/update_workers/mod.rs): **`update_worker`**, **`optimization_worker`**, **`flush_workers`**, **`applied_seq`**, **`internal_update_result`**. Static methods on **`UpdateWorkers`** implement the async/blocked loops invoked from **`UpdateHandler::new` / run**.

---

## Cross-references

| Topic | Location |
|--------|-----------|
| Segment storage unit | [segment.md](./segment.md) (`lib/segment/`) |
| Shard types & enum | `lib/collection/src/shards/shard.rs` |
| Replica set & state | `lib/collection/src/shards/replica_set/mod.rs`, `replica_set_state.rs` |
| Transfer methods | `lib/collection/src/shards/transfer/mod.rs`, `transfer/driver.rs` |
| Update handler & signals | `lib/collection/src/update_handler.rs` |
| Update worker loop | `lib/collection/src/update_workers/update_worker.rs` |

Related docs (same `docs/code-nav/components/` folder) — add or follow when present:

- [shard.md](./shard.md) — deep dive on single-shard layout and proxy behavior  
- [replication.md](./replication.md) — consensus interaction and replica states  
- [segment.md](./segment.md) — segment internals consumed via `SegmentHolder`  

Upstream crate: many types come from the **`shard`** library crate (`shard::operations`, `shard::segment_holder`, etc.) re-exported through `collection::operations`.
