# Storage crate (`storage`)

Code navigation for the **`storage`** Rust crate at [`lib/storage/`](../../../lib/storage/): the service layer between API servers and the `collection` crate. It owns the **Table of Content** (all loaded collections), the **Dispatcher** (routes collection meta-operations through Raft consensus or directly), and integrates with **Raft** for distributed cluster mode.

---

## Overview

The storage crate is the service layer between the API servers and collections. It owns the **TableOfContent** (all collections), the **Dispatcher** (routes meta-operations through consensus or directly), and integrates with **Raft consensus** for cluster mode.

- **No transport:** The crate exposes types and async/sync operations usable from REST, gRPC, or other fronts; it does not implement HTTP or tonic handlers ([`lib.rs`](../../../lib/storage/src/lib.rs)).
- **Single ToC:** Typically one [`TableOfContent`](../../../lib/storage/src/content_manager/toc/mod.rs) per process, created at startup.
- **Meta vs points:** Collection-level changes flow through [`CollectionMetaOperations`](../../../lib/storage/src/content_manager/collection_meta_ops.rs) (often via [`Dispatcher::submit_collection_meta_op`](../../../lib/storage/src/dispatcher.rs)); point-level work is handled under `content_manager/toc/` (e.g. `point_ops.rs`).

---

## Crate Location

| Path | Role |
|------|------|
| `qdrant/lib/storage/` | Crate root (`Cargo.toml`, `src/`) |
| `qdrant/lib/storage/src/lib.rs` | Public re-exports and top-level modules |

---

## Module Map

Top-level modules from [`lib/storage/src/lib.rs`](../../../lib/storage/src/lib.rs). Subpaths are under `lib/storage/src/` unless noted.

| Module | Path | Description |
|--------|------|-------------|
| `audit` | `audit.rs` | Audit logging hooks for storage-level events. |
| `audit_reader` | `audit_reader.rs` | Reading / querying audit data. |
| `common` | `common/` | Internal helpers (`utils`, etc.). |
| `content_manager` | `content_manager/` | ToC, consensus manager, aliases, snapshots, errors, shard distribution, collection meta ops (see [Content Manager Structure](#content-manager-structure)). |
| `dispatcher` | `dispatcher.rs` | [`Dispatcher`](../../../lib/storage/src/dispatcher.rs): `Arc<TableOfContent>` + optional `ConsensusStateRef`; `submit_collection_meta_op`, `cluster_status`. |
| `issues_subscribers` | `issues_subscribers.rs` | Subscribers for operational issues / diagnostics. |
| `rbac` | `rbac/` | Auth and access checks for collections and meta-operations (`Auth`, `CollectionPass`, `ops_checks`, …). |
| `types` | `types.rs` | `StorageConfig`, `ClusterStatus`, peer/cluster info types used across storage and consensus. |
| `serialize_peer_addresses` | inline in `lib.rs` | Serde helpers for `PeerAddressById` (map of peer id → address URI). |

**`content_manager` submodules** (from [`content_manager/mod.rs`](../../../lib/storage/src/content_manager/mod.rs)):

| Module | Path | Description |
|--------|------|-------------|
| `alias_mapping` | `alias_mapping.rs` | Alias persistence and mapping. |
| `collection_meta_ops` | `collection_meta_ops.rs` | `CollectionMetaOperations` and related structs/enums. |
| `collection_verification` | `collection_verification.rs` | Verification passes for collection access. |
| `collections_ops` | `collections_ops.rs` | `Collections` map wrapper, existence checks (`Checker`). |
| `consensus` | `consensus/` | WAL, entry queue, `OperationSender`, `Persistent` raft state. |
| `consensus_manager` | `consensus_manager.rs` | [`ConsensusManager`](../../../lib/storage/src/content_manager/consensus_manager.rs), `ConsensusStateRef`, Raft `Storage` impl. |
| `consensus_ops` | `content_manager/mod.rs` (inline) | [`ConsensusOperations`](../../../lib/storage/src/content_manager/mod.rs): what goes on the Raft log (including boxed `CollectionMetaOperations`). |
| `conversions` | `conversions.rs` | Type conversions for API / internal use. |
| `errors` | `errors.rs` | `StorageError` and related error types. |
| `shard_distribution` | `shard_distribution.rs` | `ShardDistributionProposal` for create-collection placement. |
| `snapshots` | `snapshots/` | Cluster / full snapshot orchestration (`recover`, `download`, …). |
| `staging` | `staging.rs` | **Feature `staging`**: test-only slowdown hooks. |
| `toc` | `toc/` | [`TableOfContent`](../../../lib/storage/src/content_manager/toc/mod.rs) and collection/point/snapshot logic (see below). |

**`toc/` internal modules** (from [`toc/mod.rs`](../../../lib/storage/src/content_manager/toc/mod.rs)):

| Module | Path | Description |
|--------|------|-------------|
| `collection_container` | `toc/collection_container.rs` | `CollectionContainer` impl for ToC (snapshots, `perform_collection_meta_op` bridge). |
| `collection_meta_ops` | `toc/collection_meta_ops.rs` | `TableOfContent::perform_collection_meta_op` match and per-op handlers. |
| `create_collection` | `toc/create_collection.rs` | Collection creation path from ToC. |
| `dispatcher` | `toc/dispatcher.rs` | `TocDispatcher`: internal bridge when both ToC and consensus are wired. |
| `point_ops` / `point_ops_internal` | `toc/point_ops*.rs` | Point-level operations on collections. |
| `request_hw_counter` | `toc/request_hw_counter.rs` | Hardware counter requests per collection. |
| `snapshots` | `toc/snapshots.rs` | Collection-scoped snapshot helpers from ToC. |
| `telemetry` | `toc/telemetry.rs` | `TocTelemetryCollector` for ToC metrics. |
| `temp_directories` | `toc/temp_directories.rs` | Temporary directory management for storage. |
| `transfer` | `toc/transfer.rs` | Shard transfer handling at ToC level. |

**`consensus/`** ([`consensus/mod.rs`](../../../lib/storage/src/content_manager/consensus/mod.rs)):

| Submodule | Role |
|-----------|------|
| `consensus_wal` | Append-only log of Raft entries (`ConsensusOpWal`). |
| `entry_queue` | Entry IDs / apply progress tracking. |
| `operation_sender` | Channel to external consensus thread (`OperationSender`). |
| `persistent` | On-disk Raft + peer metadata (`Persistent`). |

---

## TableOfContent (ToC)

Defined in [`content_manager/toc/mod.rs`](../../../lib/storage/src/content_manager/toc/mod.rs).

### What it owns

| Field | Role |
|-------|------|
| `collections` | `Arc<RwLock<Collections>>` — in-memory map of collection name → `Arc<Collection>`. |
| `storage_config` | `Arc<StorageConfig>` — paths, performance, collection defaults, snapshots path, etc. |
| `search_runtime` / `update_runtime` / `general_runtime` | Three separate **Tokio** `Runtime` instances: search work, updates, and general blocking/async orchestration (e.g. loading collections). |
| `optimizer_resource_budget` | `ResourceBudget` — global CPU budget for optimization across collections. |
| `alias_persistence` | `RwLock<AliasPersistence>` — durable alias map (opened from disk at init). |
| `this_peer_id` | `PeerId` — identity of this node in the cluster. |
| `channel_service` | `ChannelService` — gRPC channels to remote peers (from `collection` crate). |
| `consensus_proposal_sender` | `Option<OperationSender>` — if `Some`, distributed mode: ToC can propose cluster operations; if `None`, standalone. |
| `toc_dispatcher` | `Mutex<Option<TocDispatcher>>` — optional backlink for internal dispatch when consensus is attached. |
| `update_rate_limiter` | `Option<Semaphore>` — limits concurrent updates in distributed mode (configurable or auto from CPU count). |
| `collection_create_lock` | `Mutex<()>` — serializes collection creation. |
| `collection_hw_metrics` | `DashMap<CollectionId, Arc<HwSharedDrain>>` — hardware usage aggregation per collection (and aliases keyed by collection id). |
| `telemetry` | `TocTelemetryCollector` — snapshot-related telemetry for collections. |

### Loading collections from disk

1. Ensures `storage_path/collections/` (constant [`COLLECTIONS_DIR`](../../../lib/storage/src/content_manager/toc/mod.rs)) exists; optional `temp_path` directory.
2. Reads the collections directory; for each subdirectory, checks [`CollectionConfigInternal::check`](../../../lib/collection/) — skips entries without a valid config.
3. Derives **collection name** from the directory name (UTF-8).
4. Builds one async task per collection calling [`Collection::load`](../../../lib/collection/) with paths, shared storage config, `channel_service`, peer id, optional consensus callbacks (`change_peer_from_state`, `request_shard_transfer`, `abort_shard_transfer`), **search/update runtime handles**, and `optimizer_resource_budget`.
5. Runs those tasks with **bounded concurrency** from `storage_config.performance.load_concurrency`, driven on **`general_runtime`** via `buffer_unordered` + `block_on`, then inserts into the `collections` map.
6. Initializes per-collection snapshot telemetry for each loaded name.
7. Opens **aliases** via [`AliasPersistence::open`](../../../lib/storage/src/content_manager/alias_mapping.rs) at `storage_path/aliases` ([`ALIASES_PATH`](../../../lib/storage/src/content_manager/toc/mod.rs)).

### Alias resolution

- Internal fetch path [`get_collection_unchecked`](../../../lib/storage/src/content_manager/toc/mod.rs) holds a read lock on `collections`, then a read lock on `alias_persistence`, and calls [`resolve_name`](../../../lib/storage/src/content_manager/toc/mod.rs).
- [`resolve_name`](../../../lib/storage/src/content_manager/toc/mod.rs): looks up the input string in `AliasPersistence::get`; if an alias exists, uses the target collection name; otherwise uses the original string. Then [`validate_collection_exists`](../../../lib/storage/src/content_manager/collections_ops.rs) on the resolved name — failure if neither a valid collection nor resolvable alias.
- Public [`get_collection`](../../../lib/storage/src/content_manager/toc/mod.rs) applies RBAC via `CollectionPass`; listing aliases uses `alias_persistence` plus access checks where applicable.

### Applying meta-operations (implementation entry)

The async [`TableOfContent::perform_collection_meta_op`](../../../lib/storage/src/content_manager/toc/collection_meta_ops.rs) matches on [`CollectionMetaOperations`](../../../lib/storage/src/content_manager/collection_meta_ops.rs) and delegates to `create_collection`, `update_collection`, `delete_collection`, `update_aliases`, `handle_resharding`, `handle_transfer`, `set_shard_replica_state`, shard key and payload index helpers, etc. A synchronous wrapper [`perform_collection_meta_op_sync`](../../../lib/storage/src/content_manager/toc/collection_meta_ops.rs) uses `general_runtime.block_on`.

---

## Dispatcher

[`Dispatcher`](../../../lib/storage/src/dispatcher.rs) is a small façade over the service layer.

| Component | Role |
|-----------|------|
| `toc: Arc<TableOfContent>` | Shared table of content for all handlers. |
| `consensus_state: Option<ConsensusStateRef>` | Present in cluster mode; absent in standalone. |
| `resharding_enabled` | Flag passed through `with_consensus` for feature gating. |

**[`submit_collection_meta_op`](../../../lib/storage/src/dispatcher.rs):**

1. Validates auth via [`Auth::check_collection_meta_operation`](../../../lib/storage/src/rbac/).
2. **With consensus** (`consensus_state` is `Some`): may rewrite `CreateCollection` (shard distribution for `Auto`, empty distribution for `Custom`, fresh UUID), register awaiters for replica activation, wrap as [`ConsensusOperations::CollectionMeta`](../../../lib/storage/src/content_manager/mod.rs), call [`propose_consensus_op_with_await`](../../../lib/storage/src/content_manager/consensus_manager.rs), optionally wait for follow-up consensus ops, shard-key activation, and cluster sync (`await_commit_on_all_peers`) for specific operations.
3. **Without consensus:** clones `toc` and `spawn`s `perform_collection_meta_op` on the runtime, awaiting the join handle (operation runs to completion on standalone).

**[`cluster_status`](../../../lib/storage/src/dispatcher.rs):** delegates to `ConsensusStateRef::cluster_status()` when consensus is enabled, else [`ClusterStatus::Disabled`](../../../lib/storage/src/types.rs).

Other helpers: `toc()` (with auth + verification pass), `await_consensus_sync`, `wait_for_shard_key_activation`, hardware metrics passthrough to ToC.

---

## ConsensusManager

Defined in [`content_manager/consensus_manager.rs`](../../../lib/storage/src/content_manager/consensus_manager.rs). Generic over `C: CollectionContainer`; in production, `C` is [`TableOfContent`](../../../lib/storage/src/content_manager/toc/mod.rs) ([`prelude::ConsensusState`](../../../lib/storage/src/content_manager/consensus_manager.rs) = `ConsensusManager<TableOfContent>`).

### Main pieces

| Field | Role |
|-------|------|
| `persistent` | `RwLock<Persistent>` — durable Raft hard state, peer addresses, metadata, applied index tracking. |
| `is_leader_established` | `Arc<IsReady>` — proposals are rejected until leader is known. |
| `wal` | `Mutex<ConsensusOpWal>` — log entries for apply loop. |
| `soft_state` | `RwLock<Option<SoftState>>` — non-persisted Raft soft state (role, leader). |
| `toc` | `Arc<C>` — applies user-visible state changes (`perform_collection_meta_op`, snapshots). |
| `on_consensus_op_apply` | `Mutex<HashMap<ConsensusOperations, broadcast::Sender<Result<bool, StorageError>>>>` — waiters for “this op applied locally”. |
| `propose_sender` | `OperationSender` — sends to the **external** consensus thread (see crate-level consensus driver, e.g. `src/consensus.rs` in the main binary). |
| `consensus_thread_status` | `RwLock<ConsensusThreadStatus>` — health of the consensus thread. |

**Raft `Storage`:** [`ConsensusStateRef`](../../../lib/storage/src/content_manager/consensus_manager.rs) (`Arc<ConsensusState>`) implements [`raft::Storage`](https://docs.rs/raft/) by forwarding to the inner `ConsensusManager` (`initial_state`, `entries`, `term`, `first_index`/`last_index`, `snapshot`).

### Proposing and committing

- API paths call [`propose_consensus_op_with_await`](../../../lib/storage/src/content_manager/consensus_manager.rs): wait for leader, register (or subscribe to) an `on_consensus_op_apply` entry, send the operation through `propose_sender`, then await the broadcast result with a timeout.
- The consensus thread appends/replicates per Raft; committed entries are pulled from the WAL.

### Applying entries

- [`apply_entries`](../../../lib/storage/src/content_manager/consensus_manager.rs) loops over **unapplied** indices from `persistent`, loads each entry from the WAL, then:
  - **EntryNormal:** [`apply_normal_entry`](../../../lib/storage/src/content_manager/consensus_manager.rs).
  - **EntryConfChangeV2:** [`apply_conf_change_entry`](../../../lib/storage/src/content_manager/consensus_manager.rs) (membership changes).
- **[`apply_normal_entry`](../../../lib/storage/src/content_manager/consensus_manager.rs):** deserializes CBOR payload to [`ConsensusOperations`](../../../lib/storage/src/content_manager/mod.rs). For `CollectionMeta`, calls **`toc.perform_collection_meta_op(*operation)`** (sync path on the ToC). Other variants update `Persistent` (peer metadata, cluster metadata). `RequestSnapshot` / `ReportSnapshot` are not handled here (`unreachable!()` in this arm). Notifies any registered `on_apply` sender with the `Result`.

**Snapshots:** [`apply_snapshot`](../../../lib/storage/src/content_manager/consensus_manager.rs) decodes [`SnapshotData`](../../../lib/storage/src/content_manager/consensus_manager.rs) (collections state + aliases + addresses + metadata), applies `collections_data` via `toc.apply_collections_snapshot`, updates `persistent`, clears WAL as documented in-code.

---

## CollectionMetaOperations

Enum in [`content_manager/collection_meta_ops.rs`](../../../lib/storage/src/content_manager/collection_meta_ops.rs) — the cluster-level / catalog operations applied through ToC (and, in distributed mode, wrapped in `ConsensusOperations::CollectionMeta`).

| Variant | Meaning |
|---------|---------|
| `CreateCollection(CreateCollectionOperation)` | New collection + config + optional shard distribution proposal. |
| `UpdateCollection(UpdateCollectionOperation)` | Vectors, HNSW, optimizers, quantization, sparse vectors, strict mode, metadata, **shard replica changes** (add/remove), etc. |
| `DeleteCollection(DeleteCollectionOperation)` | Remove collection and data. |
| `ChangeAliases(ChangeAliasesOperation)` | Batch of [`AliasOperations`](../../../lib/storage/src/content_manager/collection_meta_ops.rs): create, delete, rename alias. |
| `Resharding(CollectionId, ReshardingOperation)` | Resharding lifecycle: `Start`, `CommitRead`, `CommitWrite`, `Finish`, `Abort` ([`ReshardKey`](../../../lib/collection/)). |
| `TransferShard(CollectionId, ShardTransferOperations)` | `Start`, `Restart`, `Finish`, `SnapshotRecovered`, `RecoveryToPartial`, `Abort` — shard movement between peers. |
| `SetShardReplicaState(SetShardReplicaState)` | Set replica state for a shard on a peer (e.g. init → active). |
| `CreateShardKey` / `DropShardKey` | User-defined shard keys (custom sharding). |
| `CreatePayloadIndex` / `DropPayloadIndex` | Payload field indexes at collection level. |
| `Nop { token }` | No-op for synchronization / testing. |
| `TestSlowDown` | **Feature `staging`**: artificial delay on a peer. |

Helper types in the same file include `CreateCollection`, `UpdateCollection`, `ShardTransferOperations`, `ReshardingOperation`, and alias structs used by REST/OpenAPI.

---

## Content Manager Structure

Directory [`lib/storage/src/content_manager/`](../../../lib/storage/src/content_manager/):

| Area | Contents |
|------|----------|
| **`toc/`** | [`TableOfContent`](../../../lib/storage/src/content_manager/toc/mod.rs), loading collections, point ops, transfers, snapshots, telemetry, `CollectionContainer` impl. |
| **`consensus/`** | WAL, persistent Raft state, entry queue, `OperationSender` — **infrastructure** for consensus, not the full Raft loop (that lives in the binary). |
| **`consensus_manager.rs`** | [`ConsensusManager`](../../../lib/storage/src/content_manager/consensus_manager.rs) + `ConsensusStateRef` + snapshot types; applies committed log entries to ToC. |
| **`snapshots/`** | Cluster/full snapshot download and recovery ([`snapshots/mod.rs`](../../../lib/storage/src/content_manager/snapshots/mod.rs)). |
| **`errors.rs`** | [`StorageError`](../../../lib/storage/src/content_manager/errors.rs) and `StorageResult`. |
| **Other** | `alias_mapping`, `collections_ops`, `collection_meta_ops`, `collection_verification`, `conversions`, `shard_distribution`, optional `staging`. |

---

## Cross-References

| Related area | Where to read next |
|--------------|-------------------|
| Distributed collection logic | [`collection` crate](./collection.md) — `Collection`, `ShardReplicaSet`, `ChannelService`, transfers, resharding. |
| On-disk vectors / indexes | [`segment` crate](./segment.md). |
| Consensus thread / RawNode wiring | Application crate (e.g. `src/consensus.rs` in the main Qdrant binary), which owns the Raft tick loop and uses `ConsensusStateRef` as `Storage`. |
| API → storage | REST/gRPC layers construct `CollectionMetaOperations` / call `Dispatcher` methods. |
| RBAC | [`lib/storage/src/rbac/`](../../../lib/storage/src/rbac/mod.rs). |
