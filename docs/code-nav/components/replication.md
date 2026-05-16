# Shard replication (replica sets and transfers)

Code navigation for **data redundancy** across peers: how `ShardReplicaSet` fans out writes, prefers local reads, runs shard transfers, and uses WAL deltas and clocks for recovery. **Cluster metadata** (collections, transfers in consensus, etc.) is covered in [consensus.md](./consensus.md).

---

## Overview

**Replication** keeps redundant copies of each shard on multiple peers. Each logical shard is represented by a **`ShardReplicaSet`**: a **local** shard (`Shard`, often `LocalShard`, or a **proxy** during transfer) plus zero or more **`RemoteShard`** handles.

**Updates** are sent to every **updatable** replica (subject to replica state and `write_consistency_factor`). **Reads** prefer the **local** replica when it is ready and readable, and can fan out to remotes according to **read consistency** and **`read_fan_out_factor`**.

---

## ShardReplicaSet

Defined in `lib/collection/src/shards/replica_set/mod.rs`.

| Field / concept | Role |
|-----------------|------|
| **`local`** | `RwLock<Option<Shard>>` — local shard or proxy during replication/transfer. |
| **`remotes`** | `RwLock<Vec<RemoteShard>>` — gRPC-backed remotes. |
| **`replica_state`** | `SaveOnDisk<ReplicaSetState>` — persisted peer → **`ReplicaState`** map. |
| **`locally_disabled_peers`** | Peers marked dead locally before consensus catches up; excluded from updates/reads. |
| **`clock_set`** | `Mutex<ClockSet>` — assigns **clock tags** for ordering writes from this peer. |
| **`write_ordering_lock`** | Serializes writes when **strong/medium** `WriteOrdering` is used. |

**Replica states** (see `replica_set_state.rs` and the state diagram in `mod.rs`) include among others **`Initializing`**, **`Active`**, **`Listener`** (incoming replica catching up), **`Partial`** / **`PartialSnapshot`** / **`Recovery`**, **`Dead`**, **`ManualRecovery`**, **`Resharding`** / **`ReshardingScaleDown`**, **`ActiveRead`**, etc. States control whether local/remote **update** and **read** paths run and which **`WaitUntil`** mode applies (e.g. **`Listener`** uses **`WaitUntil::Wal`** on the local path in `update.rs`).

---

## Update replication

Primary entry points: **`update_with_consistency`** → leader selection / forward vs local **`update`** → **`update_impl`** (`lib/collection/src/shards/replica_set/update.rs`).

**Clock tags**

- The leader side acquires a **`ClockGuard`** from **`get_clock`** and builds an **`OperationWithClockTag`** with a **`ClockTag`** (peer id, clock id, tick).
- Replicas echo clock information in **`UpdateResult`**; the coordinator may **`advance_to`** a newer tick if remotes report a higher tick for the same clock.

**Fan-out**

- **`update_impl`** collects **updatable** local + remote shards (`is_peer_updatable`), clones the tagged operation, and runs local and remote **`Shard::update`** futures (optionally **`buffer_unordered`** if `shared_storage_config.update_concurrency` is set).
- Success is judged against **`write_consistency_factor`** (from collection params, capped by replica count): at least **`minimal_success_count`** successes are required. Partial failures can trigger **peer deactivation** callbacks toward consensus when appropriate.

**`WaitUntil`**

- Passed through to each shard’s update (e.g. wait for indexing vs WAL durability). **`ReplicaState::Listener`** forces **`WaitUntil::Wal`** for the **local** leg even if the client requested a stronger wait (`update_impl`).

**Write ordering**

- **`WriteOrdering::Weak`**: this peer coordinates locally.
- **`Medium` / `Strong`**: **`leader_peer_for_update`** picks a designated peer (highest alive or highest replica id); non-leaders **`forward_update`** to that peer.

**Retries**

- If updates return **clock rejected**, **`update`** retries up to **`UPDATE_MAX_CLOCK_REJECTED_RETRIES`** with a fresh tick.

---

## Read path

Implemented around **`execute_read_operation`**, **`execute_and_resolve_read_operation`**, and **`execute_cluster_read_operation`** (`lib/collection/src/shards/replica_set/execute_read_operation.rs`). Thin wrappers in **`read_ops.rs`** (scroll, search, count, etc.) delegate to these.

**Prefer local**

- For cluster reads, the code **`try_read`s** the local shard; if the local peer is **readable** and not busy with an update, a **local** read is scheduled first.
- **Default fan-out**: **`read_fan_out_factor`** defaults to **0** when local is ready (local only unless consistency requires more); defaults to **1** if local is missing or not ready — so remotes are used as fallback.

**Read consistency**

- **`ReadConsistency`** (`All`, `Majority`, `Quorum`, or numeric **factor**) maps to how many successful replica responses are required and how **`Res::resolve`** merges them (`ResolveCondition::All` vs `Majority`).

**`local_only`**

- Skips fan-out and uses **`execute_local_read_operation`** only.

Read failures on individual peers are handled locally; the comment in **`execute_read_operation`** notes that **failing peer ids are not reported to consensus** (unlike some update failure paths).

---

## Shard transfers

Core types: **`ShardTransfer`**, **`ShardTransferMethod`**, **`TransferStage`** (`lib/collection/src/shards/transfer/mod.rs`). The driver selects implementation by method (`transfer/driver.rs`).

**Transfer methods** (`ShardTransferMethod`)

| Variant | Meaning (from in-code docs) |
|---------|-----------------------------|
| **`StreamRecords`** | Stream all shard records in batches until complete. |
| **`Snapshot`** | Snapshot shard, transfer archive, restore on receiver. |
| **`WalDelta`** | Transfer difference via **WAL delta** when possible. |
| **`ReshardingStreamRecords`** | Resharding-specific streaming until points are moved. |

**Stages**

- **`TransferStage`** includes **`Proxifying`**, **`Plunging`** (drain queued updates), **`CreatingSnapshot`**, **`Transferring`**, **`Recovering`**, **`FlushingQueue`**, **`WaitingConsensus`**, **`Finalizing`** — reflecting proxy setup, data movement, recovery, queued updates, consensus sync, and teardown.

**Proxy shards**

- During transfer, the **local** slot may hold a **proxy** shard (`Shard` abstraction) so updates can be forwarded while data is copied; **`ShardReplicaSet`** is built to allow that (`mod.rs` comments).

**Consensus**

- **`ShardTransferConsensus`** trait in `transfer/mod.rs` abstracts proposing **start/abort** and state changes to cluster consensus (actual proposals are `ConsensusOperations` — see [consensus.md](./consensus.md)).

---

## Recovery

**Recoverable WAL** (`lib/collection/src/wal_delta.rs`, struct **`RecoverableWal`**)

- Wraps a locked **`SerdeWal<OperationWithClockTag>`** plus **`ClockMap`** instances: **`newest_clocks`** (high water marks per peer/clock) and **`oldest_clocks`** (cutoff: ticks **below** mapped values are not recoverable from this WAL).
- **`lock_and_write`** advances/corrects the operation’s **`ClockTag`** via **`advance_clock_and_correct_tag`** before appending; rejects with **`WalError::ClockRejected`** if the clock is stale.

**Recovery points**

- **`RecoveryPoint`** (`lib/collection/src/shards/local_shard/clock_map.rs`) captures clock positions used when exchanging **WAL delta** / recovery over gRPC (see also **`RecoveryPointClockTag`** in API types). **`ClockMap::to_recovery_point`** builds a point from current maps.
- Comments in **`wal_delta.rs`** describe taking a **clock snapshot** when deactivating a replica so **WAL delta** recovery does not skip operations after aborted transfers.

**WAL delta transfer**

- Module **`transfer/wal_delta.rs`** implements the **WalDelta** method on top of these primitives (delta computation and application between peers).

---

## Cross-references

| Topic | Location |
|-------|----------|
| Replica set struct and state diagram | `qdrant/lib/collection/src/shards/replica_set/mod.rs` |
| Update fan-out, clocks, write factor | `qdrant/lib/collection/src/shards/replica_set/update.rs` |
| Read routing and consistency | `qdrant/lib/collection/src/shards/replica_set/execute_read_operation.rs`, `read_ops.rs` |
| Replica state enum | `qdrant/lib/collection/src/shards/replica_set/replica_set_state.rs` |
| Clock sets / tags | `qdrant/lib/collection/src/shards/replica_set/clock_set.rs`, `qdrant/lib/collection/src/operations/` (`ClockTag`, `OperationWithClockTag`) |
| Transfer types and driver | `qdrant/lib/collection/src/shards/transfer/mod.rs`, `driver.rs` |
| Recoverable WAL + deltas | `qdrant/lib/collection/src/wal_delta.rs`, `transfer/wal_delta.rs` |
| Clock maps / recovery points | `qdrant/lib/collection/src/shards/local_shard/clock_map.rs` |
| Raft metadata (not point replication) | [consensus.md](./consensus.md) |
