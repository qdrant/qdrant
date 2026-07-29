# Collection create: execution path

## Overview

Creating a collection allocates **on-disk layout** (collection path, snapshots path derivation), builds a resolved `CollectionConfigInternal`, instantiates **`Collection::new`** (shard holder, per-shard `ShardReplicaSet`, optimizers budget, runtimes), and registers the `Arc<Collection>` in the table of contents. In a **cluster**, the same logical operation is proposed through **consensus** so every node eventually applies `TableOfContent::perform_collection_meta_op` with a agreed **`CollectionShardDistribution`**; in **standalone** mode the dispatcher runs that path directly on the local TOC without Raft.

## Creation Path

### Standalone

When `Dispatcher` is constructed without `consensus_state` (`lib/storage/src/dispatcher.rs`), `submit_collection_meta_op` skips the cluster branch and runs:

- `tokio::task::spawn` → `TableOfContent::perform_collection_meta_op(operation)` (`lib/storage/src/content_manager/toc/collection_meta_ops.rs`).

For `CollectionMetaOperations::CreateCollection`, if no distribution was attached, **auto sharding** uses `CollectionShardDistribution::all_local(shard_number, this_peer_id)` with `number_of_peers = 1`; **custom** sharding starts from an empty `ShardDistributionProposal`. Then `TableOfContent::create_collection` runs (`lib/storage/src/content_manager/toc/create_collection.rs`).

The API layer obtains a `Dispatcher`, checks auth / strict mode as elsewhere, and builds `CollectionMetaOperations::CreateCollection` (Actix/gRPC paths mirror this pattern; dispatcher is the fan-in for meta-ops).

### Cluster

With consensus enabled (`Dispatcher::with_consensus`):

1. `submit_collection_meta_op` may call `toc.suggest_shard_distribution` for **auto** sharding when the client did not supply a distribution: uses **peer count** from consensus state, default/suggested shard counts from `CollectionConfigDefaults`, replication factor defaults/overrides, and **known peers** from `channel_service.id_to_address` plus `this_peer_id` (`TableOfContent::suggest_shard_distribution`, `lib/storage/src/content_manager/toc/mod.rs`).
2. For auto sharding it may register **expected** `ConsensusOperations::initialize_replica` entries per `(shard_id, peer_id)` to await replica activation after commit.
3. It wraps the op in `ConsensusOperations::CollectionMeta` and calls `propose_consensus_op_with_await` on the consensus state (Raft propose + wait for local apply).
4. After commit, **`ConsensusManager::apply_normal_entry`** handles `ConsensusOperations::CollectionMeta` by calling **`self.toc.perform_collection_meta_op(*operation)`** synchronously (`lib/storage/src/content_manager/consensus_manager.rs`) — so each node executes the same `create_collection` branch as standalone once the entry is applied.
5. Optional **post-commit** waits: await registered replica-initialization operations, shard-key activation rules, and `await_consensus_sync` after create so peers align on metadata before serving point traffic (`dispatcher.rs`).

## Collection::new

`Collection::new` (`lib/collection/src/collection/mod.rs`) runs immediately after the TOC has created the collection directory and computed config:

- Constructs `ShardHolder` at the collection path and applies optional **shard key mapping**.
- Loads or initializes **payload index schema** (`load_payload_index_schema`).
- Wraps config in `Arc<RwLock<…>>` and, for each `(shard_id, peers)` in `CollectionShardDistribution::shards`, determines whether **this peer** hosts a local replica (`peers.remove(&this_peer_id)`).
- Merges **optimizer config** with optional `optimizers_overwrite` from storage defaults.
- Builds **`ShardReplicaSet::build`** with shard key (if any), callbacks for replica failure / shard transfer / abort transfer, shared config, WAL/optimizer settings, channel service, and update/search runtimes, then **`shard_holder.add_shard`**.
- Persists **collection version** and **`collection_config.save(path)`** — after this, creation is considered durable on disk.
- Returns `Collection` with transfer pool, telemetry hooks, and resource budget; TOC then inserts it into the in-memory map and initializes snapshot telemetry.

`Collection::load` (same file) is the restart path: read version, migrate if allowed, load config, rebuild shard holder from disk — not the create path, but the counterpart for cold start.

## Shard Distribution

- **`ShardDistributionProposal::new`** (used from `suggest_shard_distribution`) takes shard count, replication factor, and the peer list to produce a mapping `shard_id → Vec<PeerId>` (even spread across known peers — see debug log in `toc/mod.rs`).
- **Standalone auto** — `CollectionShardDistribution::all_local` places every shard on `this_peer_id`.
- **Custom sharding** — Initial create may use an empty proposal; shards appear later when shard keys are added (see `collection_meta_ops.rs` and related consensus paths).
- **Explicit distribution** — If the meta-op already carries a distribution (e.g. from tests or advanced callers), `perform_collection_meta_op` uses it as-is (`take_distribution` / `into()`).

## Cross-References

- **Dispatcher** — `lib/storage/src/dispatcher.rs` (`submit_collection_meta_op`, consensus vs standalone branch, `suggest_shard_distribution` call site).
- **TOC meta-op router** — `lib/storage/src/content_manager/toc/collection_meta_ops.rs` (`perform_collection_meta_op`, `CreateCollection` arm).
- **TOC create implementation** — `lib/storage/src/content_manager/toc/create_collection.rs` (validation, path creation, config assembly, `Collection::new`, registry insert).
- **Consensus apply** — `lib/storage/src/content_manager/consensus_manager.rs` (`apply_normal_entry` → `CollectionMeta` → `perform_collection_meta_op`).
- **Collection type** — `lib/collection/src/collection/mod.rs` (`Collection::new`, `Collection::load`, shard holder and replica set wiring).
- **Shard distribution proposal** — `lib/storage/src/content_manager/shard_distribution.rs` (proposal type; referenced from TOC).
- **Replica set construction** — `lib/collection/src/shards/replica_set/` (build + state machine; creation is invoked from `Collection::new`).
