# Raft consensus (cluster metadata)

Code navigation for **Raft-based cluster coordination**: collection metadata, topology, and related control-plane operations. This is separate from **point-level replication**, which uses direct fan-out between replicas.

---

## Overview

Qdrant uses **Raft consensus** (via `raft::RawNode`) for **cluster coordination**. Operations such as collection create/delete, alias changes, shard transfers, replica state changes, and peer membership are serialized as `ConsensusOperations`, proposed to Raft, committed in log order, and then applied to local storage state.

**Point updates** (vectors, payloads, deletes, etc.) are **not** committed through Raft. They are applied on shards and replicated **directly** from the coordinating replica set to other replicas (see [replication.md](./replication.md)).

---

## Architecture

| Piece | Role |
|-------|------|
| **`ConsensusManager`** (`lib/storage/src/content_manager/consensus_manager.rs`) | Implements `raft::Storage` for `RawNode`. Owns persistent Raft state, soft state, the consensus WAL, hooks into `TableOfContent`, and exposes `propose_consensus_op_with_await`. |
| **`ConsensusStateRef`** | Type alias / handle to the storage backing `RawNode` (see `consensus_manager::prelude::ConsensusState`). |
| **Consensus thread** (`src/consensus.rs`, struct `Consensus`) | Dedicated OS thread running the **`raft::RawNode`** loop: receives `Message::FromClient` / `Message::FromPeer`, calls `step` / `propose` / `ready`, applies committed entries and snapshots. |
| **`OperationSender`** (`lib/storage/src/content_manager/consensus/operation_sender.rs`) | Thin mutex around `std::sync::mpsc::Sender<ConsensusOperations>` used from async/storage code to enqueue proposals. |
| **Forward-proposals thread** (`src/consensus.rs`, `Consensus::run`) | Blocks on the std **mpsc** receiver and forwards each `ConsensusOperations` into the consensus thread’s **tokio** channel as `Message::FromClient`. |
| **`Persistent`** (`lib/storage/src/content_manager/consensus/persistent.rs`) | On-disk **Raft state** between restarts: `RaftState` (hard + conf state), snapshot metadata, apply-progress queue, peer addresses, peer metadata, cluster metadata JSON, `this_peer_id`. |
| **`ConsensusOpWal`** (`lib/storage/src/content_manager/consensus/consensus_wal.rs`) | **Consensus (meta) WAL** under `collections_meta_wal/`: stores Raft log entries for recovery; tracks compaction by Raft index. Distinct from per-shard point WALs. |

Supporting pieces include **P2P Raft messaging** (gRPC `RaftClient`, `RaftMessageBroker` in `src/consensus.rs`), **`is_leader_established`** (`IsReady`) gating proposals until leadership is stable, and **`on_consensus_op_apply`** (broadcast map) to correlate a proposed op with apply completion on this peer.

---

## Proposal Flow

1. **Caller** (e.g. dispatcher, TOC transfer path) builds a `ConsensusOperations` variant and typically calls `ConsensusManager::propose_consensus_op_with_await` (`consensus_manager.rs`).
2. **Leader gate**: the method waits (with timeout) until `is_leader_established` reports ready; otherwise it errors (e.g. no leader elected in time).
3. **De-duplication / await**: it registers the operation in `on_consensus_op_apply` (reusing an in-flight broadcast sender if the same op is already proposed), then **`OperationSender::send`** pushes the op onto the std **mpsc** channel.
4. **Forward thread** reads from mpsc and **`blocking_send`**s `Message::FromClient(operation)` to the consensus thread’s async receiver.
5. **Consensus thread** (`advance_node_impl` in `src/consensus.rs`):
   - **Conf changes** (`AddPeer`, `RemovePeer`) → `RawNode::propose_conf_change`.
   - **Snapshot control** → `request_snapshot` / `report_snapshot`.
   - **Other meta ops** → CBOR-serialize `ConsensusOperations` and `RawNode::propose` a normal log entry.
   - **Peer messages** → `RawNode::step`.
6. **Raft** replicates; on **ready**, the storage implementation **appends** entries to the consensus WAL and updates persistent state as appropriate.
7. **Committed entries** are **applied** (e.g. via `apply_entries`), mutating collection/cluster state through `TableOfContent` and related paths; successful apply notifies waiters on `on_consensus_op_apply`.

On startup, **`apply_entries`** may run before the main loop to drain **already-committed** entries that were not yet applied (`src/consensus.rs`).

---

## Standalone vs cluster

- In a **cluster**, the process wires an **`OperationSender`** into `ConsensusManager` and starts the **consensus** + **forward-proposals** threads. Meta changes go through Raft as above.
- In **standalone** (single-node) mode, **`consensus_proposal_sender`** on `TableOfContent` is **`None`**. Callers that require consensus return **`StorageError`**: e.g. `get_consensus_proposal_sender()` → *"Qdrant is running in standalone mode"* (`toc/mod.rs`). Operations that need **`propose_consensus_op_with_await`** (such as some cluster metadata updates with `wait: true`) also require a registered **`TocDispatcher`** with consensus access; without it, the same standalone error path applies.
- Callbacks that would **propose** replica deactivation, shard transfers, or aborts **log errors** when `proposal_sender` is missing (*"single node deployment"*) instead of sending Raft messages (`toc/mod.rs`).

Point ingestion and local shard updates still work in standalone; only **distributed consensus** and **cluster-wide** coordination APIs are absent or error.

---

## Cross-references

| Topic | Location |
|-------|----------|
| Consensus thread + `RawNode` loop | `qdrant/src/consensus.rs` |
| Raft storage, propose/apply, WAL integration | `qdrant/lib/storage/src/content_manager/consensus_manager.rs` |
| Enqueue proposals from storage layer | `qdrant/lib/storage/src/content_manager/consensus/operation_sender.rs` |
| Persisted Raft + topology | `qdrant/lib/storage/src/content_manager/consensus/persistent.rs` |
| Meta-operation WAL | `qdrant/lib/storage/src/content_manager/consensus/consensus_wal.rs` |
| Consensus op enum | `qdrant/lib/storage/src/content_manager/consensus_ops.rs` |
| TOC integration, standalone guards | `qdrant/lib/storage/src/content_manager/toc/mod.rs` |
| Point replication (not Raft) | [replication.md](./replication.md), `qdrant/lib/collection/src/shards/replica_set/` |
