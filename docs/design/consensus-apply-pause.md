# Design: Pause Consensus Apply on Disk-Capacity Errors (R2)

| | |
|---|---|
| Status | Draft proposal, design only. **Not agreed. No test in Section 6 has been written or run.** B2 and B3 are open |
| Scope | R2 only: keep Raft running when a committed entry cannot be applied because the disk is full. Reserved consensus storage (R1) is a separate proposal, [consensus-disk-reservation.md](consensus-disk-reservation.md). R2 does not depend on it |
| Code revisions | dev `23320c2c88e8026261c9e0fc2d3ece1b98e0854a` (1.20.1-dev); incident build `eabee371` (1.17.1); raft-rs `aafb07c7bab439c6139926a77dfafc5b10e9bc84` |
| Incident | 2026-10-15/16, shared data volume ran out of **data blocks** (user-confirmed; inodes were not exhausted). Timeline in the R1 document, Section 2 |

## 1. Summary

In the incident, pods `...-36` and `...-38` logged `Consensus stopped with error` with the collection disk-guard text `No space left on device: WAL buffer size exceeds available disk space`. That guard fires when free space drops below `2 × wal_capacity_mb` (64 MiB with defaults, `local_shard/mod.rs:277-283`, `shard_ops.rs:83-96`). Consensus stopped because `apply_entries_impl` returns `Err` on any `ServiceError` from a meta-operation (`consensus_manager.rs:448-452`). Then the Raft instance is dropped. At `eabee371` the thread exits for good (`src/consensus.rs:110-118`). On dev it rebuilds with backoff and retries the same entry (`src/consensus.rs:149-222`), so it keeps failing while the disk stays full.

**Proposal.** When a committed entry fails to apply because of disk capacity, and its operation is known to be retry-safe, the node pauses the state-machine apply cursor at that entry instead of stopping Raft. It keeps persisting the log and hard state, voting and acknowledging appends. It retries the apply with backoff. No proposer is told that the entry failed or succeeded until it actually applies.

**What R2 provides and what it does not.**

- It keeps a node in Raft while the consensus WAL and the Raft state file can still be written. Above zero free bytes that is normally the case, because a state save is a few KB. At exactly zero free bytes those writes can fail too. Without R1 the node then stops, as today.
- It does not make the meta-operation succeed. The paused node's state machine falls behind until space returns.
- Free space on `...-36` and `...-38` at 17:38 is not in the evidence, so we cannot show that consensus writes would have succeeded on those pods. R2 addresses the failure the logs do show.

## 2. Apply Failure Classes

| Class | Today | Proposed |
|---|---|---|
| User error | logged, entry counted as applied (`consensus_manager.rs:454-461`) | unchanged |
| **Capacity error** (collection disk guard, `ENOSPC`, `EDQUOT`) from a retry-safe operation | `Err`, Raft stops | **pause** |
| Capacity error from an operation not shown retry-safe | `Err`, Raft stops | unchanged until B2 covers it |
| Error saving `Persistent` or appending to the consensus WAL | `Err` | unchanged (R1 covers this) |
| Any other service error | `Err`, Raft stops | unchanged |

The decision needs a typed error variant for disk capacity. Today the guard returns a generic `service_error` string (`shard_ops.rs:90-92`), and the design must not match on message text.

## 3. Design

### 3.1 Holding Both Apply Cursors

There are two applied cursors, and both must stay at the failed entry:

- **Qdrant's apply queue** (`Persistent::apply_progress_queue`, persisted). `apply_entries_impl` reads entries from the WAL by this index (`consensus_manager.rs:417-428`), not from the `Ready`.
- **raft-rs's applied index.** Today `process_ready` calls `advance(ready)` (`src/consensus.rs:1144`) and `process_light_ready` calls `advance_apply()` (`:1187`) unconditionally. Both move raft-rs's applied index to the last entry *delivered* (`raw_node.rs:663-668, 711-714`). That is correct today only because every apply failure drops the `RawNode`.

raft-rs reads its applied index in `has_pending_conf` (`raft.rs:2788-2791`), in the pre-campaign check for unapplied conf changes (`raft.rs:1548-1567`), and in the joint-consensus auto-leave in `commit_apply` (`raft.rs:960-1004`). If a paused apply returned an ordinary `Ok`, a leader could accept a second conf change while the first is unapplied locally, and auto-leave could fire early. **A paused apply must not reach `advance` or `advance_apply`.**

Changes in `src/consensus.rs`:

1. `apply_entries` returns a `#[must_use]` outcome, for example `ApplyOutcome { stop_consensus, paused: Option<PauseReason> }`, instead of `anyhow::Result<bool>`.
2. `process_ready` calls `advance_append(ready)` and then `advance_apply_to(last_applied_entry)`. raft-rs documents this split (`raw_node.rs:654-700`). `advance_apply_to` accepts any index in `[applied, committed]` and calls `fatal!` outside it (`raft_log.rs:322-339`).
3. `process_light_ready` calls the same `advance_apply_to(last_applied_entry)`.
4. When nothing is paused, `last_applied_entry` equals the last delivered index, so behavior is unchanged.
5. While paused, a timer in the consensus loop retries the apply with backoff even when no new entries arrive, then calls `advance_apply_to`.
6. Startup already creates the node with `applied = last_applied_entry` (`src/consensus.rs:367-385`). If apply was paused at shutdown, raft-rs redelivers the unapplied range, and Section 3.2 absorbs the overlap.

### 3.2 Apply Queue Extension

`set_unapplied_entries` replaces the queue with the delivered batch (`persistent.rs:233-240`, `entry_queue.rs:8-11`). Under a pause, the next batch would overwrite the paused range and skip entries silently. It is replaced by `extend_unapplied_entries(first, last)`. With the current queue `[cur, q_last]`:

| Case | Result |
|---|---|
| No queue, or queue fully applied | `[first, last]`; require `first == q_last + 1` when a queue existed |
| `cur ≤ first ≤ q_last + 1` | `[cur, max(q_last, last)]` |
| `first < cur` (redelivery) | `[cur, max(q_last, last)]`; nothing re-applied |
| `first > q_last + 1` (gap) | `Err`, never skip; today's stop and rebuild |
| Snapshot install, reinit | unchanged (`entry_queue.rs:41-43`; `clear_unapplied_entries_on_reinit`) |

Compaction stays limited to `last_applied_entry` (`consensus_manager.rs:1109-1136`), so every queued entry is still in the WAL when it is applied.

### 3.3 Partial Effects, Shadow, Waiters

- **Retry safety (B2).** A handler may persist part of an operation and then return an error. The comment at `consensus_manager.rs:390-398` says so. An operation is pause-eligible only if applying it again, after any prefix of its effects was persisted, gives the same final state as applying it once. Each `ConsensusOperations` and `CollectionMetaOperations` variant needs that audit. Today's dev rebuild already retries the same entry in the same process, so the exposure exists today. This design does not widen it to unaudited variants.
- **Reload before retry.** If a variant's partial effects live in memory (for example collection config in `TableOfContent`), the audit must say what is reloaded before the retry. A variant that needs a process restart for that is not pause-eligible.
- **Shadow state machine.** On every pause, call `invalidate_shadow()` exactly as the `Err` path does (`consensus_manager.rs:387-402`).
- **Proposer waiters.** `apply_normal_entry` removes the waiter and sends it the handler result (`consensus_manager.rs:638, 646-654`). Under a pause the capacity error is not sent. The waiter stays registered and receives the final result, or times out. A timeout is reported as "committed, outcome unknown", never as a failure.

### 3.4 Leader Snapshots While Paused

`ConsensusManager::snapshot` (`consensus_manager.rs:1405-1445`) builds the snapshot data from `toc.collections_snapshot()` and the in-memory peer and cluster metadata, all of which reflect *applied* state. It stamps the snapshot with `index = hard_state.commit` (`:1431`). A leader with `applied < commit` would send followers data from `applied` labeled as `commit`. The follower would never apply the entries in between. Today the window is short, and it is pre-existing. A pause would keep it open for as long as the disk is full.

**Rule:** `snapshot()` returns `SnapshotTemporarilyUnavailable` while `last_applied_entry < hard_state.commit`. The function already returns this error when `commit < request_index` (`:1413-1417`), and raft-rs retries later. An alternative is to build the snapshot at the applied index with the term of that entry. That also closes the pre-existing window, but it needs a check that raft-rs and Qdrant accept a snapshot below the leader's commit index. The simpler rule is proposed. Both `snapshot()` and apply are expected to run on the consensus thread, so `applied` cannot change between the check and the build. This is not line-checked here, and T7 must confirm it.

### 3.5 Behavior While Paused

- Raft keeps appending, saving hard state, sending persisted messages and granting votes.
- A paused leader does not accept new conf changes while one is pending (`raft.rs:2102-2131`), so peer add and remove wait.
- A paused node with an unapplied conf change in `(applied, committed]` refuses to campaign (`raft.rs:1548-1567`). If a majority of voters is paused behind a conf change, no leader can be elected until one resumes. That is a liveness limit, not a safety violation.
- Proposals that wait for apply on the paused node time out.
- `/cluster` and telemetry report `apply_paused: capacity`, the stuck index and the backlog length.
- **Readiness.** The readiness check only exists in distributed mode (`src/main.rs:620`, `:699`). It latches: once `is_ready` is set, it never goes back to `false` (`src/common/health.rs:104-105, 380-383`). Before that, it waits until the local `last_applied_entry` reaches the cluster commit index (`health.rs:158-185, 297-302`). So:
  - A node that was already ready when the pause began stays ready. It keeps serving searches from its collections as they were at the paused index.
  - A node that starts, or restarts, while paused stays not ready until apply catches up. If `/readyz` is configured as the Kubernetes readiness probe, that pod is not added to the Service endpoints. Search traffic through the Service does not reach it until the pause ends. Whether that is the right behavior for a restarted node is a product question for review.

## 4. Startup

`Consensus::new` replays the unapplied queue (`src/consensus.rs:367-385`). A capacity error on a pause-eligible operation during that replay must pause, not fail startup. Startup panics in shard loading, which kept `...-36` down from 17:39 on, are outside this design.

## 5. Risks

- **B2.** No source shows that every handler is retry-safe after a partial failure. Only audited variants pause.
- **B3.** Source reading shows that raft-rs supports a held applied index through `advance_append` and `advance_apply_to`. It does not prove that nothing else in raft-rs or Qdrant assumes the applied index catches up within bounded time (for example the `commit`-based checks around `src/consensus.rs:889`).
- **Backlog growth.** The unapplied log grows for as long as the pause lasts. Compaction cannot pass the paused index, so the consensus WAL keeps growing on a volume that is already full. Without R1, the node stops at its first failed WAL append or state save, as today.
- **Dev differs from the incident build.** A backport to 1.17 has to handle the thread exiting there.

## 6. Tests (Planned; None Written or Run)

| ID | Setup | Assertion |
|---|---|---|
| T1 | Each row of the table in Section 3.2, including redelivery after a rebuild and a gap | Queue matches the table; gap returns `Err`; no index skipped; queue survives restart |
| T2 | 3 nodes on loopback volumes; one and then two nodes below the guard threshold; propose a pause-eligible meta-operation | Replication and voting continue; `apply_paused` reported; the meta-operation still fails with the guard error on the paused node; applied index frozen |
| T3 | T2 plus a hook that records raft-rs `raft_log.applied` after each `Ready` | raft-rs applied index equals `last_applied_entry` and never exceeds the paused index; no `fatal!` |
| T4 (B3) | Pause the leader and one follower for 10 × the election timeout with a conf change committed behind the paused entry; also pause a majority behind a conf change | Leader rejects new conf changes; paused nodes with an unapplied conf change do not campaign; after resume, the conf change applies exactly once and auto-leave fires only after it |
| T5 | Waiter on a pause-eligible operation while paused | No error while paused; the final result after resume, or a timeout reported as "outcome unknown" |
| T6 | `shadow_state_machine: panic` and a handler failpoint that persists a partial effect and then returns a capacity error | No divergence panic on retry; shadow invalidated on each pause |
| T6b (B2) | For each pause-eligible operation, a failpoint after each persisted sub-effect, then retry | Final state equals a single apply |
| T7 | Paused leader with a lagging follower that needs a snapshot | No snapshot is produced with `index > last_applied_entry`; the follower installs one only after resume, and its state matches the leader's |
| T8 | Readiness: a node already ready when the pause starts; a node restarted during a pause | The first stays ready; the second stays not ready until apply catches up, then becomes ready |
| T9 | Free space returns | Paused entries apply in order exactly once |

## Appendix. Source Anchors (dev `23320c2c8` unless noted)

- `lib/storage/src/content_manager/consensus_manager.rs`: `apply_entries` and shadow invalidation `387-402`, comment `390-398`; `apply_entries_impl` `404-498`, WAL read by queue index `417-428`, `ServiceError` stop `448-452`, user error `454-461`; waiter `638, 646-654`; `compact_wal` `1109-1136`; `snapshot` `1405-1445`, `SnapshotTemporarilyUnavailable` `1413-1417`, index from commit `1431`
- `lib/storage/src/content_manager/consensus/persistent.rs`: `set_unapplied_entries` `233-240`; `entry_queue.rs`: `new` `8-11`, `set_from_snapshot` `41-43`
- `src/consensus.rs`: rebuild `149-222`; startup re-apply `367-385`; `advance` `1144`; `advance_apply` `1187`
- `src/common/health.rs`: latch comment `104-105`; commit wait `158-185`; local index from `last_applied_entry` `297-302`; `set_ready` `380-383`. `src/main.rs`: health checker in distributed mode `620`, none otherwise `699`
- `lib/collection/src/shards/local_shard/shard_ops.rs`: guard `83-96`, generic `service_error` `90-92`; `local_shard/mod.rs` threshold `277-283`
- raft-rs `aafb07c7`: `raw_node.rs` `advance` `654-668`, `advance_append` `670-700`, `advance_apply`/`advance_apply_to` `711-720`; `raft.rs` `commit_apply` `960-1004`, campaign check `1548-1567`, conf-change rejection `2102-2131`, `has_pending_conf` `2788-2791`; `raft_log.rs` `applied_to` range check `322-339`
- `eabee371`: `src/consensus.rs` exits on error `110-118`
