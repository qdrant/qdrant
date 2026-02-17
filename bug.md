

## So what I see is:

```
2026-01-12 20:15:47.060
Starting initializing for pod 3
	2026-01-12 20:22:07.638
2026-01-12T14:52:07.637825Z DEBUG storage::content_manager::consensus_manager: Removing node 103
	2026-01-12 20:22:07.667
2026-01-12T14:52:07.667668Z  INFO qdrant::consensus: Consensus stopped
	2026-01-12 20:31:57.265
Starting initializing for pod 3
	2026-01-12 20:32:12.283
2026-01-12T15:02:12.283797Z DEBUG storage::content_manager::consensus_manager: Removing node 103
	2026-01-12 20:32:12.316
2026-01-12T15:02:12.316732Z  INFO qdrant::consensus: Consensus stopped
```


```
2026-01-12 21:13:00.452
2026-01-12T15:43:00.452451Z DEBUG storage::content_manager::consensus_manager: Applying committed entry with index 83
	2026-01-12 21:13:00.452
2026-01-12T15:43:00.452528Z  INFO raft::raft: switched to configuration, raft_id: 103, config: Configuration { voters: Configuration { incoming: Configuration { voters: {100, 101, 102} }, outgoing: Configuration { voters: {} } }, learners: {}, learners_next: {}, auto_leave: false }
	2026-01-12 21:13:00.452
2026-01-12T15:43:00.452585Z DEBUG storage::content_manager::consensus_manager: Applied conf state ConfState { voters: [100, 101, 102], learners: [], voters_outgoing: [], learners_next: [], auto_leave: false }
	2026-01-12 21:13:00.465
2026-01-12T15:43:00.464938Z DEBUG storage::content_manager::consensus_manager: Removing node 103

	2026-01-12 21:13:00.505
2026-01-12T15:43:00.505641Z DEBUG storage::content_manager::consensus_manager: Successfully applied configuration change entry. Index: 83. Stop consensus: true
```


t1c83 -> last self remove operation

```
2026-01-12 20:31:57.265 Starting initializing for pod 3
2026-01-12T15:01:58.797433Z DEBUG storage::content_manager::consensus::consensus_wal: Appending entry: Entry { entry_type: EntryNormal, term: 1, index: 1, data: [], context: [], sync_log: false }

2026-01-12T15:01:59.336240Z DEBUG storage::content_manager::consensus::consensus_wal: Appending entry: Entry { entry_type: EntryConfChangeV2, term: 17, index: 96, data: [18, 4, 8, 2, 16, 103], context: [104, 116, 116, 112, 58, 47, 47, 113, 100, 114, 97, 110, 116, 45, 99, 104, 97, 111, 115, 45, 116, 101, 115, 116, 105, 110, 103, 45, 51, 46, 113, 100, 114, 97, 110, 116, 45, 104, 101, 97, 100, 108, 101, 115, 115, 45, 99, 104, 97, 111, 115, 45, 116, 101, 115, 116, 105, 110, 103, 58, 54, 51, 51, 53, 47], sync_log: false }
2026-01-12T15:02:12.283797Z DEBUG storage::content_manager::consensus_manager: Removing node 103
```

```
Starting initializing for pod 3
2026-01-12T15:43:00.368200Z  INFO storage::content_manager::consensus::persistent: Loading raft state from ./storage/raft_state.json
2026-01-12T15:43:00.452451Z DEBUG storage::content_manager::consensus_manager: Applying committed entry with index 83 ---> Why is it starting from 83 which comitte
2026-01-12T15:43:00.505641Z DEBUG storage::content_manager::consensus_manager: Successfully applied configuration change entry. Index: 83. Stop consensus: true
2026-01-12T15:43:00.464938Z DEBUG storage::content_manager::consensus_manager: Removing node 103
```


## MSG FOR core

```
This is what I see in chaos testing n3:
2026-01-12T15:43:00.387231Z DEBUG storage::content_manager::consensus::persistent: State: Persistent { state: RaftState { hard_state: HardState { term: 17, vote: 0, commit: 96 }, conf_state: ConfState { voters: [101, 100, 102], learners: [], voters_outgoing: [], learners_next: [], auto_leave: false } }, latest_snapshot_meta: SnapshotMetadataSer { term: 0, index: 0 }, apply_progress_queue: EntryApplyProgressQueue(Some((83, 96))), first_voter: Some(100), peer_address_by_id: RwLock { data: {103: http://qdrant-chaos-testing-3.qdrant-headless-chaos-testing:6335/} }, peer_metadata_by_id: RwLock { data: {} }, cluster_metadata: {}, this_peer_id: 103, path: "./storage/raft_state.json", dirty: false }

/// noisy logs..

2026-01-12T15:43:00.452451Z DEBUG storage::content_manager::consensus_manager: Applying committed entry with index 83
2026-01-12T15:43:00.452528Z  INFO raft::raft: switched to configuration, raft_id: 103, config: Configuration { voters: Configuration { incoming: Configuration { voters: {100, 101, 102} }, outgoing: Configuration { voters: {} } }, learners: {}, learners_next: {}, auto_leave: false }
2026-01-12T15:43:00.452585Z DEBUG storage::content_manager::consensus_manager: Applied conf state ConfState { voters: [100, 101, 102], learners: [], voters_outgoing: [], learners_next: [], auto_leave: false }
2026-01-12T15:43:00.464938Z DEBUG storage::content_manager::consensus_manager: Removing node 103
2026-01-12T15:43:00.505641Z DEBUG storage::content_manager::consensus_manager: Successfully applied configuration change entry. Index: 83. Stop consensus: true

Notice how EntryApplyProgressQueue  from raft_state.json is saying 83 -> 96 needs to be applied. 83 seems to be self removal.

Before this:
2026-01-12T15:01:57.278706Z  INFO storage::content_manager::consensus::persistent: Initializing new raft state at ./storage/raft_state.json
```
