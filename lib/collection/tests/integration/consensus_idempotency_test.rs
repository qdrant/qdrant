//! Replay/idempotency tests for `Collection::set_shard_replica_state` and the
//! resharding-abort composition (audit findings B and I, Part 1).
//!
//! These exercise the in-process `Collection` API directly. The multi-peer
//! crash-window scenarios (mid-teardown down crash, post-dir-drop up residue)
//! live in the Python consensus suite
//! (`tests/consensus_tests/test_resharding_set_replica_dead_replay.py`,
//! `test_resharding_down_abort_converges_after_crash.py`,
//! `test_resharding_replay.py`); here we cover what a single process can drive
//! deterministically.

use async_trait::async_trait;
use collection::operations::cluster_ops::ReshardingDirection;
use collection::operations::types::CollectionResult;
use collection::shards::CollectionId;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::resharding::ReshardKey;
use collection::shards::shard::{PeerId, ShardId};
use collection::shards::transfer::{
    ShardTransfer, ShardTransferConsensus, ShardTransferKey, ShardTransferMethod,
};
use tempfile::Builder;
use uuid::Uuid;

use crate::common::simple_collection_fixture;

/// `Collection::start_resharding` never touches its `ShardTransferConsensus`
/// argument (the resharding driver is disabled), so every method can be a stub.
struct NoopReshardingConsensus;

#[async_trait]
impl ShardTransferConsensus for NoopReshardingConsensus {
    fn this_peer_id(&self) -> PeerId {
        0
    }

    fn peers(&self) -> Vec<PeerId> {
        vec![0]
    }

    fn consensus_commit_term(&self) -> (u64, u64) {
        (0, 0)
    }

    fn recovered_switch_to_partial(
        &self,
        _transfer_config: &ShardTransfer,
        _collection_id: CollectionId,
    ) -> CollectionResult<()> {
        unimplemented!("not exercised by start_resharding")
    }

    async fn start_shard_transfer(
        &self,
        _transfer_config: ShardTransfer,
        _collection_id: CollectionId,
    ) -> CollectionResult<()> {
        unimplemented!("not exercised by start_resharding")
    }

    async fn restart_shard_transfer(
        &self,
        _transfer_config: ShardTransfer,
        _collection_id: CollectionId,
        _default_method: ShardTransferMethod,
    ) -> CollectionResult<()> {
        unimplemented!("not exercised by start_resharding")
    }

    async fn abort_shard_transfer(
        &self,
        _transfer: ShardTransferKey,
        _collection_id: CollectionId,
        _reason: &str,
    ) -> CollectionResult<()> {
        unimplemented!("not exercised by start_resharding")
    }

    async fn set_shard_replica_set_state(
        &self,
        _peer_id: Option<PeerId>,
        _collection_id: CollectionId,
        _shard_id: ShardId,
        _state: ReplicaState,
        _from_state: Option<ReplicaState>,
    ) -> CollectionResult<()> {
        unimplemented!("not exercised by start_resharding")
    }

    async fn commit_read_hashring(
        &self,
        _collection_id: CollectionId,
        _reshard_key: ReshardKey,
    ) -> CollectionResult<()> {
        unimplemented!("not exercised by start_resharding")
    }

    async fn commit_write_hashring(
        &self,
        _collection_id: CollectionId,
        _reshard_key: ReshardKey,
    ) -> CollectionResult<()> {
        unimplemented!("not exercised by start_resharding")
    }
}

/// [audit-B] validation #2 relaxation: when `current_state` already equals the
/// target `new_state`, the entry is a replay whose final write landed — it must
/// be accepted (and re-run idempotently) even if `from_state` no longer matches,
/// instead of being dismissed as `bad_input`.
#[tokio::test]
async fn test_set_replica_state_accepts_replay_when_current_equals_new() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = simple_collection_fixture(dir.path(), 1).await;

    // Shard 0 on peer 0 is `Active`. Re-apply a transition *into* `Active` with a
    // `from_state` that no longer matches (`Initializing`) — as a replayed entry
    // whose `Active` write already landed would look. Pre-relaxation this failed
    // validation #2 with `bad_input`; now it is an accepted no-op.
    collection
        .set_shard_replica_state(0, 0, ReplicaState::Active, Some(ReplicaState::Initializing))
        .await
        .expect("replay whose final write already landed must be accepted, not dismissed");

    let state = collection.state().await;
    assert_eq!(
        state.shards[&0].replicas.get(&0),
        Some(&ReplicaState::Active),
        "replica state must remain Active after the idempotent replay",
    );
}

/// [audit-B] Setting a resharding-*up* replica `Dead` aborts the resharding
/// first (dropping the shard) and then converges: the shard is gone, the
/// persisted resharding state is cleared, and re-applying the same entry does
/// not resurrect the shard or the state.
#[tokio::test]
async fn test_set_resharding_up_replica_dead_drops_shard_and_clears_state() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    // Auto-sharded collection with shards 0 and 1.
    let collection = simple_collection_fixture(dir.path(), 2).await;

    // Start resharding *up*: creates shard 2 on peer 0 in `Resharding` state and
    // persists resharding_state (shard_number -> 3).
    let key = ReshardKey {
        uuid: Uuid::new_v4(),
        direction: ReshardingDirection::Up,
        peer_id: 0,
        shard_id: 2,
        shard_key: None,
    };
    collection
        .start_resharding(
            key,
            Box::new(NoopReshardingConsensus),
            std::future::ready(()),
            std::future::ready(()),
        )
        .await
        .expect("start_resharding up must succeed");

    assert!(
        collection.resharding_state().await.is_some(),
        "resharding must be in progress after start",
    );
    assert!(
        collection.state().await.shards.contains_key(&2),
        "resharding up must have created shard 2",
    );

    // Deactivate the resharding replica. This triggers the abort composition
    // (with except_replica = (2, 0)): the shard is dropped, so the handler takes
    // the missing-shard early return after clearing the state.
    collection
        .set_shard_replica_state(2, 0, ReplicaState::Dead, Some(ReplicaState::Resharding))
        .await
        .expect("deactivating a resharding-up replica must abort resharding and return Ok");

    assert!(
        collection.resharding_state().await.is_none(),
        "resharding_state must be cleared after the abort",
    );
    assert!(
        !collection.state().await.shards.contains_key(&2),
        "shard 2 must be dropped by the resharding-up abort",
    );

    // Replay the exact same entry. The shard is gone and resharding_state is
    // None, so the crash-signature re-entry does not fire and the entry is
    // benignly dismissed (not_found) — critically, nothing is resurrected and
    // the state stays converged.
    let replay = collection
        .set_shard_replica_state(2, 0, ReplicaState::Dead, Some(ReplicaState::Resharding))
        .await;
    assert!(
        replay.is_err(),
        "replay after full completion is a benign dismissal (shard already gone)",
    );
    assert!(
        collection.resharding_state().await.is_none(),
        "resharding_state must stay cleared across the replay",
    );
    assert!(
        !collection.state().await.shards.contains_key(&2),
        "the dropped shard must not be resurrected by the replay",
    );
}
