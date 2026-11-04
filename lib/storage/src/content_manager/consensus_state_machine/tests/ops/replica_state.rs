//! `SetShardReplicaState` operation

use std::collections::HashMap;

use collection::operations::cluster_ops::ReshardingDirection;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::resharding::ReshardState;
use collection::shards::transfer::ShardTransferMethod;
#[cfg(feature = "staging")]
use uuid::Uuid;

use super::*;
use crate::content_manager::consensus_state_machine::*;
use crate::content_manager::errors::StorageError;

#[test]
fn set_shard_replica_state_upserts_known_peer() {
    let mut state = auto_resharding_state(1);
    add_peer(&mut state, OTHER_PEER_ID, Some("1.18.0"));

    let mut machine = state_machine(state);
    let outcome = machine.apply(&set_replica_state_op(
        0,
        OTHER_PEER_ID,
        ReplicaState::Initializing,
        None,
    ));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("setting replica state should be accepted, got {outcome:?}");
    };
    assert!(matches!(
        actions.as_slice(),
        [Action::SetReplicaState {
            peer_id: OTHER_PEER_ID,
            state: ReplicaState::Initializing,
            ..
        }]
    ));
    assert_eq!(
        machine
            .state()
            .collection(COLLECTION)
            .expect("collection")
            .shards[&0]
            .replicas[&OTHER_PEER_ID],
        ReplicaState::Initializing,
    );
}

#[test]
fn set_shard_replica_state_reject_from_state_mismatch() {
    let state = auto_resharding_state(1);
    let mut machine = state_machine(state.clone());

    let outcome = machine.apply(&set_replica_state_op(
        0,
        PEER_ID,
        ReplicaState::Active,
        Some(ReplicaState::Initializing),
    ));

    assert!(matches!(outcome, Err(StorageError::BadInput { .. })));
    assert_eq!(machine.state(), &state);
}

#[test]
fn set_shard_replica_state_reject_last_active() {
    let state = auto_resharding_state(1);
    let mut machine = state_machine(state.clone());

    let outcome = machine.apply(&set_replica_state_op(0, PEER_ID, ReplicaState::Dead, None));

    assert!(matches!(outcome, Err(StorageError::BadInput { .. })));
    assert_eq!(machine.state(), &state);
}

#[test]
fn set_shard_replica_state_dead_aborts_transfer_first() {
    let mut state = transfer_state();
    let transfer = shard_transfer(
        PEER_ID,
        OTHER_PEER_ID,
        true,
        ShardTransferMethod::StreamRecords,
    );
    let collection = state.collections.get_mut(COLLECTION).expect("collection");
    collection
        .shards
        .get_mut(&0)
        .expect("shard")
        .replicas
        .insert(OTHER_PEER_ID, ReplicaState::Active);
    collection.transfers.insert(transfer);

    let mut machine = state_machine(state);
    let outcome = machine.apply(&set_replica_state_op(
        0,
        OTHER_PEER_ID,
        ReplicaState::Dead,
        None,
    ));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("marking a transfer replica dead should be accepted, got {outcome:?}");
    };
    assert!(matches!(
        actions.last(),
        Some(Action::SetReplicaState {
            peer_id: OTHER_PEER_ID,
            state: ReplicaState::Dead,
            ..
        })
    ));
    let unregister = actions
        .iter()
        .position(|action| matches!(action, Action::UnregisterTransfer { .. }))
        .expect("transfer aborted");
    assert!(unregister + 1 == actions.len() - 1);
}

#[test]
fn set_shard_replica_state_dead_aborts_scale_up() {
    let key = resharding_key(ReshardingDirection::Up, 1);
    let mut state = auto_resharding_state(2);
    let collection = state.collections.get_mut(COLLECTION).expect("collection");
    collection.resharding = Some(ReshardState::new(
        key.uuid,
        key.direction,
        key.peer_id,
        key.shard_id,
        key.shard_key.clone(),
    ));
    collection.shards.get_mut(&1).expect("shard").replicas =
        HashMap::from([(PEER_ID, ReplicaState::Resharding)]);

    let mut machine = state_machine(state);
    let outcome = machine.apply(&set_replica_state_op(1, PEER_ID, ReplicaState::Dead, None));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("marking a scale-up replica dead should be accepted, got {outcome:?}");
    };
    let actions = actions.as_slice();

    #[cfg(feature = "staging")]
    let [actions @ .., Action::TestPauseReshardingAbort] = actions else {
        panic!("abort hooks must follow the state clear, got {actions:?}");
    };

    assert!(matches!(
        actions.last(),
        Some(Action::SetReshardingState { state: None, .. })
    ));
    assert!(!actions.iter().any(|action| matches!(
        action,
        Action::SetReplicaState {
            state: ReplicaState::Dead,
            ..
        }
    )));

    let collection = machine.state().collection(COLLECTION).expect("collection");
    assert!(!collection.shards.contains_key(&1));
    assert!(collection.resharding.is_none());
}

#[test]
fn set_shard_replica_state_missing_shard_finishes_abort() {
    let key = resharding_key(ReshardingDirection::Up, 1);
    let mut state = auto_resharding_state(1);
    state
        .collections
        .get_mut(COLLECTION)
        .expect("collection")
        .resharding = Some(ReshardState::new(
        key.uuid,
        key.direction,
        key.peer_id,
        key.shard_id,
        key.shard_key.clone(),
    ));

    let mut machine = state_machine(state);
    let outcome = machine.apply(&set_replica_state_op(1, PEER_ID, ReplicaState::Dead, None));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("replaying after scale-up shard drop should be accepted, got {outcome:?}");
    };
    let actions = actions.as_slice();

    #[cfg(feature = "staging")]
    let [actions @ .., Action::TestPauseReshardingAbort] = actions else {
        panic!("abort hooks must follow the state clear, got {actions:?}");
    };

    assert!(matches!(
        actions.last(),
        Some(Action::SetReshardingState { state: None, .. })
    ));
    assert!(
        machine
            .state()
            .collection(COLLECTION)
            .expect("collection")
            .resharding
            .is_none()
    );
}

#[cfg(feature = "staging")]
#[test]
fn scale_down_abort_hooks_precede_dead_write_and_are_omitted_on_replay() {
    let mut state = auto_resharding_state(2);
    let collection = state.collections.get_mut(COLLECTION).expect("collection");
    collection.resharding = Some(ReshardState::new(
        Uuid::nil(),
        ReshardingDirection::Down,
        PEER_ID,
        1,
        None,
    ));
    collection
        .shards
        .get_mut(&0)
        .expect("shard")
        .replicas
        .insert(OTHER_PEER_ID, ReplicaState::ReshardingScaleDown);

    let operation = set_replica_state_op(0, OTHER_PEER_ID, ReplicaState::Dead, None);
    let mut machine = state_machine(state.clone());
    let Ok(Plan { actions, .. }) = machine.apply(&operation) else {
        panic!("marking a scale-down replica dead should be accepted");
    };

    let clear = actions
        .iter()
        .position(|action| matches!(action, Action::SetReshardingState { state: None, .. }))
        .expect("resharding clear");
    assert!(matches!(
        &actions[clear + 1..],
        [
            Action::TestCrashAfterScaleDownStateClear,
            Action::TestPauseReshardingAbort,
            Action::SetReplicaState {
                shard_id: 0,
                peer_id: OTHER_PEER_ID,
                state: ReplicaState::Dead,
                ..
            },
        ]
    ));

    for action in &actions[..=clear] {
        state.apply_action(action);
    }

    let mut replay = state_machine(state);
    let Ok(Plan { actions, .. }) = replay.apply(&operation) else {
        panic!("replay after clearing resharding state should be accepted");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::SetReplicaState { .. }]
    ));
    assert_eq!(replay.state(), machine.state());
}
