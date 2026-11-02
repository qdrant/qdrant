//! Resharding operations

use std::collections::{HashMap, HashSet};

use collection::config::ShardingMethod;
use collection::operations::cluster_ops::ReshardingDirection;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::resharding::{ReshardState, ReshardingStage};
use segment::types::*;

use super::*;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::*;
use crate::content_manager::errors::StorageError;

#[test]
fn resharding_start_up() {
    let state = auto_resharding_state(1);
    let key = resharding_key(ReshardingDirection::Up, 1);

    let mut machine = state_machine(state);
    let outcome = machine.apply(&resharding_op(ReshardingOperation::Start(key.clone())));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("starting scale-up resharding should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [
            Action::CreateAndRegisterShards { .. },
            Action::SetReshardingState { .. },
            Action::SetShardNumber { .. },
        ]
    ));

    let collection = machine.state().collection(COLLECTION).expect("collection");
    assert_eq!(
        collection.resharding.as_ref().map(ReshardState::key),
        Some(key)
    );
    assert_eq!(collection.config.params.shard_number.get(), 2);
    assert_eq!(
        collection.shards[&1].replicas,
        HashMap::from([(PEER_ID, ReplicaState::Resharding)]),
    );
}

#[test]
fn resharding_start_up_replay() {
    let key = resharding_key(ReshardingDirection::Up, 1);
    let mut machine = state_machine(auto_resharding_state(1));
    machine
        .apply(&resharding_op(ReshardingOperation::Start(key.clone())))
        .unwrap();

    let goal = machine.state().clone();
    let outcome = machine.apply(&resharding_op(ReshardingOperation::Start(key)));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("replaying resharding start should be accepted, got {outcome:?}");
    };

    assert!(actions.is_empty());
    assert_eq!(machine.state(), &goal);
}

#[test]
fn resharding_start_up_reject_missing_shard_key() {
    let shard_key = ShardKey::from("north");
    let mut state = custom_sharding_state();
    add_shard_key(&mut state, shard_key.clone(), &[1]);
    add_shard_key(&mut state, "south".into(), &[2]);

    let mut key = resharding_key(ReshardingDirection::Up, 3);
    key.shard_key = Some(shard_key.clone());

    let mut machine = state_machine(state);
    let outcome = machine.apply(&drop_shard_key_op(shard_key));
    assert!(outcome.is_ok());
    let state = machine.state().clone();

    let outcome = machine.apply(&resharding_op(ReshardingOperation::Start(key)));

    assert!(matches!(
        outcome,
        Err(StorageError::BadRequest { ref description })
            if description == "\"north\" hashring does not exist"
    ));
    assert_eq!(machine.state(), &state);
}

#[test]
fn resharding_start_up_custom_shard_key_replay() {
    let shard_key = ShardKey::from("north");
    let mut state = custom_sharding_state();
    add_shard_key(&mut state, shard_key.clone(), &[1]);

    let mut key = resharding_key(ReshardingDirection::Up, 2);
    key.shard_key = Some(shard_key);
    let operation = resharding_op(ReshardingOperation::Start(key));

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&operation);
    let Ok(Plan { actions, .. }) = outcome else {
        panic!("starting custom scale-up resharding should be accepted, got {outcome:?}");
    };
    let goal = machine.state().clone();

    assert!(matches!(
        actions.as_slice(),
        [
            Action::CreateAndRegisterShards { .. },
            Action::SetReshardingState { .. },
        ]
    ));

    for cut in 0..=actions.len() {
        let mut partial = state.clone();
        for action in &actions[..cut] {
            partial.apply_action(action);
        }

        let mut replay = state_machine(partial);
        let outcome = replay.apply(&operation);

        assert!(outcome.is_ok(), "{outcome:?}");
        assert_eq!(replay.state(), &goal);
    }
}

#[test]
fn resharding_start_down_reject_last_shard() {
    let state = auto_resharding_state(1);
    let key = resharding_key(ReshardingDirection::Down, 0);

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&resharding_op(ReshardingOperation::Start(key)));

    assert!(matches!(outcome, Err(StorageError::BadRequest { .. })));
    assert_eq!(machine.state(), &state);
}

#[test]
fn resharding_commit_stages() {
    let key = resharding_key(ReshardingDirection::Up, 1);
    let mut machine = state_machine(auto_resharding_state(1));
    machine
        .apply(&resharding_op(ReshardingOperation::Start(key.clone())))
        .unwrap();

    let outcome = machine.apply(&resharding_op(ReshardingOperation::CommitRead(key.clone())));
    let Ok(Plan { actions, .. }) = outcome else {
        panic!("committing the read hash ring should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [
            Action::SetReshardingStage {
                stage: ReshardingStage::ReadHashRingCommitted,
                ..
            },
            Action::InvalidateCleanLocalShards { .. },
        ]
    ));

    let outcome = machine.apply(&resharding_op(ReshardingOperation::CommitWrite(
        key.clone(),
    )));
    let Ok(Plan { actions, .. }) = outcome else {
        panic!("committing the write hash ring should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::SetReshardingStage {
            stage: ReshardingStage::WriteHashRingCommitted,
            ..
        }]
    ));

    assert_eq!(
        machine
            .state()
            .collection(COLLECTION)
            .and_then(|collection| collection.resharding.as_ref())
            .map(|resharding| resharding.stage),
        Some(ReshardingStage::WriteHashRingCommitted),
    );
}

#[test]
fn resharding_finish_down() {
    let key = resharding_key(ReshardingDirection::Down, 1);
    let mut state = auto_resharding_state(2);
    let mut resharding = ReshardState::new(
        key.uuid,
        key.direction,
        key.peer_id,
        key.shard_id,
        key.shard_key.clone(),
    );
    resharding.stage = ReshardingStage::WriteHashRingCommitted;
    state
        .collections
        .get_mut(COLLECTION)
        .expect("collection")
        .resharding = Some(resharding);

    let mut machine = state_machine(state);
    let outcome = machine.apply(&resharding_op(ReshardingOperation::Finish(key)));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("finishing scale-down resharding should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [
            Action::SetShardNumber { .. },
            Action::DropShard { shard_id: 1, .. },
            Action::SetReshardingState { state: None, .. },
        ]
    ));

    let collection = machine.state().collection(COLLECTION).expect("collection");
    assert_eq!(collection.config.params.shard_number.get(), 1);
    assert!(!collection.shards.contains_key(&1));
    assert!(collection.resharding.is_none());
}

#[test]
fn resharding_abort_up() {
    let key = resharding_key(ReshardingDirection::Up, 1);
    let mut machine = state_machine(auto_resharding_state(1));
    machine
        .apply(&resharding_op(ReshardingOperation::Start(key.clone())))
        .unwrap();

    let outcome = machine.apply(&resharding_op(ReshardingOperation::Abort(key)));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("aborting scale-up resharding should be accepted, got {outcome:?}");
    };

    let actions = actions.as_slice();

    #[cfg(feature = "staging")]
    let [actions @ .., Action::TestPauseReshardingAbort] = actions else {
        panic!("abort hooks must follow the state clear, got {actions:?}");
    };

    assert!(matches!(
        actions,
        [
            Action::InvalidateCleanLocalShards { .. },
            Action::RevertHashRing { .. },
            Action::SetShardNumber { .. },
            Action::DropShard { shard_id: 1, .. },
            Action::SetReshardingState { state: None, .. },
        ]
    ));

    let collection = machine.state().collection(COLLECTION).expect("collection");
    assert_eq!(collection.config.params.shard_number.get(), 1);
    assert!(!collection.shards.contains_key(&1));
    assert!(collection.resharding.is_none());
}

#[test]
fn resharding_abort_up_removes_custom_mapping() {
    let shard_key = ShardKey::from("north");
    let mut state = custom_sharding_state();
    add_shard_key(&mut state, shard_key.clone(), &[0, 1]);
    set_resharding(&mut state, shard_key.clone(), 1);
    let key = state
        .collection(COLLECTION)
        .and_then(|collection| collection.resharding.as_ref())
        .map(ReshardState::key)
        .expect("resharding");

    let mut machine = state_machine(state);
    let outcome = machine.apply(&resharding_op(ReshardingOperation::Abort(key)));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("aborting custom scale-up should be accepted, got {outcome:?}");
    };
    assert!(actions.iter().any(|action| matches!(
        action,
        Action::RemoveShardFromKeyMapping { shard_id: 1, .. }
    )));

    let collection = machine.state().collection(COLLECTION).expect("collection");
    assert_eq!(
        collection.shards_key_mapping[&shard_key],
        HashSet::from([0]),
    );
    assert!(!collection.shards.contains_key(&1));
}

#[test]
fn resharding_abort_down_reverts_replicas() {
    let key = resharding_key(ReshardingDirection::Down, 1);
    let mut state = auto_resharding_state(2);

    let resharding = ReshardState::new(
        key.uuid,
        key.direction,
        key.peer_id,
        key.shard_id,
        key.shard_key.clone(),
    );

    let collection = state.collections.get_mut(COLLECTION).expect("collection");
    collection.resharding = Some(resharding);
    collection
        .shards
        .get_mut(&0)
        .expect("shard")
        .replicas
        .insert(OTHER_PEER_ID, ReplicaState::ReshardingScaleDown);

    let mut machine = state_machine(state);
    let outcome = machine.apply(&resharding_op(ReshardingOperation::Abort(key)));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("aborting scale-down resharding should be accepted, got {outcome:?}");
    };

    let actions = actions.as_slice();

    #[cfg(feature = "staging")]
    let [
        actions @ ..,
        Action::TestCrashAfterScaleDownStateClear,
        Action::TestPauseReshardingAbort,
    ] = actions
    else {
        panic!("abort hooks must follow the state clear, got {actions:?}");
    };

    assert!(matches!(
        actions,
        [
            Action::InvalidateCleanLocalShards { .. },
            Action::SetReplicaState {
                shard_id: 0,
                peer_id: OTHER_PEER_ID,
                state: ReplicaState::Active,
                ..
            },
            Action::DeleteMigratedPoints { .. },
            Action::RevertHashRing { .. },
            Action::SetReshardingState { state: None, .. },
        ]
    ));
}

#[test]
fn resharding_abort_down_without_key_matches_unmapped_shards() {
    let key = resharding_key(ReshardingDirection::Down, 1);
    let mut state = auto_resharding_state(3);

    let resharding = ReshardState::new(
        key.uuid,
        key.direction,
        key.peer_id,
        key.shard_id,
        key.shard_key.clone(),
    );

    let collection = state.collections.get_mut(COLLECTION).expect("collection");
    collection.config.params.sharding_method = Some(ShardingMethod::Custom);
    collection
        .shards_key_mapping
        .entry(ShardKey::from("north"))
        .or_default()
        .insert(0);
    collection.resharding = Some(resharding);

    for shard_id in [0, 2] {
        collection
            .shards
            .get_mut(&shard_id)
            .expect("shard")
            .replicas
            .insert(OTHER_PEER_ID, ReplicaState::ReshardingScaleDown);
    }

    let mut machine = state_machine(state);
    let outcome = machine.apply(&resharding_op(ReshardingOperation::Abort(key)));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("aborting scale-down resharding should be accepted, got {outcome:?}");
    };

    let Action::InvalidateCleanLocalShards { shard_ids, .. } = &actions[0] else {
        panic!("abort should invalidate shard cleanup, got {actions:?}");
    };
    assert_eq!(shard_ids, &[1, 2]);

    let actions = actions.as_slice();

    #[cfg(feature = "staging")]
    let [
        actions @ ..,
        Action::TestCrashAfterScaleDownStateClear,
        Action::TestPauseReshardingAbort,
    ] = actions
    else {
        panic!("abort hooks must follow the state clear, got {actions:?}");
    };

    assert!(matches!(
        actions,
        [
            Action::InvalidateCleanLocalShards { .. },
            Action::SetReplicaState {
                shard_id: 2,
                peer_id: OTHER_PEER_ID,
                state: ReplicaState::Active,
                ..
            },
            Action::DeleteMigratedPoints { .. },
            Action::RevertHashRing { .. },
            Action::SetReshardingState { state: None, .. },
        ]
    ));

    let collection = machine.state().collection(COLLECTION).expect("collection");
    assert_eq!(
        collection.shards[&0].replicas[&OTHER_PEER_ID],
        ReplicaState::ReshardingScaleDown,
    );
    assert_eq!(
        collection.shards[&2].replicas[&OTHER_PEER_ID],
        ReplicaState::Active,
    );
}

#[test]
fn resharding_abort_reject_after_read_commit() {
    let key = resharding_key(ReshardingDirection::Up, 1);
    let mut machine = state_machine(auto_resharding_state(1));
    machine
        .apply(&resharding_op(ReshardingOperation::Start(key.clone())))
        .unwrap();
    machine
        .apply(&resharding_op(ReshardingOperation::CommitRead(key.clone())))
        .unwrap();
    let state = machine.state().clone();

    let outcome = machine.apply(&resharding_op(ReshardingOperation::Abort(key)));

    assert!(matches!(outcome, Err(StorageError::BadRequest { .. })));
    assert_eq!(machine.state(), &state);
}

fn resharding_op(operation: ReshardingOperation) -> ConsensusOperations {
    let operation = CollectionMetaOperations::Resharding(COLLECTION.into(), operation);

    collection_meta_op(operation)
}

#[cfg(feature = "staging")]
#[test]
fn scale_down_abort_hooks_precede_dead_write_and_are_omitted_on_replay() {
    let mut state = auto_resharding_state(2);
    let collection = state.collections.get_mut(COLLECTION).expect("collection");
    collection.resharding = Some(ReshardState::new(
        uuid::Uuid::nil(),
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
