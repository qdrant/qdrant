//! Shard transfer operations

use std::collections::HashMap;
use std::num::NonZeroU32;

use collection::collection_state::ShardInfo;
use collection::operations::cluster_ops::ReshardingDirection;
use collection::operations::config_diff::OptimizersConfigDiff;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::resharding::ReshardState;
use collection::shards::transfer::{ShardTransfer, ShardTransferMethod, ShardTransferRestart};

use super::*;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::*;
use crate::content_manager::errors::StorageError;

#[test]
fn transfer_start() {
    let state = transfer_state();
    let transfer = shard_transfer(
        PEER_ID,
        OTHER_PEER_ID,
        false,
        ShardTransferMethod::StreamRecords,
    );

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&transfer_op(ShardTransferOperations::Start(
        transfer.clone(),
    )));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("starting a transfer should be accepted, got {outcome:?}");
    };
    let actions = actions.as_slice();

    #[cfg(feature = "staging")]
    let [Action::TestPauseShardTransferStart, actions @ ..] = actions else {
        panic!("sender pause must precede registration, got {actions:?}");
    };

    assert!(matches!(
        actions,
        [
            Action::RegisterTransfer { .. },
            Action::SetReplicaState {
                state: ReplicaState::Partial,
                ..
            },
            Action::SpawnTransferDriver { .. },
        ]
    ));

    let collection = machine.state().collection(COLLECTION).expect("collection");
    assert_eq!(
        collection.shards[&0].replicas.get(&OTHER_PEER_ID),
        Some(&ReplicaState::Partial),
    );
    assert!(collection.transfers.contains(&transfer));
}

#[test]
fn transfer_start_rejects_resharding_between_shard_keys() {
    let mut state = custom_sharding_state();
    add_peer(&mut state, PEER_ID, Some("1.18.0"));
    add_peer(&mut state, OTHER_PEER_ID, Some("1.18.0"));
    add_shard_key(&mut state, "source".into(), &[0]);
    add_shard_key(&mut state, "target".into(), &[1]);
    let transfer = ShardTransfer {
        shard_id: 0,
        to_shard_id: Some(1),
        from: PEER_ID,
        to: OTHER_PEER_ID,
        sync: true,
        method: Some(ShardTransferMethod::ReshardingStreamRecords),
        filter: None,
    };

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&transfer_op(ShardTransferOperations::Start(transfer)));

    assert!(matches!(outcome, Err(StorageError::BadRequest { .. })));
    assert_eq!(machine.state(), &state);
}

#[test]
fn transfer_start_receiver_initializes_local_shard() {
    let mut state = transfer_state();
    state
        .collections
        .get_mut(COLLECTION)
        .expect("collection")
        .shards
        .get_mut(&0)
        .expect("shard")
        .replicas
        .insert(OTHER_PEER_ID, ReplicaState::Active);
    let transfer = shard_transfer(OTHER_PEER_ID, PEER_ID, false, ShardTransferMethod::Snapshot);

    let mut machine = state_machine(state);
    let outcome = machine.apply(&transfer_op(ShardTransferOperations::Start(transfer)));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("starting a received transfer should be accepted, got {outcome:?}");
    };
    assert!(matches!(
        actions.as_slice(),
        [
            Action::RegisterTransfer { .. },
            Action::InitLocalShard {
                mode: LocalShardInitMode::EnsureExists,
                ..
            },
            Action::SetReplicaState {
                state: ReplicaState::Recovery,
                ..
            },
        ]
    ));
}

#[test]
fn transfer_start_replay_rejects_completed() {
    let transfer = shard_transfer(
        PEER_ID,
        OTHER_PEER_ID,
        false,
        ShardTransferMethod::StreamRecords,
    );
    let mut machine = state_machine(transfer_state());
    machine
        .apply(&transfer_op(ShardTransferOperations::Start(
            transfer.clone(),
        )))
        .unwrap();
    let state = machine.state().clone();

    let outcome = machine.apply(&transfer_op(ShardTransferOperations::Start(transfer)));

    assert!(matches!(outcome, Err(StorageError::BadRequest { .. })));
    assert_eq!(machine.state(), &state);
}

#[test]
fn transfer_start_applies_optimizer_override_to_default_method() {
    let mut state = auto_resharding_state(1);
    add_peer(&mut state, PEER_ID, Some("1.17.0"));
    add_peer(&mut state, OTHER_PEER_ID, Some("1.17.0"));

    let mut context = node_context();
    context.default_shard_transfer_method = Some(ShardTransferMethod::StreamRecords);
    context.optimizers_overwrite = Some(prevent_unoptimized_diff());

    let mut transfer = shard_transfer(
        PEER_ID,
        OTHER_PEER_ID,
        false,
        ShardTransferMethod::StreamRecords,
    );
    transfer.method = None;

    let mut machine = ConsensusStateMachine::new(state, context);
    let outcome = machine.apply(&transfer_op(ShardTransferOperations::Start(transfer)));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("starting a transfer should be accepted, got {outcome:?}");
    };
    let actions = actions.as_slice();

    #[cfg(feature = "staging")]
    let [Action::TestPauseShardTransferStart, actions @ ..] = actions else {
        panic!("sender pause must precede registration, got {actions:?}");
    };

    assert!(matches!(
        actions,
        [
            Action::RegisterTransfer {
                transfer: ShardTransfer {
                    method: Some(ShardTransferMethod::Snapshot),
                    ..
                },
                ..
            },
            Action::SetReplicaState {
                state: ReplicaState::Recovery,
                ..
            },
            Action::SpawnTransferDriver { .. },
        ]
    ));
}

#[test]
fn transfer_restart() {
    let mut state = transfer_state();
    let transfer = shard_transfer(PEER_ID, OTHER_PEER_ID, false, ShardTransferMethod::WalDelta);
    let collection = state.collections.get_mut(COLLECTION).expect("collection");
    collection.transfers.insert(transfer.clone());
    collection
        .shards
        .get_mut(&0)
        .expect("shard")
        .replicas
        .insert(OTHER_PEER_ID, ReplicaState::Recovery);
    let restart = ShardTransferRestart {
        shard_id: 0,
        to_shard_id: None,
        from: PEER_ID,
        to: OTHER_PEER_ID,
        method: ShardTransferMethod::Snapshot,
    };

    let mut machine = state_machine(state);
    let outcome = machine.apply(&transfer_op(ShardTransferOperations::Restart(restart)));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("restarting a transfer should be accepted, got {outcome:?}");
    };
    assert!(matches!(
        actions.as_slice(),
        [
            Action::StopTransferDriver { .. },
            Action::RevertProxyShard { .. },
            Action::SetReplicaState {
                state: ReplicaState::Recovery,
                ..
            },
            Action::SetTransferMethod {
                method: ShardTransferMethod::Snapshot,
                ..
            },
            Action::SpawnTransferDriver { .. },
        ]
    ));

    let transfer = machine
        .state()
        .collection(COLLECTION)
        .expect("collection")
        .transfers
        .iter()
        .next()
        .expect("transfer");
    assert_eq!(transfer.method, Some(ShardTransferMethod::Snapshot));
}

#[test]
fn transfer_restart_validates_initial_state_before_shard() {
    let mut state = transfer_state();
    let transfer = ShardTransfer {
        shard_id: 1,
        to_shard_id: None,
        from: PEER_ID,
        to: OTHER_PEER_ID,
        sync: false,
        method: Some(ShardTransferMethod::StreamRecords),
        filter: None,
    };
    state
        .collections
        .get_mut(COLLECTION)
        .expect("collection")
        .transfers
        .insert(transfer.clone());

    let restart = ShardTransferRestart {
        shard_id: transfer.shard_id,
        to_shard_id: transfer.to_shard_id,
        from: transfer.from,
        to: transfer.to,
        method: ShardTransferMethod::ReshardingStreamRecords,
    };

    let mut machine = state_machine(state);
    let outcome = machine.apply(&transfer_op(ShardTransferOperations::Restart(restart)));

    assert!(matches!(outcome, Err(StorageError::BadInput { .. })));
}

#[test]
fn transfer_finish_move() {
    let mut state = transfer_state();
    let transfer = shard_transfer(
        PEER_ID,
        OTHER_PEER_ID,
        false,
        ShardTransferMethod::StreamRecords,
    );
    let collection = state.collections.get_mut(COLLECTION).expect("collection");
    collection.transfers.insert(transfer.clone());
    collection
        .shards
        .get_mut(&0)
        .expect("shard")
        .replicas
        .insert(OTHER_PEER_ID, ReplicaState::Partial);

    let mut machine = state_machine(state);
    let outcome = machine.apply(&transfer_op(ShardTransferOperations::Finish(transfer)));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("finishing a transfer should be accepted, got {outcome:?}");
    };
    assert!(matches!(
        actions.as_slice(),
        [
            Action::StopTransferDriver { .. },
            Action::SetReplicaState {
                peer_id: OTHER_PEER_ID,
                state: ReplicaState::Active,
                ..
            },
            Action::InvalidateCleanLocalShards { .. },
            Action::RemoveReplica {
                peer_id: PEER_ID,
                ..
            },
            Action::UnregisterTransfer {
                outcome: TransferOutcome::Finish,
                ..
            },
        ]
    ));

    let collection = machine.state().collection(COLLECTION).expect("collection");
    assert_eq!(
        collection.shards[&0].replicas,
        HashMap::from([(OTHER_PEER_ID, ReplicaState::Active)]),
    );
    assert!(collection.transfers.is_empty());
}

#[test]
fn transfer_abort_move() {
    let mut state = transfer_state();
    let transfer = shard_transfer(
        PEER_ID,
        OTHER_PEER_ID,
        false,
        ShardTransferMethod::StreamRecords,
    );
    let collection = state.collections.get_mut(COLLECTION).expect("collection");
    collection.transfers.insert(transfer.clone());
    collection
        .shards
        .get_mut(&0)
        .expect("shard")
        .replicas
        .insert(OTHER_PEER_ID, ReplicaState::Partial);

    let mut machine = state_machine(state);
    let outcome = machine.apply(&transfer_op(ShardTransferOperations::Abort {
        transfer: transfer.key(),
        reason: "test".into(),
    }));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("aborting a transfer should be accepted, got {outcome:?}");
    };
    assert!(matches!(
        actions.as_slice(),
        [
            Action::StopTransferDriver { .. },
            Action::InvalidateCleanLocalShards { .. },
            Action::RemoveReplica {
                peer_id: OTHER_PEER_ID,
                ..
            },
            Action::RevertProxyShard { .. },
            Action::UnregisterTransfer {
                outcome: TransferOutcome::Abort,
                ..
            },
        ]
    ));

    let collection = machine.state().collection(COLLECTION).expect("collection");
    assert!(!collection.shards[&0].replicas.contains_key(&OTHER_PEER_ID));
    assert!(collection.transfers.is_empty());
}

#[test]
fn transfer_abort_resharding_clears_resharding_first() {
    let key = resharding_key(ReshardingDirection::Up, 1);
    let mut state = auto_resharding_state(1);
    let collection = state.collections.get_mut(COLLECTION).expect("collection");
    collection.shards.insert(
        1,
        ShardInfo {
            replicas: HashMap::from([(OTHER_PEER_ID, ReplicaState::Resharding)]),
        },
    );
    collection.config.params.shard_number = NonZeroU32::new(2).unwrap();
    collection.resharding = Some(ReshardState::new(
        key.uuid,
        key.direction,
        key.peer_id,
        key.shard_id,
        key.shard_key.clone(),
    ));
    let transfer = ShardTransfer {
        shard_id: 0,
        to_shard_id: Some(1),
        from: PEER_ID,
        to: OTHER_PEER_ID,
        sync: true,
        method: Some(ShardTransferMethod::ReshardingStreamRecords),
        filter: None,
    };
    collection.transfers.insert(transfer.clone());

    let mut machine = state_machine(state);
    let outcome = machine.apply(&transfer_op(ShardTransferOperations::Abort {
        transfer: transfer.key(),
        reason: "test".into(),
    }));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("aborting a resharding transfer should be accepted, got {outcome:?}");
    };
    let clear = actions
        .iter()
        .position(|action| matches!(action, Action::SetReshardingState { state: None, .. }))
        .expect("resharding clear");
    let unregister = actions
        .iter()
        .position(|action| matches!(action, Action::UnregisterTransfer { .. }))
        .expect("transfer unregister");
    assert!(clear < unregister);

    #[cfg(feature = "staging")]
    assert!(matches!(
        &actions[clear + 1..],
        [
            Action::TestPauseReshardingAbort,
            Action::StopTransferDriver { .. },
            ..
        ]
    ));

    let collection = machine.state().collection(COLLECTION).expect("collection");
    assert!(collection.resharding.is_none());
    assert!(collection.transfers.is_empty());
}

#[test]
fn transfer_recovery_to_partial() {
    let mut state = transfer_state();
    let transfer = shard_transfer(PEER_ID, OTHER_PEER_ID, true, ShardTransferMethod::Snapshot);
    let collection = state.collections.get_mut(COLLECTION).expect("collection");
    collection.transfers.insert(transfer.clone());
    collection
        .shards
        .get_mut(&0)
        .expect("shard")
        .replicas
        .insert(OTHER_PEER_ID, ReplicaState::Recovery);

    let mut machine = state_machine(state);
    let outcome = machine.apply(&transfer_op(ShardTransferOperations::RecoveryToPartial(
        transfer.key(),
    )));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("advancing recovery should be accepted, got {outcome:?}");
    };
    assert!(matches!(
        actions.as_slice(),
        [Action::SetReplicaState {
            state: ReplicaState::Partial,
            ..
        }]
    ));
}

fn transfer_op(operation: ShardTransferOperations) -> ConsensusOperations {
    collection_meta_op(CollectionMetaOperations::TransferShard(
        COLLECTION.into(),
        operation,
    ))
}

fn prevent_unoptimized_diff() -> OptimizersConfigDiff {
    OptimizersConfigDiff {
        prevent_unoptimized: Some(true),
        ..optimizers_diff()
    }
}
