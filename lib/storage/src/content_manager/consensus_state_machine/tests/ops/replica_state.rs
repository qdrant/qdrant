//! `SetShardReplicaState` operation

use std::collections::HashMap;

use collection::operations::cluster_ops::ReshardingDirection;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::resharding::ReshardState;
use collection::shards::shard::PeerId;
use collection::shards::transfer::{ShardTransfer, ShardTransferMethod};
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

/// The scale-up target is `Active` while migrations into it are still registered,
/// and it is sending an ordinary replication of the new shard
#[test]
fn set_shard_replica_state_dead_scale_up_target_aborts_every_transfer() {
    const RECEIVER_PEER_ID: PeerId = 44;

    let key = resharding_key(ReshardingDirection::Up, 2);
    let mut state = auto_resharding_state(3);
    for peer_id in [PEER_ID, OTHER_PEER_ID, RECEIVER_PEER_ID] {
        add_peer(&mut state, peer_id, Some("1.20.0"));
    }

    let collection = state.collections.get_mut(COLLECTION).expect("collection");
    collection.resharding = Some(ReshardState::new(
        key.uuid,
        key.direction,
        key.peer_id,
        key.shard_id,
        key.shard_key.clone(),
    ));
    collection.shards.get_mut(&2).expect("shard").replicas = HashMap::from([
        (PEER_ID, ReplicaState::Active),
        (OTHER_PEER_ID, ReplicaState::Active),
        (RECEIVER_PEER_ID, ReplicaState::Partial),
    ]);

    // Aborting the first migration drops shard 2 and unregisters the second one.
    // The replication is not related to resharding, so the Dead write must abort it.
    for shard_id in [0, 1] {
        collection.transfers.insert(ShardTransfer {
            shard_id,
            to_shard_id: Some(2),
            from: PEER_ID,
            to: PEER_ID,
            sync: false,
            method: Some(ShardTransferMethod::ReshardingStreamRecords),
            filter: None,
        });
    }
    collection.transfers.insert(ShardTransfer {
        shard_id: 2,
        to_shard_id: None,
        from: PEER_ID,
        to: RECEIVER_PEER_ID,
        sync: true,
        method: Some(ShardTransferMethod::StreamRecords),
        filter: None,
    });

    let operation =
        set_replica_state_op(2, PEER_ID, ReplicaState::Dead, Some(ReplicaState::Active));
    let mut machine = state_machine(state);
    let outcome = machine.apply(&operation);

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("marking a scale-up target dead should be accepted, got {outcome:?}");
    };
    assert!(
        !actions.iter().any(|action| matches!(
            action,
            Action::SetReplicaState {
                state: ReplicaState::Dead,
                ..
            }
        )),
        "the dropped shard must not get a final Dead write: {actions:?}",
    );

    let collection = machine.state().collection(COLLECTION).expect("collection");
    assert!(!collection.shards.contains_key(&2));
    assert!(collection.resharding.is_none());
    assert!(
        collection.transfers.is_empty(),
        "every transfer must be aborted, got {:?}",
        collection.transfers,
    );

    let replay = machine.plan(&operation);
    assert!(
        matches!(replay, Err(StorageError::NotFound { .. })),
        "{replay:?}"
    );
}

/// The source replica of the scale-down shard is `Active` while it migrates into two other
/// shards and sends an ordinary replication of the scale-down shard
#[test]
fn set_shard_replica_state_dead_scale_down_source_aborts_every_transfer() {
    const RECEIVER_PEER_ID: PeerId = 44;

    // Before and after `ABORT_TRANSFERS_ON_SHARD_DROP_FIX_FROM_VERSION`,
    // which changes how the Dead write collects transfers
    for version in ["1.16.0", "1.20.0"] {
        let key = resharding_key(ReshardingDirection::Down, 2);
        let mut state = auto_resharding_state(3);
        for peer_id in [PEER_ID, OTHER_PEER_ID, RECEIVER_PEER_ID] {
            add_peer(&mut state, peer_id, Some(version));
        }

        let collection = state.collections.get_mut(COLLECTION).expect("collection");
        collection.resharding = Some(ReshardState::new(
            key.uuid,
            key.direction,
            key.peer_id,
            key.shard_id,
            key.shard_key.clone(),
        ));
        for shard_id in [0, 1] {
            collection
                .shards
                .get_mut(&shard_id)
                .expect("shard")
                .replicas = HashMap::from([(PEER_ID, ReplicaState::ReshardingScaleDown)]);
        }
        collection.shards.get_mut(&2).expect("shard").replicas = HashMap::from([
            (PEER_ID, ReplicaState::Active),
            (OTHER_PEER_ID, ReplicaState::Active),
            (RECEIVER_PEER_ID, ReplicaState::Partial),
        ]);

        // The replication sorts first and is not related to resharding.
        // Aborting the first migration aborts resharding and unregisters the second one,
        // which the Dead write must then skip. Scale-down does not drop the shard,
        // so the final Dead write must still happen.
        for to_shard_id in [0, 1] {
            collection.transfers.insert(ShardTransfer {
                shard_id: 2,
                to_shard_id: Some(to_shard_id),
                from: PEER_ID,
                to: PEER_ID,
                sync: false,
                method: Some(ShardTransferMethod::ReshardingStreamRecords),
                filter: None,
            });
        }
        collection.transfers.insert(ShardTransfer {
            shard_id: 2,
            to_shard_id: None,
            from: PEER_ID,
            to: RECEIVER_PEER_ID,
            sync: true,
            method: Some(ShardTransferMethod::StreamRecords),
            filter: None,
        });

        let operation =
            set_replica_state_op(2, PEER_ID, ReplicaState::Dead, Some(ReplicaState::Active));
        let mut machine = state_machine(state);
        let outcome = machine.apply(&operation);

        let Ok(Plan { actions, .. }) = outcome else {
            panic!(
                "marking a scale-down source dead should be accepted, \
                 version {version}, got {outcome:?}"
            );
        };

        let resharding_clears = actions
            .iter()
            .filter(|action| matches!(action, Action::SetReshardingState { state: None, .. }))
            .count();
        assert_eq!(
            resharding_clears, 1,
            "resharding must be aborted once: {actions:?}"
        );
        assert!(
            matches!(
                actions.last(),
                Some(Action::SetReplicaState {
                    shard_id: 2,
                    peer_id: PEER_ID,
                    state: ReplicaState::Dead,
                    ..
                })
            ),
            "the kept shard must get the final Dead write: {actions:?}",
        );

        let collection = machine.state().collection(COLLECTION).expect("collection");
        assert!(collection.resharding.is_none());
        assert!(
            collection.transfers.is_empty(),
            "every transfer must be aborted, got {:?}",
            collection.transfers,
        );
        for shard_id in [0, 1] {
            assert_eq!(
                collection.shards[&shard_id].replicas,
                HashMap::from([(PEER_ID, ReplicaState::Active)]),
                "aborting scale-down must restore the target replicas",
            );
        }
        assert_eq!(
            collection.shards[&2].replicas,
            HashMap::from([
                (PEER_ID, ReplicaState::Dead),
                (OTHER_PEER_ID, ReplicaState::Active),
                (RECEIVER_PEER_ID, ReplicaState::Dead),
            ]),
        );

        // The replica is `Dead` now, so `from_state: Active` no longer matches
        let replay = machine.plan(&operation);
        assert!(
            matches!(replay, Err(StorageError::BadInput { .. })),
            "{replay:?}"
        );
    }
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
