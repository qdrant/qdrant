//! Peer operations: adding and removing peers, peer metadata

use std::collections::HashMap;

use collection::operations::cluster_ops::ReshardingDirection;
use collection::operations::types::PeerMetadata;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::resharding::{ReshardState, ReshardingStage};
use collection::shards::transfer::{ShardTransfer, ShardTransferMethod};
use uuid::Uuid;

use super::*;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::*;

#[test]
fn add_peer_operation() {
    let mut machine = state_machine(ClusterState::default());
    let outcome = machine.apply(&ConsensusOperations::AddPeer {
        peer_id: PEER_ID,
        uri: "http://peer-42".into(),
    });

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("adding a peer should be accepted, got {outcome:?}");
    };
    assert!(matches!(
        actions.as_slice(),
        [Action::AddPeer {
            peer_id: PEER_ID,
            ..
        }]
    ));
    assert_eq!(
        machine.state().peer_address_by_id[&PEER_ID].to_string(),
        "http://peer-42/",
    );
}

#[test]
fn add_peer_replay() {
    let mut state = ClusterState::default();
    add_peer(&mut state, PEER_ID, None);

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&ConsensusOperations::AddPeer {
        peer_id: PEER_ID,
        uri: "http://peer-42/".into(),
    });

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("re-adding a peer should be accepted, got {outcome:?}");
    };
    assert!(actions.is_empty());
    assert_eq!(machine.state(), &state);
}

#[test]
fn remove_peer_operation() {
    let mut state = auto_resharding_state(1);
    add_peer(&mut state, OTHER_PEER_ID, Some("1.18.0"));
    state
        .collections
        .get_mut(COLLECTION)
        .expect("collection")
        .shards
        .get_mut(&0)
        .expect("shard")
        .replicas = HashMap::from([(OTHER_PEER_ID, ReplicaState::Active)]);

    let mut machine = state_machine(state);
    let outcome = machine.apply(&ConsensusOperations::RemovePeer(OTHER_PEER_ID));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("removing a peer should be accepted, got {outcome:?}");
    };
    assert!(matches!(
        actions.as_slice(),
        [
            Action::RemoveReplica {
                peer_id: OTHER_PEER_ID,
                ..
            },
            Action::RemovePeer {
                peer_id: OTHER_PEER_ID,
            },
        ]
    ));

    let state = machine.state();
    assert!(!state.peer_address_by_id.contains_key(&OTHER_PEER_ID));
    assert!(!state.peer_metadata_by_id.contains_key(&OTHER_PEER_ID));
    assert!(
        !state.collection(COLLECTION).expect("collection").shards[&0]
            .replicas
            .contains_key(&OTHER_PEER_ID)
    );
}

#[test]
fn remove_peer_aborts_transfer_before_replica() {
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
        .insert(OTHER_PEER_ID, ReplicaState::Partial);
    collection.transfers.insert(transfer);

    let mut machine = state_machine(state);
    let outcome = machine.apply(&ConsensusOperations::RemovePeer(OTHER_PEER_ID));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("removing a transferring peer should be accepted, got {outcome:?}");
    };
    let unregister = actions
        .iter()
        .position(|action| matches!(action, Action::UnregisterTransfer { .. }))
        .expect("transfer unregistered");
    let remove_replica = actions
        .iter()
        .position(|action| matches!(action, Action::RemoveReplica { .. }))
        .expect("replica removed");
    let remove_peer = actions
        .iter()
        .position(|action| matches!(action, Action::RemovePeer { .. }))
        .expect("peer removed");
    assert!(unregister < remove_replica);
    assert!(remove_replica < remove_peer);
}

#[test]
fn remove_peer_aborts_related_resharding_transfers() {
    let mut state = auto_resharding_state(3);
    add_peer(&mut state, OTHER_PEER_ID, Some("1.18.0"));
    let resharding = ReshardState::new(Uuid::nil(), ReshardingDirection::Up, PEER_ID, 2, None);
    let transfers: Vec<_> = (0..2)
        .map(|shard_id| ShardTransfer {
            shard_id,
            to_shard_id: Some(2),
            from: PEER_ID,
            to: OTHER_PEER_ID,
            sync: true,
            method: Some(ShardTransferMethod::ReshardingStreamRecords),
            filter: None,
        })
        .collect();
    let collection = state.collections.get_mut(COLLECTION).expect("collection");
    collection.resharding = Some(resharding);
    collection.shards.get_mut(&2).expect("shard").replicas =
        HashMap::from([(OTHER_PEER_ID, ReplicaState::Resharding)]);
    collection.transfers.extend(transfers.iter().cloned());

    let mut machine = state_machine(state);
    let outcome = machine.apply(&ConsensusOperations::RemovePeer(OTHER_PEER_ID));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("removing a resharding transfer peer should be accepted, got {outcome:?}");
    };

    for transfer in transfers {
        let unregister_count = actions
            .iter()
            .filter(|action| {
                matches!(action, Action::UnregisterTransfer { key, .. } if *key == transfer.key())
            })
            .count();
        assert_eq!(unregister_count, 1);
    }
    assert!(matches!(
        actions.last(),
        Some(Action::RemovePeer {
            peer_id: OTHER_PEER_ID
        })
    ));

    let state = machine.state();
    let collection = state.collection(COLLECTION).expect("collection");
    assert!(collection.resharding.is_none());
    assert!(collection.transfers.is_empty());
    assert_eq!(collection.config.params.shard_number.get(), 2);
    assert_eq!(collection.shards.len(), 2);
    assert!(
        collection
            .shards
            .values()
            .all(|shard| { shard.replicas == HashMap::from([(PEER_ID, ReplicaState::Active)]) })
    );
    assert!(!state.peer_address_by_id.contains_key(&OTHER_PEER_ID));
    assert!(!state.peer_metadata_by_id.contains_key(&OTHER_PEER_ID));
}

#[test]
fn remove_peer_force_aborts_resharding() {
    let mut state = auto_resharding_state(2);
    add_peer(&mut state, OTHER_PEER_ID, Some("1.18.0"));
    let mut resharding =
        ReshardState::new(Uuid::nil(), ReshardingDirection::Up, OTHER_PEER_ID, 1, None);
    resharding.stage = ReshardingStage::ReadHashRingCommitted;
    state
        .collections
        .get_mut(COLLECTION)
        .expect("collection")
        .resharding = Some(resharding);

    let mut machine = state_machine(state);
    let outcome = machine.apply(&ConsensusOperations::RemovePeer(OTHER_PEER_ID));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("removing a resharding peer should be accepted, got {outcome:?}");
    };
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::SetReshardingState { state: None, .. }))
    );
    #[cfg(feature = "staging")]
    assert!(!actions.iter().any(|action| matches!(
        action,
        Action::TestPauseReshardingAbort | Action::TestCrashAfterScaleDownStateClear
    )));
    assert!(matches!(
        actions.last(),
        Some(Action::RemovePeer {
            peer_id: OTHER_PEER_ID,
        })
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

#[test]
fn remove_peer_replay() {
    let mut state = ClusterState::default();
    add_peer(&mut state, OTHER_PEER_ID, Some("1.18.0"));
    let operation = ConsensusOperations::RemovePeer(OTHER_PEER_ID);
    let mut machine = state_machine(state);
    machine.apply(&operation).unwrap();
    let goal = machine.state().clone();

    let outcome = machine.apply(&operation);

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("replaying peer removal should be accepted, got {outcome:?}");
    };
    assert!(matches!(
        actions.as_slice(),
        [Action::RemovePeer {
            peer_id: OTHER_PEER_ID,
        }]
    ));
    assert_eq!(machine.state(), &goal);
}

#[test]
fn update_peer_metadata() {
    let mut machine = state_machine(ClusterState::default());
    let outcome = machine.apply(&update_peer_metadata_op(PEER_ID, "1.15.0"));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("metadata of a peer without any should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::SetPeerMetadata { .. }],
    ));

    assert_eq!(
        machine.state().peer_metadata_by_id.get(&PEER_ID),
        Some(&peer_metadata("1.15.0")),
    );
}

#[test]
fn update_peer_metadata_replace() {
    let mut state = ClusterState::default();

    state
        .peer_metadata_by_id
        .insert(PEER_ID, peer_metadata("1.14.0"));

    let mut machine = state_machine(state);
    let outcome = machine.apply(&update_peer_metadata_op(PEER_ID, "1.15.0"));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("a peer reporting a new version should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::SetPeerMetadata { .. }],
    ));

    assert_eq!(
        machine.state().peer_metadata_by_id.get(&PEER_ID),
        Some(&peer_metadata("1.15.0")),
    );
}

#[test]
fn update_peer_metadata_replay() {
    let mut state = ClusterState::default();

    state
        .peer_metadata_by_id
        .insert(PEER_ID, peer_metadata("1.15.0"));

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&update_peer_metadata_op(PEER_ID, "1.15.0"));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("replay of applied metadata should be accepted, got {outcome:?}");
    };

    // Nothing left to do: metadata is absolute and the applier only writes it
    assert!(actions.is_empty());

    assert_eq!(machine.state(), &state);
}

fn update_peer_metadata_op(peer_id: PeerId, version: &str) -> ConsensusOperations {
    ConsensusOperations::UpdatePeerMetadata {
        peer_id,
        metadata: peer_metadata(version),
    }
}

fn peer_metadata(version: &str) -> PeerMetadata {
    PeerMetadata::new(version.parse().expect("valid version"))
}
