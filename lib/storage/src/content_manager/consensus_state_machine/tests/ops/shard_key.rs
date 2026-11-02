//! `CreateShardKey` and `DropShardKey` operations

use std::collections::{BTreeSet, HashMap, HashSet};

use collection::collection_state::ShardInfo;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use segment::types::*;

use super::*;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::*;
use crate::content_manager::errors::StorageError;

#[test]
fn create_shard_key() {
    let old_key = ShardKey::from("old");
    let shard_key = ShardKey::from("north");
    let placement = vec![vec![PEER_ID, OTHER_PEER_ID], vec![OTHER_PEER_ID]];

    let mut state = custom_sharding_state();
    add_peer(&mut state, PEER_ID, None);
    add_peer(&mut state, OTHER_PEER_ID, None);

    let collection = state
        .collections
        .get_mut(COLLECTION)
        .expect("collection exists");
    collection.shards.insert(
        4,
        ShardInfo {
            replicas: HashMap::from([(PEER_ID, ReplicaState::Active)]),
        },
    );
    collection
        .shards_key_mapping
        .entry(old_key)
        .or_default()
        .insert(4);

    let mut machine = state_machine(state);
    let outcome = machine.apply(&create_shard_key_op(
        shard_key.clone(),
        placement.clone(),
        Some(ReplicaState::Partial),
    ));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("creating a shard key should be accepted, got {outcome:?}");
    };

    assert_eq!(
        actions,
        vec![Action::CreateAndRegisterShards {
            collection: COLLECTION.into(),
            shard_key: Some(shard_key.clone()),
            shards: vec![
                (5, placement[0].clone(), ReplicaState::Partial),
                (6, placement[1].clone(), ReplicaState::Partial),
            ],
        },],
    );

    let collection = machine
        .state()
        .collection(COLLECTION)
        .expect("collection exists");
    let shard_ids: BTreeSet<_> = collection.shards_key_mapping[&shard_key]
        .iter()
        .copied()
        .collect();

    assert_eq!(shard_ids, BTreeSet::from([5, 6]));
    assert_eq!(
        collection.shards[&5].replicas,
        HashMap::from([
            (PEER_ID, ReplicaState::Partial),
            (OTHER_PEER_ID, ReplicaState::Partial),
        ]),
    );
    assert_eq!(
        collection.shards[&6].replicas,
        HashMap::from([(OTHER_PEER_ID, ReplicaState::Partial)]),
    );
}

#[test]
fn create_shard_key_default_initial_state() {
    let cases = [
        (true, "1.14.2", ReplicaState::Initializing),
        (true, "1.14.0", ReplicaState::Active),
        (false, "1.14.2", ReplicaState::Active),
    ];

    for (is_distributed, version, expected) in cases {
        let mut context = node_context();
        context.is_distributed = is_distributed;

        let mut state = custom_sharding_state();
        add_peer(&mut state, PEER_ID, Some(version));

        let mut machine = ConsensusStateMachine::new(state, context);
        let outcome = machine.apply(&create_shard_key_op(
            "north".into(),
            vec![vec![PEER_ID]],
            None,
        ));

        let Ok(Plan { actions, .. }) = outcome else {
            panic!("creating a shard key should be accepted, got {outcome:?}");
        };

        assert!(
            matches!(
                actions.as_slice(),
                [
                    Action::CreateAndRegisterShards { shards, .. },
                ] if shards.as_slice() == [(1, vec![PEER_ID], expected)]
            ),
            "wrong initial state for distributed={is_distributed}, version={version}: {actions:?}"
        );

        let replica_state = machine
            .state()
            .collection(COLLECTION)
            .expect("collection exists")
            .shards[&1]
            .replicas[&PEER_ID];

        assert_eq!(replica_state, expected);
    }
}

#[test]
fn create_shard_key_replay() {
    let mut state = custom_sharding_state();
    add_peer(&mut state, PEER_ID, None);

    let operation = create_shard_key_op(
        "north".into(),
        vec![vec![PEER_ID]],
        Some(ReplicaState::Active),
    );

    let mut machine = state_machine(state);
    let first = machine.apply(&operation);
    assert!(first.is_ok());

    let applied = machine.state().clone();
    let replay = machine.apply(&operation);

    assert!(matches!(replay, Err(StorageError::BadRequest { .. })));
    assert_eq!(
        machine.state(),
        &applied,
        "replay should not change anything"
    );
}

#[test]
fn create_shard_key_reject_auto_sharding() {
    let mut state = cluster_state(Vec::new());
    add_peer(&mut state, PEER_ID, None);

    create_shard_key_rejects_without_change(
        state,
        create_shard_key_op(
            "north".into(),
            vec![vec![PEER_ID]],
            Some(ReplicaState::Active),
        ),
    );
}

#[test]
fn create_shard_key_reject_existing() {
    let shard_key = ShardKey::from("north");
    let mut state = custom_sharding_state();
    add_peer(&mut state, PEER_ID, None);
    state
        .collections
        .get_mut(COLLECTION)
        .expect("collection exists")
        .shards_key_mapping
        .insert(shard_key.clone(), Default::default());

    create_shard_key_rejects_without_change(
        state,
        create_shard_key_op(shard_key, vec![vec![PEER_ID]], Some(ReplicaState::Active)),
    );
}

#[test]
fn create_shard_key_reject_empty_placement() {
    create_shard_key_rejects_without_change(
        custom_sharding_state(),
        create_shard_key_op("north".into(), Vec::new(), Some(ReplicaState::Active)),
    );
}

#[test]
fn create_shard_key_reject_unknown_peer() {
    create_shard_key_rejects_without_change(
        custom_sharding_state(),
        create_shard_key_op(
            "north".into(),
            vec![vec![PEER_ID]],
            Some(ReplicaState::Active),
        ),
    );
}

#[test]
fn drop_shard_key() {
    let shard_key = ShardKey::from("north");
    let other_key = ShardKey::from("south");
    let mut state = custom_sharding_state();
    add_shard_key(&mut state, shard_key.clone(), &[3, 1]);
    add_shard_key(&mut state, other_key.clone(), &[5]);
    set_resharding(&mut state, other_key.clone(), 5);

    let mut machine = state_machine(state);
    let outcome = machine.apply(&drop_shard_key_op(shard_key.clone()));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("dropping a shard key should be accepted, got {outcome:?}");
    };

    assert_eq!(
        actions,
        vec![
            Action::InvalidateCleanLocalShards {
                collection: COLLECTION.into(),
                shard_ids: vec![1, 3],
            },
            Action::RemoveShardKey {
                collection: COLLECTION.into(),
                shard_key: shard_key.clone(),
            },
            Action::DropShard {
                collection: COLLECTION.into(),
                shard_id: 1,
            },
            Action::DropShard {
                collection: COLLECTION.into(),
                shard_id: 3,
            },
        ],
    );

    let collection = machine
        .state()
        .collection(COLLECTION)
        .expect("collection exists");

    assert!(!collection.shards_key_mapping.contains_key(&shard_key));
    assert!(!collection.shards.contains_key(&1));
    assert!(!collection.shards.contains_key(&3));
    assert_eq!(
        collection.shards_key_mapping[&other_key],
        HashSet::from([5])
    );
    assert!(collection.shards.contains_key(&5));
    assert_eq!(
        collection
            .resharding
            .as_ref()
            .and_then(|resharding| resharding.shard_key.as_ref()),
        Some(&other_key),
    );
}

#[test]
fn drop_shard_key_replay() {
    let state = custom_sharding_state();

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&drop_shard_key_op("north".into()));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("replay of an applied shard-key drop should be accepted, got {outcome:?}");
    };

    assert!(actions.is_empty());
    assert_eq!(machine.state(), &state, "replay should not change anything");
}

#[test]
fn drop_shard_key_replay_after_mapping_removal() {
    let shard_key = ShardKey::from("north");
    let mut state = custom_sharding_state();
    add_shard_key(&mut state, shard_key.clone(), &[1, 2]);

    let operation = drop_shard_key_op(shard_key);
    let mut uncrashed = state_machine(state.clone());
    let outcome = uncrashed.apply(&operation);
    let Ok(Plan { actions, .. }) = outcome else {
        panic!("dropping a shard key should be accepted, got {outcome:?}");
    };
    let goal = uncrashed.state().clone();

    assert!(matches!(&actions[1], Action::RemoveShardKey { .. }));

    let mut crashed = state;
    for action in &actions[..=1] {
        crashed.apply_action(action);
    }

    let mut replay = state_machine(crashed);
    let outcome = replay.apply(&operation);

    let Ok(Plan {
        actions: replay_actions,
        ..
    }) = outcome
    else {
        panic!("replay after mapping removal should be accepted, got {outcome:?}");
    };

    assert!(replay_actions.is_empty());
    assert_eq!(replay.state(), &goal);
}

#[test]
fn drop_shard_key_reject_auto_sharding() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&drop_shard_key_op("north".into()));

    assert!(matches!(outcome, Err(StorageError::BadRequest { .. })));
    assert_eq!(machine.state(), &state);
}

#[test]
fn drop_shard_key_resharding_same_key() {
    let shard_key = ShardKey::from("north");
    let mut state = custom_sharding_state();
    add_shard_key(&mut state, shard_key.clone(), &[1]);
    set_resharding(&mut state, shard_key.clone(), 1);

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&drop_shard_key_op(shard_key));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("dropping a resharding shard key should be accepted, got {outcome:?}");
    };
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::SetReshardingState { state: None, .. }))
    );
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::RemoveShardKey { .. }))
    );

    let collection = machine.state().collection(COLLECTION).expect("collection");
    assert!(collection.resharding.is_none());
    assert!(collection.shards_key_mapping.is_empty());
}

fn create_shard_key_op(
    shard_key: ShardKey,
    placement: Vec<Vec<PeerId>>,
    initial_state: Option<ReplicaState>,
) -> ConsensusOperations {
    collection_meta_op(CollectionMetaOperations::CreateShardKey(CreateShardKey {
        collection_name: COLLECTION.into(),
        shard_key,
        placement,
        initial_state,
    }))
}

fn create_shard_key_rejects_without_change(state: ClusterState, operation: ConsensusOperations) {
    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&operation);

    assert!(matches!(outcome, Err(StorageError::BadRequest { .. })));
    assert_eq!(machine.state(), &state);
}
