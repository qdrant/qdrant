//! `UpdateCollection` operation, including replica changes

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU32;

use collection::operations::config_diff::{
    CollectionParamsDiff, HnswConfigDiff, QuantizationConfigDiff,
};
use collection::operations::types::{
    SparseVectorParams, SparseVectorsConfig, VectorParamsDiff, VectorsConfigDiff,
};
use collection::shards::replica_set;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::transfer::ShardTransferMethod;
use segment::data_types::modifier::Modifier;
use segment::types::*;
use serde_json::json;

use super::*;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::*;
use crate::content_manager::errors::StorageError;

#[test]
fn update_collection() {
    let mut update = empty_update();
    update.hnsw_config = Some(hnsw_diff(8));

    let mut machine = state_machine(cluster_state(Vec::new()));
    let outcome = machine.apply(&update_collection_op(update));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("updating a collection should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::UpdateCollectionConfig { .. }]
    ));

    assert_eq!(collection_config(&machine).hnsw_config.m, 8);
}

#[test]
fn update_collection_diff_order() {
    let update = UpdateCollection {
        vectors: Some(vectors_diff("text")),
        optimizers_config: Some(optimizers_diff()),
        params: Some(params_diff()),
        hnsw_config: Some(hnsw_diff(8)),
        quantization_config: Some(QuantizationConfigDiff::new_disabled()),
        sparse_vectors: Some(sparse_vectors_diff("sparse")),
        strict_mode_config: Some(strict_mode_diff(true)),
        metadata: Some(metadata(json!({ "region": "eu" }))),
    };

    let mut machine = state_machine(cluster_state(vec![
        ("text", dense(4, Distance::Cosine)),
        ("sparse", sparse()),
    ]));

    let outcome = machine.apply(&update_collection_op(update));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("updating a collection should be accepted, got {outcome:?}");
    };

    // Same order `TableOfContent::update_collection` saves them in
    let kinds: Vec<_> = actions.iter().map(config_diff_kind).collect();

    assert_eq!(
        kinds,
        [
            "optimizers",
            "params",
            "hnsw",
            "vectors",
            "quantization",
            "sparse vectors",
            "strict mode",
            "metadata",
        ],
    );
}

#[test]
fn update_collection_replay() {
    let mut update = empty_update();
    update.hnsw_config = Some(hnsw_diff(8));
    update.strict_mode_config = Some(strict_mode_diff(true));
    update.metadata = Some(metadata(json!({ "region": "eu" })));

    let mut machine = state_machine(cluster_state(Vec::new()));
    machine
        .apply(&update_collection_op(update.clone()))
        .unwrap();

    let applied = machine.state().clone();
    let outcome = machine.apply(&update_collection_op(update));

    let Ok(_) = outcome else {
        panic!("replay of an applied update should be accepted, got {outcome:?}");
    };

    assert_eq!(
        machine.state(),
        &applied,
        "replay should not change anything"
    );
}

#[test]
fn update_collection_reject_vector_name() {
    let mut update = empty_update();
    update.hnsw_config = Some(hnsw_diff(8));
    update.vectors = Some(vectors_diff("missing"));

    update_collection_reject_whole_operation(update);
}

#[test]
fn update_collection_reject_sparse_vector_name() {
    let mut update = empty_update();
    update.hnsw_config = Some(hnsw_diff(8));
    update.sparse_vectors = Some(sparse_vectors_diff("missing"));

    update_collection_reject_whole_operation(update);
}

/// A diff rejected in the middle keeps nothing, not even the diffs before it
fn update_collection_reject_whole_operation(update: UpdateCollection) {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&update_collection_op(update));

    assert!(matches!(outcome, Err(StorageError::BadInput { .. })));

    assert_eq!(machine.state(), &state);
}

#[test]
fn update_collection_metadata_merge() {
    let mut state = cluster_state(Vec::new());
    state
        .collections
        .get_mut(COLLECTION)
        .expect("collection exists")
        .config
        .metadata = Some(metadata(json!({ "region": "eu", "tier": "gold" })));

    let mut update = empty_update();
    update.metadata = Some(metadata(json!({ "region": null, "size": 2 })));

    let mut machine = state_machine(state);
    machine.apply(&update_collection_op(update)).unwrap();

    // Merged into what is there, and a null value removes its key
    assert_eq!(
        collection_config(&machine).metadata,
        Some(metadata(json!({ "tier": "gold", "size": 2 }))),
    );
}

#[test]
fn update_collection_metadata_null_without_metadata() {
    let mut update = empty_update();
    update.metadata = Some(metadata(json!({ "region": null })));

    let mut machine = state_machine(cluster_state(Vec::new()));
    machine.apply(&update_collection_op(update)).unwrap();

    // A collection with no metadata takes the payload as it is, so the null is stored.
    // A replay merges the payload into itself and drops the key.
    assert_eq!(
        collection_config(&machine).metadata,
        Some(metadata(json!({ "region": null }))),
    );
}

#[test]
fn update_collection_replica_changes() {
    let mut operation = UpdateCollectionOperation::new_empty(COLLECTION.into());
    operation.set_shard_replica_changes(vec![replica_set::Change::Remove(0, OTHER_PEER_ID)]);

    let mut state = auto_resharding_state(1);
    state
        .collections
        .get_mut(COLLECTION)
        .expect("collection")
        .shards
        .get_mut(&0)
        .expect("shard")
        .replicas
        .insert(OTHER_PEER_ID, ReplicaState::Active);

    let mut machine = state_machine(state);
    let outcome = machine.apply(&collection_meta_op(
        CollectionMetaOperations::UpdateCollection(operation),
    ));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("removing a replica should be accepted, got {outcome:?}");
    };
    assert!(matches!(
        actions.as_slice(),
        [Action::RemoveReplica {
            peer_id: OTHER_PEER_ID,
            ..
        }]
    ));
    assert!(
        !machine
            .state()
            .collection(COLLECTION)
            .expect("collection")
            .shards[&0]
            .replicas
            .contains_key(&OTHER_PEER_ID)
    );
}

#[test]
fn update_collection_replica_change_aborts_transfer_before_removal() {
    let mut operation = UpdateCollectionOperation::new_empty(COLLECTION.into());
    operation.set_shard_replica_changes(vec![replica_set::Change::Remove(0, OTHER_PEER_ID)]);

    let mut state = transfer_state();
    let transfer = shard_transfer(
        PEER_ID,
        OTHER_PEER_ID,
        false,
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
    let outcome = machine.apply(&collection_meta_op(
        CollectionMetaOperations::UpdateCollection(operation),
    ));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("removing a transfer destination should be accepted, got {outcome:?}");
    };
    assert!(matches!(
        actions.as_slice(),
        [
            Action::StopTransferDriver { .. },
            Action::RevertProxyShard { .. },
            Action::UnregisterTransfer { .. },
            Action::RemoveReplica {
                shard_id: 0,
                peer_id: OTHER_PEER_ID,
                ..
            },
        ]
    ));
}

#[test]
fn update_collection_replica_change_guard_rejects_partial_replay() {
    for sync in [false, true] {
        for source_state in [ReplicaState::Initializing, ReplicaState::Active] {
            update_collection_replica_change_guard_rejects_partial_replay_case(sync, source_state);
        }
    }
}

fn update_collection_replica_change_guard_rejects_partial_replay_case(
    sync: bool,
    source_state: ReplicaState,
) {
    let mut state = transfer_state();
    let transfer = shard_transfer(PEER_ID, OTHER_PEER_ID, sync, ShardTransferMethod::Snapshot);
    let collection = state.collections.get_mut(COLLECTION).expect("collection");
    collection.shards.get_mut(&0).expect("shard").replicas = HashMap::from([
        (PEER_ID, source_state),
        (OTHER_PEER_ID, ReplicaState::Active),
    ]);
    collection.transfers.insert(transfer);

    let mut operation = UpdateCollectionOperation::new_empty(COLLECTION.into());
    operation.set_shard_replica_changes(vec![replica_set::Change::Remove(0, PEER_ID)]);
    let operation = collection_meta_op(CollectionMetaOperations::UpdateCollection(operation));
    let mut machine = state_machine(state.clone());
    let Ok(Plan { actions, .. }) = machine.apply(&operation) else {
        panic!("removing a replica with another active replica should be accepted");
    };
    let goal = machine.state().clone();

    let mut partial_rejections = 0;
    for crash_after in 0..=actions.len() {
        let mut crashed = state.clone();
        for action in &actions[..crash_after] {
            crashed.apply_action(action);
        }
        let reached_goal = crashed == goal;
        let mut replay = state_machine(crashed);
        match replay.apply(&operation) {
            Ok(_) => assert_eq!(replay.state(), &goal),
            Err(error) if !reached_goal => {
                partial_rejections += 1;
                assert!(super::replay::replica_removal_guard_may_reject(
                    &state,
                    &operation,
                    &actions[..crash_after],
                    &error,
                ));
                assert!(!super::replay::replica_removal_guard_may_reject(
                    &state,
                    &operation,
                    &actions[..1],
                    &error,
                ));
                let unrelated_error =
                    StorageError::bad_request(format!("Peer {PEER_ID} has no replica of shard 0"));
                assert!(!super::replay::replica_removal_guard_may_reject(
                    &state,
                    &operation,
                    &actions[..crash_after],
                    &unrelated_error,
                ));
            }
            Err(_) => {}
        }
    }
    assert_eq!(partial_rejections, 3);
}

#[test]
fn update_collection_replica_change_order() {
    let mut operation = UpdateCollectionOperation::new_empty(COLLECTION.into());
    operation.update_collection.hnsw_config = Some(hnsw_diff(8));
    operation.update_collection.strict_mode_config = Some(strict_mode_diff(true));
    operation.set_shard_replica_changes(vec![replica_set::Change::Remove(0, OTHER_PEER_ID)]);

    let mut state = auto_resharding_state(1);
    state
        .collections
        .get_mut(COLLECTION)
        .expect("collection")
        .shards
        .get_mut(&0)
        .expect("shard")
        .replicas
        .insert(OTHER_PEER_ID, ReplicaState::Active);

    let mut machine = state_machine(state);
    let outcome = machine.apply(&collection_meta_op(
        CollectionMetaOperations::UpdateCollection(operation),
    ));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("updating config and replicas should be accepted, got {outcome:?}");
    };
    assert!(matches!(
        actions.as_slice(),
        [
            Action::UpdateCollectionConfig {
                diff,
                ..
            },
            Action::RemoveReplica { .. },
            Action::UpdateCollectionConfig {
                diff: strict,
                ..
            },
        ] if matches!(**diff, CollectionConfigDiff::Hnsw(_))
            && matches!(**strict, CollectionConfigDiff::StrictMode(_))
    ));
}

#[test]
fn update_collection_replica_change_replay_rejects_complete() {
    let mut operation = UpdateCollectionOperation::new_empty(COLLECTION.into());
    operation.set_shard_replica_changes(vec![replica_set::Change::Remove(0, OTHER_PEER_ID)]);

    let mut state = auto_resharding_state(1);
    state
        .collections
        .get_mut(COLLECTION)
        .expect("collection")
        .shards
        .get_mut(&0)
        .expect("shard")
        .replicas
        .insert(OTHER_PEER_ID, ReplicaState::Active);

    let op = collection_meta_op(CollectionMetaOperations::UpdateCollection(operation));
    let mut machine = state_machine(state);
    machine.apply(&op).unwrap();
    let goal = machine.state().clone();

    let outcome = machine.apply(&op);

    assert!(matches!(outcome, Err(StorageError::BadRequest { .. })));
    assert_eq!(machine.state(), &goal);
}

/// Aborting a transfer whose source is being removed marks its target `Dead` (sync) or
/// removes it (a move). From 1.20.3 the removal is validated against the map the abort
/// leaves behind, so it is rejected when the target was the only other replica that can
/// serve as a source of truth. Before, the removal committed and the replay of its own
/// abort's prefix rejected it.
#[test]
fn update_collection_replica_change_validates_post_abort_map() {
    for sync in [false, true] {
        for source_state in [ReplicaState::Initializing, ReplicaState::Active] {
            let mut state = auto_resharding_state(1);
            add_peer(&mut state, PEER_ID, Some("1.20.3-dev"));
            add_peer(&mut state, OTHER_PEER_ID, Some("1.20.3-dev"));
            let collection = state.collections.get_mut(COLLECTION).expect("collection");
            collection.shards.get_mut(&0).expect("shard").replicas = HashMap::from([
                (PEER_ID, source_state),
                (OTHER_PEER_ID, ReplicaState::Active),
            ]);
            collection.transfers.insert(shard_transfer(
                PEER_ID,
                OTHER_PEER_ID,
                sync,
                ShardTransferMethod::Snapshot,
            ));

            let mut operation = UpdateCollectionOperation::new_empty(COLLECTION.into());
            operation.set_shard_replica_changes(vec![replica_set::Change::Remove(0, PEER_ID)]);

            let mut machine = state_machine(state.clone());
            let outcome = machine.apply(&collection_meta_op(
                CollectionMetaOperations::UpdateCollection(operation),
            ));

            let Err(StorageError::BadRequest { description }) = &outcome else {
                panic!(
                    "removing the source must be rejected once the abort kills the target \
                     (sync={sync}, source={source_state:?}), got {outcome:?}",
                );
            };
            assert_eq!(
                *description,
                format!("Shard 0 must have at least one active replica after removing {PEER_ID}"),
            );
            assert_eq!(
                machine.state(),
                &state,
                "a rejected removal changes nothing"
            );
        }
    }
}

/// A removal that still leaves a source of truth behind is accepted, and replaying it from
/// any prefix of its actions reaches the same state.
#[test]
fn update_collection_replica_change_post_abort_accept_replays() {
    const THIRD_PEER_ID: u64 = 44;

    for sync in [false, true] {
        let mut state = auto_resharding_state(1);
        add_peer(&mut state, PEER_ID, Some("1.20.3-dev"));
        add_peer(&mut state, OTHER_PEER_ID, Some("1.20.3-dev"));
        add_peer(&mut state, THIRD_PEER_ID, Some("1.20.3-dev"));
        let collection = state.collections.get_mut(COLLECTION).expect("collection");
        collection.shards.get_mut(&0).expect("shard").replicas = HashMap::from([
            (PEER_ID, ReplicaState::Active),
            (OTHER_PEER_ID, ReplicaState::Active),
            (THIRD_PEER_ID, ReplicaState::Active),
        ]);
        collection.transfers.insert(shard_transfer(
            PEER_ID,
            OTHER_PEER_ID,
            sync,
            ShardTransferMethod::Snapshot,
        ));

        let mut operation = UpdateCollectionOperation::new_empty(COLLECTION.into());
        operation.set_shard_replica_changes(vec![replica_set::Change::Remove(0, PEER_ID)]);
        let operation = collection_meta_op(CollectionMetaOperations::UpdateCollection(operation));

        let mut machine = state_machine(state.clone());
        let Ok(Plan { actions, .. }) = machine.apply(&operation) else {
            panic!("removal that keeps a source of truth should be accepted (sync={sync})");
        };
        let goal = machine.state().clone();

        // The abort kills the transfer target, the source is removed, the third replica stays.
        let replicas = &goal.collection(COLLECTION).expect("collection").shards[&0].replicas;
        assert!(!replicas.contains_key(&PEER_ID));
        if sync {
            assert_eq!(replicas[&OTHER_PEER_ID], ReplicaState::Dead);
        } else {
            assert!(!replicas.contains_key(&OTHER_PEER_ID));
        }
        assert_eq!(replicas[&THIRD_PEER_ID], ReplicaState::Active);

        for crash_after in 0..=actions.len() {
            let mut crashed = state.clone();
            for action in &actions[..crash_after] {
                crashed.apply_action(action);
            }

            let reached_goal = crashed == goal;
            let mut replay = state_machine(crashed);
            match replay.apply(&operation) {
                Ok(_) => assert_eq!(replay.state(), &goal),
                Err(error) => assert!(
                    reached_goal,
                    "replay after {crash_after} of {} actions rejected before the goal: {error}",
                    actions.len(),
                ),
            }
        }
    }
}

/// Each removal in a batch is validated against the state the earlier removals leave
/// behind, so a batch cannot remove the last source of truth one replica at a time.
#[test]
fn update_collection_replica_changes_validated_against_earlier_removals() {
    let mut state = auto_resharding_state(1);
    add_peer(&mut state, PEER_ID, Some("1.20.3-dev"));
    add_peer(&mut state, OTHER_PEER_ID, Some("1.20.3-dev"));
    let collection = state.collections.get_mut(COLLECTION).expect("collection");
    collection.shards.get_mut(&0).expect("shard").replicas = HashMap::from([
        (PEER_ID, ReplicaState::Active),
        (OTHER_PEER_ID, ReplicaState::Active),
    ]);

    let mut operation = UpdateCollectionOperation::new_empty(COLLECTION.into());
    operation.set_shard_replica_changes(vec![
        replica_set::Change::Remove(0, PEER_ID),
        replica_set::Change::Remove(0, OTHER_PEER_ID),
    ]);

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&collection_meta_op(
        CollectionMetaOperations::UpdateCollection(operation),
    ));

    assert!(
        matches!(outcome, Err(StorageError::BadRequest { .. })),
        "removing both replicas of a shard in one batch must be rejected, got {outcome:?}",
    );
    assert_eq!(machine.state(), &state);
}

fn update_collection_op(update: UpdateCollection) -> ConsensusOperations {
    let operation =
        UpdateCollectionOperation::new(COLLECTION.into(), update).expect("valid operation");

    collection_meta_op(CollectionMetaOperations::UpdateCollection(operation))
}

/// Update with every diff absent, for a test to fill in the ones it covers
fn empty_update() -> UpdateCollection {
    UpdateCollectionOperation::new_empty(COLLECTION.into()).update_collection
}

fn collection_config(machine: &ConsensusStateMachine) -> &CollectionConfigInternal {
    &machine
        .state()
        .collection(COLLECTION)
        .expect("collection exists")
        .config
}

fn config_diff_kind(action: &Action) -> &'static str {
    let Action::UpdateCollectionConfig { diff, .. } = action else {
        panic!("expected a config update, got {action:?}");
    };

    match **diff {
        CollectionConfigDiff::Optimizers(_) => "optimizers",
        CollectionConfigDiff::Params(_) => "params",
        CollectionConfigDiff::Hnsw(_) => "hnsw",
        CollectionConfigDiff::Vectors(_) => "vectors",
        CollectionConfigDiff::Quantization(_) => "quantization",
        CollectionConfigDiff::SparseVectors(_) => "sparse vectors",
        CollectionConfigDiff::StrictMode(_) => "strict mode",
        CollectionConfigDiff::Metadata(_) => "metadata",
    }
}

fn hnsw_diff(m: usize) -> HnswConfigDiff {
    HnswConfigDiff {
        m: Some(m),
        ..Default::default()
    }
}

fn params_diff() -> CollectionParamsDiff {
    CollectionParamsDiff {
        replication_factor: NonZeroU32::new(2),
        write_consistency_factor: None,
        read_fan_out_factor: None,
        read_fan_out_delay_ms: None,
        #[expect(deprecated)]
        on_disk_payload: None,
        payload: None,
        id_tracker: None,
    }
}

fn vectors_diff(vector_name: &str) -> VectorsConfigDiff {
    let params = VectorParamsDiff {
        hnsw_config: Some(hnsw_diff(8)),
        quantization_config: None,
        #[expect(deprecated)]
        on_disk: None,
        memory: None,
    };

    VectorsConfigDiff(BTreeMap::from([(vector_name.into(), params)]))
}

fn sparse_vectors_diff(vector_name: &str) -> SparseVectorsConfig {
    let params = SparseVectorParams {
        index: None,
        modifier: Some(Modifier::Idf),
    };

    SparseVectorsConfig(BTreeMap::from([(vector_name.into(), params)]))
}

fn strict_mode_diff(enabled: bool) -> StrictModeConfig {
    StrictModeConfig {
        enabled: Some(enabled),
        ..Default::default()
    }
}
