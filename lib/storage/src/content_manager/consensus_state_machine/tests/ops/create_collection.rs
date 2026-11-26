//! `CreateCollection` operation

use collection::config::ShardingMethod;
use collection::shards::shard::ShardId;
use segment::data_types::collection_defaults::CollectionConfigDefaults;
use serde_json::{Value, json};

use super::*;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::*;
use crate::content_manager::errors::StorageError;
use crate::content_manager::shard_distribution::ShardDistributionProposal;

#[test]
fn create_collection() {
    let mut machine = state_machine(ClusterState::default());
    let outcome = machine.apply(&create_collection_op(
        create_collection_request(),
        Some(vec![vec![PEER_ID]]),
    ));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("creating a collection should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::CreateCollection { .. }]
    ));

    // Config comes from the node, and the one shard the proposer placed is local and initializing
    let mut expected = collection_state(Vec::new());
    expected.shards = shards(vec![vec![PEER_ID]]);

    assert_eq!(machine.state().collection(COLLECTION), Some(&expected));
}

#[test]
fn create_collection_distribution() {
    let mut machine = state_machine(ClusterState::default());
    let outcome = machine.apply(&create_collection_op(
        create_collection_request(),
        Some(vec![vec![PEER_ID, OTHER_PEER_ID], vec![OTHER_PEER_ID]]),
    ));

    let Ok(_) = outcome else {
        panic!("creating a collection should be accepted, got {outcome:?}");
    };

    let state = machine.state().collection(COLLECTION).expect("created");

    assert_eq!(
        state.shards,
        shards(vec![vec![PEER_ID, OTHER_PEER_ID], vec![OTHER_PEER_ID],])
    );

    // Auto sharding takes the shard number from the distribution
    assert_eq!(state.config.params.shard_number.get(), 2);
}

#[test]
fn create_collection_custom_sharding() {
    let mut create_collection = create_collection_request();
    create_collection.sharding_method = Some(ShardingMethod::Custom);

    let mut machine = state_machine(ClusterState::default());
    machine
        .apply(&create_collection_op(create_collection, None))
        .unwrap();

    let state = machine.state().collection(COLLECTION).expect("created");

    // Shards are created with the shard key, and the shard number counts shards per key
    assert!(state.shards.is_empty());
    assert_eq!(state.config.params.shard_number.get(), 1);
}

#[test]
fn create_collection_defaults() {
    let mut context = node_context();
    context.collection_defaults = Some(CollectionConfigDefaults {
        shard_number: Some(3),
        replication_factor: Some(2),
        shard_number_per_node: None,
        write_consistency_factor: None,
        vectors: None,
        quantization: None,
        strict_mode: None,
    });

    // Without a distribution this node places every shard on itself, as many as it defaults to
    let mut machine = ConsensusStateMachine::new(ClusterState::default(), context);
    machine
        .apply(&create_collection_op(create_collection_request(), None))
        .unwrap();

    let state = machine.state().collection(COLLECTION).expect("created");

    assert_eq!(
        state.shards,
        shards(vec![vec![PEER_ID], vec![PEER_ID], vec![PEER_ID]]),
    );

    assert_eq!(state.config.params.shard_number.get(), 3);
    assert_eq!(state.config.params.replication_factor.get(), 2);
}

#[test]
fn create_collection_reject_existing() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&create_collection_op(
        create_collection_request(),
        Some(vec![vec![PEER_ID]]),
    ));

    assert!(matches!(outcome, Err(StorageError::AlreadyExists { .. })));

    assert_eq!(machine.state(), &state);
}

#[test]
fn create_collection_reject_alias_name() {
    let mut state = ClusterState::default();
    state.aliases.insert(COLLECTION.into(), "beta".into());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&create_collection_op(
        create_collection_request(),
        Some(vec![vec![PEER_ID]]),
    ));

    assert!(matches!(outcome, Err(StorageError::BadInput { .. })));

    assert_eq!(machine.state(), &state);
}

#[test]
fn create_collection_reject_max_collections() {
    let mut context = node_context();
    context.max_collections = Some(1);

    let mut state = ClusterState::default();
    state
        .collections
        .insert("beta".into(), collection_state(Vec::new()));

    let mut machine = ConsensusStateMachine::new(state.clone(), context);
    let outcome = machine.apply(&create_collection_op(
        create_collection_request(),
        Some(vec![vec![PEER_ID]]),
    ));

    assert!(matches!(outcome, Err(StorageError::BadRequest { .. })));

    assert_eq!(machine.state(), &state);
}

#[test]
fn create_collection_reject_zero_shards() {
    let state = ClusterState::default();

    // Auto sharding takes the shard number from the distribution, and zero is not a shard number
    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&create_collection_op(
        create_collection_request(),
        Some(Vec::new()),
    ));

    assert!(matches!(outcome, Err(StorageError::BadInput { .. })));

    assert_eq!(machine.state(), &state);
}

#[test]
fn create_collection_vector_names() {
    // `Single` config has an implicit unnamed vector
    let unnamed = json!({
        "vectors": { "size": 4, "distance": "Cosine" },
    });

    let named = json!({
        "vectors": { "image": { "size": 4, "distance": "Cosine" } },
        "sparse_vectors": { "text": {} },
    });

    for request in [unnamed, named] {
        let mut machine = state_machine(ClusterState::default());
        let outcome = machine.apply(&create_collection_op(
            create_collection_json(request.clone()),
            None,
        ));

        assert!(
            outcome.is_ok(),
            "creating a collection with valid vector names should be accepted, \
             request: {request}, got {outcome:?}",
        );

        assert!(machine.state().has_collection(COLLECTION));
    }
}

#[test]
fn create_collection_reject_invalid_dense_vector_name() {
    for vector_name in [".", ".."] {
        create_collection_reject_invalid_vector_name(json!({
            "vectors": { vector_name: { "size": 4, "distance": "Cosine" } },
        }));
    }
}

#[test]
fn create_collection_reject_invalid_sparse_vector_name() {
    for vector_name in [".", ".."] {
        create_collection_reject_invalid_vector_name(json!({
            "sparse_vectors": { vector_name: {} },
        }));
    }
}

#[test]
fn create_collection_reject_empty_sparse_vector_name() {
    create_collection_reject_invalid_vector_name(json!({
        "vectors": { "size": 4, "distance": "Cosine" },
        "sparse_vectors": { "": {} },
    }));
}

#[test]
fn create_collection_reject_duplicate_dense_sparse_vector_name() {
    create_collection_reject_invalid_vector_name(json!({
        "vectors": { "text": { "size": 4, "distance": "Cosine" } },
        "sparse_vectors": { "text": {} },
    }));
}

fn create_collection_reject_invalid_vector_name(request: Value) {
    let state = ClusterState::default();

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&unchecked_create_collection_op(create_collection_json(
        request.clone(),
    )));

    assert!(
        matches!(outcome, Err(StorageError::BadInput { .. })),
        "invalid vector name should be rejected as bad input, request: {request}, got {outcome:?}",
    );

    assert_eq!(machine.state(), &state);
}

/// `placement` names the peers of every shard, the way a proposer sets them.
/// Without it the operation reaches the machine the way a single node proposes it.
fn create_collection_op(
    create_collection: CreateCollection,
    placement: Option<Vec<Vec<PeerId>>>,
) -> ConsensusOperations {
    let mut operation = CreateCollectionOperation::new(COLLECTION.into(), create_collection)
        .expect("valid operation");

    if let Some(placement) = placement {
        operation.set_distribution(ShardDistributionProposal {
            distribution: placement
                .into_iter()
                .enumerate()
                .map(|(idx, peers)| (idx as ShardId, peers))
                .collect(),
        });
    }

    collection_meta_op(CollectionMetaOperations::CreateCollection(operation))
}

/// Skips the validation in `CreateCollectionOperation::new`:
/// a committed entry reaches the machine without going through it
fn unchecked_create_collection_op(create_collection: CreateCollection) -> ConsensusOperations {
    let mut operation =
        CreateCollectionOperation::new(COLLECTION.into(), create_collection_request())
            .expect("valid operation");

    operation.create_collection = create_collection;

    collection_meta_op(CollectionMetaOperations::CreateCollection(operation))
}

fn create_collection_json(request: Value) -> CreateCollection {
    serde_json::from_value(request).expect("valid create collection request")
}
