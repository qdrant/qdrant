//! Explicit tests asserting behavior of individual consensus operations
//! and tests for cases that `proptest` is unlikely to generate or reach

use std::collections::HashMap;

use segment::data_types::vector_name_config::*;
use segment::types::*;

use super::*;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::*;
use crate::content_manager::errors::StorageError;

const COLLECTION: &str = "alpha";

#[test]
fn nop() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&collection_meta_op(CollectionMetaOperations::Nop {
        token: 42,
    }));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("a nop should be accepted, got {outcome:?}");
    };

    assert!(actions.is_empty());
    assert_eq!(machine.state(), &state);
}

#[test]
fn create_named_vector_dense() {
    let machine = create_named_vector_impl(dense(4, Distance::Cosine));

    let params = &machine
        .state()
        .collection(COLLECTION)
        .expect("collection exists")
        .config
        .params;

    assert!(params.vectors.get_params("text").is_some());
}

#[test]
fn create_named_vector_sparse() {
    let machine = create_named_vector_impl(sparse());

    let params = &machine
        .state()
        .collection(COLLECTION)
        .expect("collection exists")
        .config
        .params;

    let sparse = params
        .sparse_vectors
        .as_ref()
        .expect("sparse vector config exists");

    assert!(sparse.contains_key("text"));
}

fn create_named_vector_impl(config: VectorNameConfig) -> ConsensusStateMachine {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state);
    let outcome = machine.apply(&create_named_vector_op("text", config));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("creating a new named vector should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::AddNamedVector { .. }],
    ));

    machine
}

#[test]
fn create_named_vector_replay() {
    let config = dense(4, Distance::Cosine);
    let state = cluster_state(vec![("text", config.clone())]);

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&create_named_vector_op("text", config));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("replay of an applied named vector should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::AddNamedVector { .. }],
    ));

    assert_eq!(machine.state(), &state, "replay should not change anything");
}

#[test]
fn create_named_vector_reject_existing_diff_dim() {
    create_named_vector_reject_existing(dense(8, Distance::Cosine));
}

#[test]
fn create_named_vector_reject_existing_diff_type() {
    create_named_vector_reject_existing(sparse());
}

fn create_named_vector_reject_existing(config: VectorNameConfig) {
    let state = cluster_state(vec![("text", dense(4, Distance::Cosine))]);

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&create_named_vector_op("text", config));

    assert!(matches!(
        outcome,
        ApplyOutcome::Rejected(StorageError::BadInput { .. })
    ));

    assert_eq!(machine.state(), &state);
}

#[test]
fn delete_named_vector_dense() {
    delete_named_vector_impl(dense(4, Distance::Cosine));
}

#[test]
fn delete_named_vector_sparse() {
    delete_named_vector_impl(sparse());
}

fn delete_named_vector_impl(config: VectorNameConfig) {
    let state = cluster_state(vec![("text", config)]);

    let mut machine = state_machine(state);
    let outcome = machine.apply(&delete_named_vector_op("text"));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("deleting an existing vector should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::DropNamedVector { .. }],
    ));

    let params = &machine
        .state()
        .collection(COLLECTION)
        .expect("collection exists")
        .config
        .params;

    assert!(params.vectors.get_params("text").is_none());

    assert!(
        params
            .sparse_vectors
            .as_ref()
            .is_none_or(|sparse| !sparse.contains_key("text")),
    );
}

#[test]
fn delete_named_vector_missing() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&delete_named_vector_op("text"));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("deleting a vector that does not exist should be accepted, got {outcome:?}");
    };

    // Action is emitted even if state already matches
    assert!(matches!(
        actions.as_slice(),
        [Action::DropNamedVector { .. }],
    ));

    assert_eq!(machine.state(), &state);
}

#[test]
fn create_payload_index() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state);
    let outcome = machine.apply(&create_payload_index_op("city", PayloadSchemaType::Keyword));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("creating payload index should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::SetPayloadIndex { .. }],
    ));

    let schema = &machine
        .state()
        .collection(COLLECTION)
        .expect("collection exists")
        .payload_index_schema
        .schema;

    assert!(schema.contains_key(&field_name("city")));
}

#[test]
fn create_payload_index_replay() {
    let state = cluster_state_with_index("city", PayloadSchemaType::Keyword);

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&create_payload_index_op("city", PayloadSchemaType::Keyword));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("replay of an applied payload index should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::SetPayloadIndex { .. }],
    ));

    assert_eq!(machine.state(), &state, "replay should not change anything");
}

#[test]
fn create_payload_index_replace_schema() {
    let state = cluster_state_with_index("city", PayloadSchemaType::Keyword);

    let mut machine = state_machine(state);
    let outcome = machine.apply(&create_payload_index_op("city", PayloadSchemaType::Integer));

    let ApplyOutcome::Accepted(actions) = outcome else {
        panic!("indexing a field again with another schema should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::SetPayloadIndex { .. }],
    ));

    let schema = &machine
        .state()
        .collection(COLLECTION)
        .expect("collection exists")
        .payload_index_schema
        .schema;

    // A field indexed with a different schema is replaced, where a named vector is rejected
    assert_eq!(
        schema.get(&field_name("city")),
        Some(&PayloadFieldSchema::FieldType(PayloadSchemaType::Integer)),
    );
}

fn cluster_state(vectors: Vec<(&str, VectorNameConfig)>) -> ClusterState {
    let vectors = vectors
        .into_iter()
        .map(|(name, config)| (name.into(), config))
        .collect();

    let collection_state = collection_state(vectors);

    ClusterState {
        collections: HashMap::from([(COLLECTION.into(), collection_state)]),
        ..Default::default()
    }
}

fn cluster_state_with_index(field: &str, field_type: PayloadSchemaType) -> ClusterState {
    let mut state = cluster_state(Vec::new());

    state
        .collections
        .get_mut(COLLECTION)
        .expect("collection exists")
        .payload_index_schema
        .schema
        .insert(field_name(field), PayloadFieldSchema::FieldType(field_type));

    state
}

fn collection_meta_op(op: CollectionMetaOperations) -> ConsensusOperations {
    ConsensusOperations::CollectionMeta(Box::new(op))
}

fn create_named_vector_op(vector_name: &str, config: VectorNameConfig) -> ConsensusOperations {
    collection_meta_op(CollectionMetaOperations::CreateNamedVector(
        CreateNamedVector {
            collection_name: COLLECTION.into(),
            vector_name: VectorNameBuf::from(vector_name),
            config,
        },
    ))
}

fn dense(size: usize, distance: Distance) -> VectorNameConfig {
    VectorNameConfig::dense(DenseVectorConfig {
        size,
        distance,
        multivector_config: None,
        datatype: None,
    })
}

fn sparse() -> VectorNameConfig {
    VectorNameConfig::sparse(SparseVectorConfig {
        modifier: None,
        datatype: None,
    })
}

fn delete_named_vector_op(vector_name: &str) -> ConsensusOperations {
    collection_meta_op(CollectionMetaOperations::DeleteNamedVector(
        DeleteNamedVector {
            collection_name: COLLECTION.into(),
            vector_name: VectorNameBuf::from(vector_name),
        },
    ))
}

fn create_payload_index_op(field: &str, field_type: PayloadSchemaType) -> ConsensusOperations {
    collection_meta_op(CollectionMetaOperations::CreatePayloadIndex(
        CreatePayloadIndex {
            collection_name: COLLECTION.to_string(),
            field_name: field_name(field),
            field_schema: PayloadFieldSchema::FieldType(field_type),
        },
    ))
}

fn field_name(field: &str) -> PayloadKeyType {
    field.parse().expect("valid field name")
}
