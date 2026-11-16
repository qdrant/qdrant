//! `CreateNamedVector` and `DeleteNamedVector` operations

use segment::data_types::vector_name_config::*;
use segment::types::*;

use super::*;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::*;
use crate::content_manager::errors::StorageError;

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

    let Ok(Plan { actions, .. }) = outcome else {
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

    let Ok(Plan { actions, .. }) = outcome else {
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

    assert!(matches!(outcome, Err(StorageError::BadInput { .. })));

    assert_eq!(machine.state(), &state);
}

#[test]
fn create_named_vector_reject_invalid_name_dense() {
    create_named_vector_reject_invalid_name(dense(4, Distance::Cosine));
}

#[test]
fn create_named_vector_reject_invalid_name_sparse() {
    create_named_vector_reject_invalid_name(sparse());
}

fn create_named_vector_reject_invalid_name(config: VectorNameConfig) {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&create_named_vector_op("..", config));

    assert!(
        matches!(outcome, Err(StorageError::BadInput { .. })),
        "invalid vector name should be rejected as bad input, got {outcome:?}",
    );

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

    let Ok(Plan { actions, .. }) = outcome else {
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

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("deleting a vector that does not exist should be accepted, got {outcome:?}");
    };

    // Action is emitted even if state already matches
    assert!(matches!(
        actions.as_slice(),
        [Action::DropNamedVector { .. }],
    ));

    assert_eq!(machine.state(), &state);
}

fn delete_named_vector_op(vector_name: &str) -> ConsensusOperations {
    collection_meta_op(CollectionMetaOperations::DeleteNamedVector(
        DeleteNamedVector {
            collection_name: COLLECTION.into(),
            vector_name: VectorNameBuf::from(vector_name),
        },
    ))
}
