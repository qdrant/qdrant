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

/// The empty name belongs to the unnamed dense vector: a sparse vector under it would share
/// that vector's storage directory. Collection creation rejects it, and so must this path,
/// once every peer runs a version that rejects it.
#[test]
fn create_named_vector_reject_empty_name_sparse() {
    // No peers, and all peers upgraded (including the first dev build of the release)
    let cases: [&[&str]; 3] = [&[], &["1.20.3-dev", "1.20.3"], &["1.20.3", "1.21.0"]];

    for versions in cases {
        let mut state = cluster_state(Vec::new());
        add_peers(&mut state, versions);

        let mut machine = state_machine(state.clone());
        let outcome = machine.apply(&create_named_vector_op("", sparse()));

        assert!(
            matches!(
                &outcome,
                Err(StorageError::BadInput { description })
                    if description.contains("Sparse vector name cannot be empty"),
            ),
            "empty sparse vector name should be rejected with peers {versions:?}, got {outcome:?}",
        );

        assert_eq!(machine.state(), &state);
    }
}

/// Peers before the fix apply a sparse vector named `""`. While one of them may be in the
/// cluster, upgraded peers must apply it as well, or they would diverge from the old peers.
#[test]
fn create_named_vector_accept_empty_name_sparse_from_old_peers() {
    let cases: [&[Option<&str>]; 3] = [
        &[Some("1.20.3"), Some("1.20.2")],
        &[Some("1.20.3"), Some("1.20.2-dev")],
        // A peer without metadata has an unknown version
        &[Some("1.20.3"), None],
    ];

    for versions in cases {
        let mut state = cluster_state(Vec::new());
        for (peer_id, &version) in (PEER_ID..).zip(versions) {
            add_peer(&mut state, peer_id, version);
        }

        let mut machine = state_machine(state);
        let outcome = machine.apply(&create_named_vector_op("", sparse()));

        let Ok(Plan { actions, .. }) = outcome else {
            panic!(
                "empty sparse vector name should be accepted with peers {versions:?}, got {outcome:?}"
            );
        };

        assert!(
            matches!(
                actions.as_slice(),
                [Action::AddNamedVector { vector_name, .. }] if vector_name.is_empty(),
            ),
            "unexpected actions with peers {versions:?}: {actions:?}",
        );

        let params = &machine
            .state()
            .collection(COLLECTION)
            .expect("collection exists")
            .config
            .params;

        assert!(
            params
                .sparse_vectors
                .as_ref()
                .is_some_and(|sparse| sparse.contains_key("")),
            "empty sparse vector should be registered with peers {versions:?}",
        );
    }
}

/// The format check does not depend on peer versions: old peers ran it too
#[test]
fn create_named_vector_reject_invalid_name_from_old_peers() {
    let mut state = cluster_state(Vec::new());
    add_peers(&mut state, &["1.20.3", "1.20.2"]);

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&create_named_vector_op("..", sparse()));

    assert!(
        matches!(outcome, Err(StorageError::BadInput { .. })),
        "invalid vector name should be rejected as bad input, got {outcome:?}",
    );

    assert_eq!(machine.state(), &state);
}

fn add_peers(state: &mut ClusterState, versions: &[&str]) {
    for (peer_id, &version) in (PEER_ID..).zip(versions) {
        add_peer(state, peer_id, Some(version));
    }
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
