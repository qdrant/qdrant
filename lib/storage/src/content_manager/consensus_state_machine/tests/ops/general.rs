//! No-op, internal signals and resolving the target collection

use segment::types::*;

use super::*;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::consensus_ops::{ConsensusOperations, SnapshotStatus};
use crate::content_manager::consensus_state_machine::*;
use crate::content_manager::errors::StorageError;

#[test]
fn nop() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&collection_meta_op(CollectionMetaOperations::Nop {
        token: 42,
    }));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("a nop should be accepted, got {outcome:?}");
    };

    assert!(actions.is_empty());
    assert_eq!(machine.state(), &state);
}

#[test]
fn internal_consensus_signals_are_rejected_without_changing_state() {
    let state = cluster_state(Vec::new());
    let machine = state_machine(state.clone());

    for operation in [
        ConsensusOperations::RequestSnapshot,
        ConsensusOperations::ReportSnapshot {
            peer_id: PEER_ID,
            status: SnapshotStatus::Finish,
        },
    ] {
        let error = machine.plan(&operation).unwrap_err();

        assert!(matches!(error, StorageError::ServiceError { .. }));
        assert_eq!(machine.state(), &state);
    }
}

#[test]
fn reject_missing_collection() {
    let mut machine = state_machine(ClusterState::default());
    let outcome = machine.apply(&create_named_vector_op("text", dense(4, Distance::Cosine)));

    assert!(matches!(outcome, Err(StorageError::NotFound { .. })));
}

#[test]
fn resolve_alias() {
    let mut state = cluster_state(Vec::new());
    state.aliases.insert("alias".into(), COLLECTION.into());

    let mut machine = state_machine(state);
    let outcome = machine.apply(&collection_meta_op(
        CollectionMetaOperations::CreateNamedVector(CreateNamedVector {
            collection_name: "alias".into(),
            vector_name: "text".into(),
            config: dense(4, Distance::Cosine),
        }),
    ));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("an alias should resolve to its collection, got {outcome:?}");
    };

    assert_eq!(
        actions
            .first()
            .and_then(Action::collection)
            .expect("action modifies collection"),
        &COLLECTION,
    );

    let vectors = &machine
        .state()
        .collection(COLLECTION)
        .expect("collection exists")
        .config
        .params
        .vectors;

    assert!(vectors.get_params("text").is_some());
}

#[test]
fn reject_dangling_alias() {
    let mut state = cluster_state(Vec::new());
    state.aliases.insert("dangling".into(), "missing".into());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&collection_meta_op(
        CollectionMetaOperations::CreateNamedVector(CreateNamedVector {
            collection_name: "dangling".into(),
            vector_name: "text".into(),
            config: dense(4, Distance::Cosine),
        }),
    ));

    assert!(matches!(outcome, Err(StorageError::NotFound { .. })));

    assert_eq!(machine.state(), &state);
}
