//! `DeleteCollection` operation

use std::collections::{BTreeMap, BTreeSet};

use super::*;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::*;

#[test]
fn delete_collection() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state);
    let outcome = machine.apply(&delete_collection_op(COLLECTION));

    let Ok(Plan { actions, result }) = outcome else {
        panic!("deleting a collection should be accepted, got {outcome:?}");
    };

    assert!(result);

    assert!(matches!(
        actions.as_slice(),
        [Action::DropCollection { .. }]
    ));

    assert!(!machine.state().has_collection(COLLECTION));
}

#[test]
fn delete_collection_aliases() {
    let mut state = cluster_state(Vec::new());
    state
        .collections
        .insert("beta".into(), collection_state(Vec::new()));
    state.aliases.insert("second".into(), COLLECTION.into());
    state.aliases.insert("first".into(), COLLECTION.into());
    state.aliases.insert("other".into(), "beta".into());

    let mut machine = state_machine(state);
    let outcome = machine.apply(&delete_collection_op(COLLECTION));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("deleting a collection should be accepted, got {outcome:?}");
    };

    // Aliases of the collection go first, in name order, and the collection last
    assert_eq!(
        actions,
        vec![
            Action::UpdateAliases {
                set: BTreeMap::new(),
                remove: BTreeSet::from(["first".into(), "second".into()]),
            },
            Action::DropCollection {
                collection: COLLECTION.into(),
            },
        ],
    );

    let aliases = &machine.state().aliases;

    assert_eq!(aliases.get("other").map(String::as_str), Some("beta"));
}

#[test]
fn delete_collection_replay() {
    let state = ClusterState::default();

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&delete_collection_op(COLLECTION));

    let Ok(Plan { actions, result }) = outcome else {
        panic!("replay of an applied delete should be accepted, got {outcome:?}");
    };

    // Action is emitted even if state already matches
    assert!(!result);

    assert!(matches!(
        actions.as_slice(),
        [Action::DropCollection { .. }]
    ));

    assert_eq!(machine.state(), &state, "replay should not change anything");
}

#[test]
fn delete_collection_alias_name() {
    let mut state = cluster_state(Vec::new());
    state.aliases.insert("alias".into(), COLLECTION.into());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&delete_collection_op("alias"));

    let Ok(Plan { actions, result }) = outcome else {
        panic!("deleting a collection by alias should be accepted, got {outcome:?}");
    };

    // The name is not resolved: the alias names no collection to delete, and it points at
    // `alpha`, not at itself, so it stays
    assert!(!result);

    assert!(matches!(
        actions.as_slice(),
        [Action::DropCollection { .. }]
    ));

    assert_eq!(machine.state(), &state);
}

fn delete_collection_op(collection: &str) -> ConsensusOperations {
    collection_meta_op(CollectionMetaOperations::DeleteCollection(
        DeleteCollectionOperation(collection.into()),
    ))
}
