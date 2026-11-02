//! `ChangeAliases` operation

use std::collections::{BTreeMap, BTreeSet};

use super::*;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::*;
use crate::content_manager::errors::StorageError;

#[test]
fn create_alias() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state);
    let outcome = machine.apply(&change_aliases_op(vec![create_alias_action(
        "alias", COLLECTION,
    )]));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("creating an alias should be accepted, got {outcome:?}");
    };

    assert_eq!(actions, vec![set_aliases(vec![("alias", COLLECTION)])]);

    let aliases = &machine.state().aliases;

    assert_eq!(aliases.get("alias").map(String::as_str), Some(COLLECTION));
}

#[test]
fn create_alias_replay() {
    let mut state = cluster_state(Vec::new());
    state.aliases.insert("alias".into(), COLLECTION.into());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&change_aliases_op(vec![create_alias_action(
        "alias", COLLECTION,
    )]));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("replay of an applied alias should be accepted, got {outcome:?}");
    };

    // The mapping already holds what the operation writes, so there is nothing to save
    assert!(actions.is_empty());

    assert_eq!(machine.state(), &state, "replay should not change anything");
}

#[test]
fn create_alias_reject_missing_collection() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&change_aliases_op(vec![create_alias_action(
        "alias", "missing",
    )]));

    assert!(matches!(outcome, Err(StorageError::NotFound { .. })));

    assert_eq!(machine.state(), &state);
}

#[test]
fn create_alias_reject_alias_target() {
    let mut state = cluster_state(Vec::new());
    state.aliases.insert("alias".into(), COLLECTION.into());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&change_aliases_op(vec![create_alias_action(
        "other", "alias",
    )]));

    // An alias of an alias is rejected: the target is not resolved
    assert!(matches!(outcome, Err(StorageError::NotFound { .. })));

    assert_eq!(machine.state(), &state);
}

#[test]
fn create_alias_reject_collection_name() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&change_aliases_op(vec![create_alias_action(
        COLLECTION, COLLECTION,
    )]));

    assert!(matches!(outcome, Err(StorageError::AlreadyExists { .. })));

    assert_eq!(machine.state(), &state);
}

#[test]
fn delete_alias() {
    let mut state = cluster_state(Vec::new());
    state.aliases.insert("alias".into(), COLLECTION.into());

    let mut machine = state_machine(state);
    let outcome = machine.apply(&change_aliases_op(vec![delete_alias_action("alias")]));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("deleting an alias should be accepted, got {outcome:?}");
    };

    assert_eq!(actions, vec![remove_aliases(vec!["alias"])]);

    assert!(machine.state().aliases.get("alias").is_none());
}

#[test]
fn delete_alias_missing() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&change_aliases_op(vec![delete_alias_action("alias")]));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("deleting an alias that does not exist should be accepted, got {outcome:?}");
    };

    // Nothing to remove, so nothing to save
    assert!(actions.is_empty());

    assert_eq!(machine.state(), &state);
}

#[test]
fn rename_alias() {
    let mut state = cluster_state(Vec::new());
    state.aliases.insert("alias".into(), COLLECTION.into());

    let mut machine = state_machine(state);
    let outcome = machine.apply(&change_aliases_op(vec![rename_alias_action(
        "alias", "other",
    )]));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("renaming an alias should be accepted, got {outcome:?}");
    };

    // A rename resolves to the value it moves, and the alias it takes it from
    assert_eq!(
        actions,
        vec![Action::UpdateAliases {
            set: BTreeMap::from([("other".to_string(), COLLECTION.to_string())]),
            remove: BTreeSet::from(["alias".to_string()]),
        }]
    );

    let aliases = &machine.state().aliases;

    assert!(aliases.get("alias").is_none());
    assert_eq!(aliases.get("other").map(String::as_str), Some(COLLECTION));
}

#[test]
fn rename_alias_reject_missing() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&change_aliases_op(vec![rename_alias_action(
        "alias", "other",
    )]));

    assert!(matches!(outcome, Err(StorageError::NotFound { .. })));

    assert_eq!(machine.state(), &state);
}

#[test]
fn change_aliases_reject_missing_rename() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&change_aliases_op(vec![
        create_alias_action("new", COLLECTION),
        rename_alias_action("missing", "other"),
    ]));

    assert!(matches!(outcome, Err(StorageError::NotFound { .. })));

    // The action before the rename is validated, never emitted
    assert_eq!(machine.state(), &state);
}

#[test]
fn change_aliases_in_order() {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state);
    let outcome = machine.apply(&change_aliases_op(vec![
        create_alias_action("alias", COLLECTION),
        rename_alias_action("alias", "other"),
    ]));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("renaming an alias the operation just created should be accepted, got {outcome:?}");
    };

    // The alias the operation creates and renames never reaches the mapping
    assert_eq!(actions, vec![set_aliases(vec![("other", COLLECTION)])]);

    let aliases = &machine.state().aliases;

    assert!(aliases.get("alias").is_none());
    assert_eq!(aliases.get("other").map(String::as_str), Some(COLLECTION));
}

#[test]
fn change_aliases_reject_whole_operation() {
    let mut state = cluster_state(Vec::new());
    state.aliases.insert("alias".into(), COLLECTION.into());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&change_aliases_op(vec![
        delete_alias_action("alias"),
        create_alias_action("other", "missing"),
    ]));

    assert!(matches!(outcome, Err(StorageError::NotFound { .. })));

    // The delete before the failing action is validated, never emitted
    assert_eq!(machine.state(), &state);
}

fn change_aliases_op(actions: Vec<AliasOperations>) -> ConsensusOperations {
    collection_meta_op(CollectionMetaOperations::ChangeAliases(
        ChangeAliasesOperation { actions },
    ))
}

fn set_aliases(aliases: Vec<(&str, &str)>) -> Action {
    Action::UpdateAliases {
        set: aliases
            .into_iter()
            .map(|(alias, collection)| (alias.to_string(), collection.to_string()))
            .collect(),
        remove: BTreeSet::new(),
    }
}

fn remove_aliases(aliases: Vec<&str>) -> Action {
    Action::UpdateAliases {
        set: BTreeMap::new(),
        remove: aliases.into_iter().map(String::from).collect(),
    }
}

fn create_alias_action(alias: &str, collection: &str) -> AliasOperations {
    CreateAlias {
        collection_name: collection.into(),
        alias_name: alias.into(),
    }
    .into()
}

fn delete_alias_action(alias: &str) -> AliasOperations {
    DeleteAlias {
        alias_name: alias.into(),
    }
    .into()
}

fn rename_alias_action(old_alias: &str, new_alias: &str) -> AliasOperations {
    RenameAlias {
        old_alias_name: old_alias.into(),
        new_alias_name: new_alias.into(),
    }
    .into()
}
