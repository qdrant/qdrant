//! Correctness and replay-safety properties that must hold for every consensus operation

use std::collections::HashSet;

use proptest::prelude::*;

use super::prop::*;
use super::*;
use crate::content_manager::collection_meta_ops::{
    AliasOperations, ChangeAliasesOperation, RenameAlias,
};

proptest! {
    /// Accepted operation changes the state only through its own actions.
    ///
    /// Applying the actions one by one must reach exactly the same state as `apply`.
    /// If it does not, something else modified the state.
    #[test]
    fn state_change_matches_actions((state, operation) in arb_state_and_operation()) {
        let mut machine = state_machine(state.clone());

        let ApplyOutcome::Accepted(actions) = machine.apply(&operation) else {
            return Ok(());
        };

        let mut state = state;

        for action in &actions {
            state.apply_action(action);
        }

        prop_assert_eq!(machine.state(), &state);
    }

    /// Rejected operation does not modify the state
    #[test]
    fn rejection_changes_nothing((state, operation) in arb_state_and_operation()) {
        let mut machine = state_machine(state.clone());

        if let ApplyOutcome::Rejected(_) = machine.apply(&operation) {
            prop_assert_eq!(machine.state(), &state);
        }
    }

    /// The same operation applied to the same state produces the same actions
    #[test]
    fn planning_is_deterministic((state, operation) in arb_state_and_operation()) {
        let first = apply(&state, &operation);
        let second = apply(&state, &operation);

        match (first, second) {
            (ApplyOutcome::Accepted(first), ApplyOutcome::Accepted(second)) => {
                prop_assert_eq!(first, second);
            }

            (ApplyOutcome::Rejected(_), ApplyOutcome::Rejected(_)) => {}
            (ApplyOutcome::NotCovered, ApplyOutcome::NotCovered) => {}

            (first, second) => prop_assert!(
                false,
                "same state and operation decided differently: {first:?} then {second:?}",
            ),
        }
    }

    /// Replay reaches the same state as a run that never crashed, no matter how many actions
    /// were applied before the crash.
    ///
    /// Replay may be rejected only after every action was applied, where the state is already
    /// complete. Rejecting any earlier would make the partial state permanent.
    #[test]
    fn replay_after_crash_converges((state, operation) in arb_state_and_operation()) {
        if replay_may_diverge(&operation) {
            return Ok(());
        }

        let mut uncrashed = state_machine(state.clone());

        let ApplyOutcome::Accepted(actions) = uncrashed.apply(&operation) else {
            return Ok(());
        };

        let goal = uncrashed.state().clone();

        for crash_after in 0..=actions.len() {
            let mut crashed = state.clone();

            for action in &actions[..crash_after] {
                crashed.apply_action(action);
            }

            let mut replay = state_machine(crashed);

            match replay.apply(&operation) {
                ApplyOutcome::Accepted(_) => {
                    prop_assert_eq!(
                        replay.state(),
                        &goal,
                        "replay after {} of {} actions reached a different state",
                        crash_after,
                        actions.len(),
                    );
                }

                ApplyOutcome::Rejected(err) => {
                    prop_assert_eq!(
                        crash_after,
                        actions.len(),
                        "replay after {} of {} actions was rejected: {}",
                        crash_after,
                        actions.len(),
                        err,
                    );
                }

                ApplyOutcome::NotCovered => prop_assert!(
                    false,
                    "operation planned actions but a replay reported it as not covered",
                ),
            }
        }
    }
}

/// Apply `operation` without modifying `state`
fn apply(state: &ClusterState, operation: &ConsensusOperations) -> ApplyOutcome {
    state_machine(state.clone()).apply(operation)
}

/// Operations that are not fully idempotent, and may diverge on replay.
fn replay_may_diverge(operation: &ConsensusOperations) -> bool {
    let ConsensusOperations::CollectionMeta(operation) = operation else {
        return false;
    };

    rename_alias_may_diverge(operation) || collection_metadata_may_diverge(operation)
}

/// `RenameAlias` is not idempotent: it moves whatever the alias points at,
/// so a second run moves whatever the first run left under that name.
///
/// E.g., take a list of two actions: the first renames alias `prod` to `prod_old`,
/// the second creates alias `prod` for collection `new`.
///
/// Starting from `{ prod: old }`, the first run leaves `{ prod_old: old, prod: new }`.
/// A replay renames the `prod` the first run created, and leaves `{ prod_old: new, prod: new }`.
///
/// A replay diverges only if renamed alias is recreated:
/// by a later action of the operation, or by an earlier action during the replay itself.
/// A create recreates the alias it names, a rename recreates the alias it renames to.
/// Otherwise replay rejects the whole operation, and aliases stay unchanged.
///
/// If the operation renames multiple aliases, then *all* of them have to be recreated.
/// If any one is not, then the whole operation is rejected.
///
/// And one of the renames has to move a value from the current state.
/// A create always writes the same value, a rename moves whatever the alias holds.
/// So if an action creates an alias and a later action renames it, both runs move that value:
/// `[create prod, prod → prod_old, prod_old → archive]` always converges.
/// But `[prod → prod_old, create prod]` renames what the state holds, and may diverge.
///
/// This check is an approximate heuristic, and marks some operations that never diverge
/// as "may diverge", such as `[prod_old → prod, prod → prod_old]`,
/// which puts every alias back where it started.
fn rename_alias_may_diverge(operation: &CollectionMetaOperations) -> bool {
    let CollectionMetaOperations::ChangeAliases(operation) = operation else {
        return false;
    };

    let ChangeAliasesOperation { actions } = operation;

    let mut renames_pre_existing = false;
    let mut renamed = HashSet::new();
    let mut created = HashSet::new();

    for action in actions {
        match action {
            AliasOperations::RenameAlias(action) => {
                let RenameAlias {
                    old_alias_name,
                    new_alias_name,
                } = &action.rename_alias;

                renames_pre_existing |= !created.contains(old_alias_name);

                renamed.insert(old_alias_name);
                created.insert(new_alias_name);
            }

            AliasOperations::CreateAlias(action) => {
                created.insert(&action.create_alias.alias_name);
            }

            AliasOperations::DeleteAlias(_) => (),
        }
    }

    renames_pre_existing && renamed.is_subset(&created)
}

/// Metadata is merged into the config, where a null value removes the key it names. A collection
/// with no metadata yet takes the whole payload instead, nulls included, and a replay merges that
/// payload into itself and drops those keys.
fn collection_metadata_may_diverge(operation: &CollectionMetaOperations) -> bool {
    let CollectionMetaOperations::UpdateCollection(operation) = operation else {
        return false;
    };

    operation
        .update_collection
        .metadata
        .as_ref()
        .is_some_and(|metadata| metadata.0.values().any(serde_json::Value::is_null))
}
