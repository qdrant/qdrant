//! Correctness and replay-safety properties that must hold for every consensus operation

use std::ops::Deref as _;

use proptest::prelude::*;

use super::prop::*;
use super::*;
use crate::content_manager::collection_meta_ops::{AliasOperations, ChangeAliasesOperation};

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
///
/// `RenameAlias` is not idempotent: it moves whatever the alias points at,
/// so a second run moves whatever the first run left under that name.
///
/// E.g., take a list of two actions: the first renames alias `prod` to `prod_old`,
/// the second creates alias `prod` for collection `new`.
///
/// Starting from `{ prod: old }`, the first run leaves `{ prod_old: old, prod: new }`.
/// A replay renames the `prod` the first run created, and leaves `{ prod_old: new, prod: new }`.
///
/// This only happens if the alias still exists after the first run:
/// a later action created an alias with the same name, or renamed another alias to it.
/// If it does not exist, the whole operation is rejected and aliases stay unchanged.
///
/// If the operation renames multiple aliases, then *all* of them have to exist.
/// If any one is missing, then the whole operation is rejected.
fn replay_may_diverge(operation: &ConsensusOperations) -> bool {
    let ConsensusOperations::CollectionMeta(operation) = operation else {
        return false;
    };

    let CollectionMetaOperations::ChangeAliases(operation) = operation.deref() else {
        return false;
    };

    let ChangeAliasesOperation { actions } = operation;

    let mut renames = actions
        .iter()
        .enumerate()
        .filter_map(|(idx, action)| {
            let AliasOperations::RenameAlias(action) = action else {
                return None;
            };

            let alias = &action.rename_alias.old_alias_name;
            let rest = &actions[idx + 1..];

            Some((alias, rest))
        })
        .peekable();

    // If operation does not do any renames, it should be idempotent
    let has_renames = renames.peek().is_some();

    // If operation does not recreate *all* renamed aliases, it should be rejected on replay
    // without changing anything which is idempotent
    let recreates_all_renamed_aliases =
        renames.all(|(alias, rest)| rest.iter().any(|action| creates_alias(action, alias)));

    has_renames && recreates_all_renamed_aliases
}

/// Checks if `action` creates `alias`
fn creates_alias(action: &AliasOperations, alias: &str) -> bool {
    match action {
        AliasOperations::CreateAlias(action) => action.create_alias.alias_name == alias,
        AliasOperations::RenameAlias(action) => action.rename_alias.new_alias_name == alias,
        AliasOperations::DeleteAlias(_) => false,
    }
}
