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

/// Whether replay of `operation` is allowed to end up anywhere but the goal state.
///
/// A rename is the one alias action that is not an absolute write: it reads the alias it consumes.
/// The whole operation applies in one write, so a replay that cannot find one of those aliases
/// rejects and changes nothing, which leaves the state the first run reached.
///
/// It diverges when every rename finds its alias again, because a later action of the same
/// operation put it back. Renaming `prod` to `prod_old` and then pointing `prod` at another
/// collection is that shape: the replay renames the new `prod`, and both names end up on it.
/// Telling the two runs apart needs something that records the operation as applied.
fn replay_may_diverge(operation: &ConsensusOperations) -> bool {
    let ConsensusOperations::CollectionMeta(operation) = operation else {
        return false;
    };

    let CollectionMetaOperations::ChangeAliases(operation) = operation.deref() else {
        return false;
    };

    let ChangeAliasesOperation { actions } = operation;

    let renames = actions.iter().enumerate().filter_map(|(idx, action)| {
        let AliasOperations::RenameAlias(action) = action else {
            return None;
        };

        Some((idx, &action.rename_alias.old_alias_name))
    });

    let mut renames = renames.peekable();

    // An operation without a rename converges, and `all` of nothing is true
    renames.peek().is_some()
        && renames.all(|(idx, renamed)| {
            actions[idx + 1..]
                .iter()
                .any(|action| creates_alias(action, renamed))
        })
}

/// Whether `action` makes `alias` name a collection
fn creates_alias(action: &AliasOperations, alias: &str) -> bool {
    match action {
        AliasOperations::CreateAlias(action) => action.create_alias.alias_name == alias,
        AliasOperations::RenameAlias(action) => action.rename_alias.new_alias_name == alias,
        AliasOperations::DeleteAlias(_) => false,
    }
}
