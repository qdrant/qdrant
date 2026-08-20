//! Correctness and replay-safety properties that must hold for every consensus operation

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
/// `RenameAlias` reads the alias it removes, so replaying an operation whose rename landed either
/// rejects it, leaving the actions after the rename unapplied, or renames the alias that a later
/// action put back. `TableOfContent::update_aliases` behaves the same way, and the machine
/// reproduces it. A lone rename converges: its rejection comes after the last action.
fn replay_may_diverge(operation: &ConsensusOperations) -> bool {
    let ConsensusOperations::CollectionMeta(operation) = operation else {
        return false;
    };

    let CollectionMetaOperations::ChangeAliases(ChangeAliasesOperation { actions }) = &**operation
    else {
        return false;
    };

    let renames = actions
        .iter()
        .any(|action| matches!(action, AliasOperations::RenameAlias(_)));

    renames && actions.len() > 1
}
