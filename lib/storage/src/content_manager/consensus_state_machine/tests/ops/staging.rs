//! Staging-only test operations

use super::*;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::consensus_state_machine::*;

#[test]
fn test_slow_down() {
    let operation = TestSlowDown {
        peer_id: Some(PEER_ID),
        duration_ms: 10,
    };

    let actions = staging_operation_changes_nothing(CollectionMetaOperations::TestSlowDown(
        operation.clone(),
    ));

    assert_eq!(actions, vec![Action::TestSlowDown(operation)]);
}

#[test]
fn test_transient_error() {
    let operation = TestTransientError {
        peer_id: Some(PEER_ID),
        failure_probability_percent: 100,
    };

    let actions = staging_operation_changes_nothing(CollectionMetaOperations::TestTransientError(
        operation.clone(),
    ));

    assert_eq!(actions, vec![Action::TestTransientError(operation)]);
}

#[test]
fn staging_hook_actions_change_nothing() {
    let state = transfer_state();

    for action in [
        Action::TestPauseShardTransferStart,
        Action::TestPauseReshardingAbort,
        Action::TestCrashAfterScaleDownStateClear,
    ] {
        let mut applied = state.clone();
        applied.apply_action(&action);

        assert_eq!(applied, state);
        assert_eq!(action.collection(), None);
    }
}

/// The action a staging operation emits only has an effect outside `ClusterState`
fn staging_operation_changes_nothing(operation: CollectionMetaOperations) -> Vec<Action> {
    let state = cluster_state(Vec::new());

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&collection_meta_op(operation));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("a staging operation should be accepted, got {outcome:?}");
    };

    assert_eq!(machine.state(), &state);

    actions
}
