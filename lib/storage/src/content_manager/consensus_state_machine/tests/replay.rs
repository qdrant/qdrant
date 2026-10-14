//! Correctness and replay-safety properties that must hold for every consensus operation

use std::collections::HashSet;

use collection::shards::replica_set::Change;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use proptest::prelude::*;

use super::prop::*;
use super::*;
use crate::content_manager::collection_meta_ops::{
    AliasOperations, ChangeAliasesOperation, RenameAlias,
};
use crate::content_manager::errors::StorageError;

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
    /// Replay may be rejected only once the applied prefix has reached the goal state.
    ///
    /// Usually that is the full action list. An operation may put side-effect-only actions after
    /// its last state change, so an earlier prefix can already equal the goal. Rejecting before
    /// that would make partial state permanent.
    /// Replica removal's existing last-replica rejection after transfer abort is checked separately
    /// because both implementations can invalidate that check before completing the operation.
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

            let reached_goal = crashed == goal;
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
                    let known_guard_failure = replica_removal_guard_may_reject(
                        &state,
                        &operation,
                        &actions[..crash_after],
                        &err,
                    );
                    prop_assert!(
                        reached_goal || known_guard_failure,
                        "replay after {} of {} actions was rejected before reaching the goal: {}",
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

/// Transfer abort can invalidate the replica-removal check before the caller removes its replica.
/// Both implementations check the initial replica set, so replay can reject after an abort
/// removes another replica or marks it dead. Keep checking every accepted replay and every other
/// rejection until validation is fixed in both implementations.
pub(super) fn replica_removal_guard_may_reject(
    state: &ClusterState,
    operation: &ConsensusOperations,
    applied_actions: &[Action],
    error: &StorageError,
) -> bool {
    let ConsensusOperations::CollectionMeta(operation) = operation else {
        return false;
    };
    let CollectionMetaOperations::UpdateCollection(operation) = operation.as_ref() else {
        return false;
    };
    let Some(changes) = &operation.shard_replica_changes else {
        return false;
    };
    let StorageError::BadRequest { description } = error else {
        return false;
    };
    let Ok(collection) = state.resolve_collection(&operation.collection_name) else {
        return false;
    };
    let collection_state = state.collection(&collection).expect("collection exists");

    changes.iter().any(|&Change::Remove(shard_id, peer_id)| {
        if *description
            != format!(
                "Shard {shard_id} must have at least one active replica after removing {peer_id}"
            )
        {
            return false;
        }

        collection_state.transfers.iter().any(|transfer| {
            if transfer.is_resharding()
                || transfer.to == peer_id
                || transfer.to_shard_id.unwrap_or(transfer.shard_id) != shard_id
            {
                return false;
            }

            applied_actions.iter().any(|action| {
                matches!(action, Action::RemoveReplica {
                    collection: changed_collection,
                    shard_id: changed_shard,
                    peer_id: changed_peer,
                } if !transfer.sync
                        && *changed_collection == collection
                        && *changed_shard == shard_id
                        && *changed_peer == transfer.to)
                    || matches!(action, Action::SetReplicaState {
                    collection: changed_collection,
                    shard_id: changed_shard,
                    peer_id: changed_peer,
                    state: ReplicaState::Dead,
                } if transfer.sync
                        && *changed_collection == collection
                        && *changed_shard == shard_id
                        && *changed_peer == transfer.to)
            })
        })
    })
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
