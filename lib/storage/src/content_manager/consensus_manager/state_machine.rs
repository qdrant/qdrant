#[cfg(test)]
mod tests;

use std::fmt;

use collection::shards::CollectionId;
use parking_lot::MutexGuard;
use raft::eraftpb::{ConfChangeTransition, ConfChangeType, ConfChangeV2, Entry as RaftEntry};
use raft::{RawNode, Storage};
use tonic::transport::Uri;

use super::{validation, *};
use crate::content_manager::consensus_state_machine::{
    Action, ClusterState, ConsensusStateMachine, Plan,
};

impl<C: CollectionContainer> ConsensusManager<C> {
    pub(super) fn apply_with_state_machine(
        &self,
        index: u64,
        operation: &ConsensusOperations,
    ) -> StorageResult<bool> {
        let started = Instant::now();

        let plan = match self.plan_operation(operation) {
            Ok(plan) => plan,
            Err(error @ StorageError::ServiceError { .. }) => return Err(error),
            Err(error) => {
                log::warn!(
                    "Rejected consensus entry index={index} operation={operation:?}: {error}"
                );
                return Err(error);
            }
        };

        log::debug!(
            "Applying consensus entry index={index} operation={operation:?} result={} actions={}",
            plan.result,
            ActionList(&plan.actions),
        );

        for action in &plan.actions {
            if let Err(error) = self.apply_action(action.clone()) {
                // An action may have persisted only part of its work. Retry from applied state.
                self.invalidate_state_machine();
                return Err(error);
            }
        }

        let check =
            self.finish_state_machine(std::slice::from_ref(operation), std::slice::from_ref(&plan));
        log::debug!(
            "Applied consensus entry index={index} in {:?}, state check: {check}",
            started.elapsed(),
        );

        Ok(plan.result)
    }

    fn plan_operation(&self, operation: &ConsensusOperations) -> StorageResult<Plan> {
        self.prepare_state_machine().machine.plan(operation)
    }

    /// Validate the entire batch against temporary state before any membership or storage writes
    fn plan_operations(&self, operations: &[ConsensusOperations]) -> StorageResult<Vec<Plan>> {
        let mut machine = self.prepare_state_machine().machine.clone();
        let mut plans = Vec::with_capacity(operations.len());

        for operation in operations {
            let plan = machine.plan(operation)?;
            machine.apply_actions(&plan.actions);
            plans.push(plan);
        }

        Ok(plans)
    }

    fn prepare_state_machine(&self) -> MutexGuard<'_, CachedStateMachine> {
        // Discovery holds the persistent lock through invalidation. Read it before the cache
        // so planning cannot miss an address update, and never execute storage actions here.
        let persistent = self.persistent.read();
        let mut cached = self.state_machine.lock();
        let dirty = self.toc.take_dirty_collections();

        if cached.needs_reload {
            *cached = CachedStateMachine::new(self.toc.as_ref(), &persistent);
        } else {
            resync_collections(&mut cached.machine, self.toc.as_ref(), dirty);
        }

        cached
    }

    /// Advance after successful execution and refresh external changes independently of validation
    fn finish_state_machine(
        &self,
        operations: &[ConsensusOperations],
        plans: &[Plan],
    ) -> StateCheck {
        let report = {
            let persistent = self.persistent.read();
            let mut cached = self.state_machine.lock();
            if cached.needs_reload {
                return StateCheck::Invalidated;
            }

            for plan in plans {
                cached.machine.apply_actions(&plan.actions);
            }

            // Partial snapshot recovery may have changed collection state during execution.
            resync_collections(
                &mut cached.machine,
                self.toc.as_ref(),
                self.toc.take_dirty_collections(),
            );

            if !Self::should_check_state_machine() {
                return StateCheck::NotSampled;
            }

            let report = validation::compare(
                cached.machine.state(),
                self.toc.as_ref(),
                &persistent,
                operations,
            );
            if report.is_some() {
                cached.needs_reload = true;
            }
            report
        };

        let Some(report) = report else {
            return StateCheck::Matched;
        };

        if cfg!(any(test, debug_assertions)) {
            panic!(
                "Consensus state machine diverged from applied state: {report}; \
                 operations={operations:?}",
            );
        }

        log::error!(
            "Consensus state machine diverged from applied state: {report}; \
             operations={operations:?}",
        );

        StateCheck::Diverged
    }

    pub(super) fn invalidate_state_machine(&self) {
        let mut cached = self.state_machine.lock();
        if !cached.needs_reload {
            log::debug!("Consensus state machine invalidated");
        }
        cached.needs_reload = true;
    }

    #[cfg(test)]
    pub(super) fn state_machine_needs_reload(&self) -> bool {
        self.state_machine.lock().needs_reload
    }

    fn should_check_state_machine() -> bool {
        // Check every entry in debug builds so tests catch divergences deterministically.
        // Collection reads are expensive, so release builds check a 1% sample.
        cfg!(any(test, debug_assertions)) || rand::random_ratio(1, 100)
    }

    pub(super) fn notify_operation(
        &self,
        operation: &ConsensusOperations,
        result: StorageResult<bool>,
    ) {
        let on_apply = self.on_consensus_op_apply.lock().remove(operation);
        if let Some(on_apply) = on_apply
            && on_apply.send(result).is_err()
        {
            log::debug!("Failed to notify subscribers on consensus operation completion");
        }
    }

    pub(super) fn apply_conf_change_with_state_machine<T: Storage>(
        &self,
        entry: &RaftEntry,
        change: &ConfChangeV2,
        raw_node: &mut RawNode<T>,
    ) -> StorageResult<bool> {
        let result = self.apply_conf_change_actions(entry, change, raw_node);
        if result.is_err() {
            self.invalidate_state_machine();
        }

        result
    }

    fn apply_conf_change_actions<T: Storage>(
        &self,
        entry: &RaftEntry,
        change: &ConfChangeV2,
        raw_node: &mut RawNode<T>,
    ) -> StorageResult<bool> {
        let started = Instant::now();

        let mut operations = Vec::with_capacity(change.changes.len());
        for single_change in &change.changes {
            let operation = match single_change.change_type() {
                ConfChangeType::AddNode | ConfChangeType::AddLearnerNode => {
                    if entry.context.is_empty() {
                        // Promotions carry no address. Older learner additions may also omit it.
                        continue;
                    }

                    let uri: Uri = std::str::from_utf8(&entry.context)
                        .map_err(|error| {
                            StorageError::service_error(format!(
                                "Failed to parse peer URI: {error}"
                            ))
                        })?
                        .parse()
                        .map_err(|error| {
                            StorageError::service_error(format!(
                                "Failed to parse peer URI: {error}"
                            ))
                        })?;
                    ConsensusOperations::AddPeer {
                        peer_id: single_change.node_id,
                        uri: uri.to_string(),
                    }
                }
                ConfChangeType::RemoveNode => {
                    ConsensusOperations::RemovePeer(single_change.node_id)
                }
            };
            operations.push(operation);
        }

        let batch = self.plan_operations(&operations);
        let batch = match batch {
            Ok(batch) => batch,
            Err(error @ StorageError::ServiceError { .. }) => return Err(error),
            Err(error) => {
                // Raft permits rejecting a committed configuration change by not applying it.
                // No address, collection or membership writes have run at this point.
                for operation in &operations {
                    self.notify_operation(operation, Err(error.clone()));
                }
                log::warn!(
                    "Rejected consensus configuration entry index={} changes={}: {error}",
                    entry.index,
                    ConfChanges(change),
                );
                return Ok(false);
            }
        };

        log::debug!(
            "Applying consensus configuration entry index={} changes={} actions={}",
            entry.index,
            ConfChanges(change),
            ActionList(batch.iter().flat_map(|plan| &plan.actions)),
        );

        let conf_state = apply_raft_conf_change(raw_node, change)?;
        self.persistent
            .write()
            .apply_state_update(|state| state.conf_state = conf_state)?;

        for plan in &batch {
            for action in &plan.actions {
                self.apply_action(action.clone())?;
            }
        }

        let stop_consensus = operations.iter().any(|operation| {
            matches!(operation, ConsensusOperations::RemovePeer(peer_id) if *peer_id == self.this_peer_id())
        });
        if stop_consensus {
            self.invalidate_state_machine();
        } else {
            let check = self.finish_state_machine(&operations, &batch);
            log::debug!(
                "Applied consensus configuration entry index={} in {:?}, state check: {check}",
                entry.index,
                started.elapsed(),
            );
        }

        for operation in &operations {
            self.notify_operation(operation, Ok(true));
        }

        Ok(stop_consensus)
    }
}

/// Cached applied state. Reload only when the next operation can safely read collections.
pub(super) struct CachedStateMachine {
    machine: ConsensusStateMachine,
    // Discovery runs in async code and cannot synchronously read collections through ToC.
    needs_reload: bool,
}

impl CachedStateMachine {
    pub(super) fn new(toc: &impl CollectionContainer, persistent: &Persistent) -> Self {
        let state = read_cluster_state(toc, persistent);
        let context = toc.node_context();
        log::info!(
            "Consensus state machine initialized: peer_id={}, collections={}, peers={}",
            context.peer_id,
            state.collections.len(),
            state.peer_address_by_id.len(),
        );
        let machine = ConsensusStateMachine::new(state, context);

        Self {
            machine,
            needs_reload: false,
        }
    }
}

/// Read complete applied state to initialize the cached consensus state machine
pub fn read_cluster_state(toc: &impl CollectionContainer, persistent: &Persistent) -> ClusterState {
    let CollectionsSnapshot {
        collections,
        aliases,
    } = toc.collections_snapshot();

    ClusterState {
        collections,
        aliases,
        peer_address_by_id: persistent.peer_address_by_id.read().clone(),
        peer_metadata_by_id: persistent.peer_metadata_by_id.read().clone(),
        cluster_metadata: persistent.cluster_metadata.clone(),
        quota_config: toc.quota_config(),
    }
}

fn resync_collections(
    machine: &mut ConsensusStateMachine,
    toc: &impl CollectionContainer,
    collections: impl IntoIterator<Item = CollectionId>,
) {
    for collection in collections {
        let state = toc.collection_state(&collection);
        log::debug!(
            "Consensus state machine resynchronizing collection {collection:?}: present={}",
            state.is_some(),
        );
        machine.resync_collection(&collection, state);
    }
}

/// Retry configuration bookkeeping after membership changed but an action or save failed
fn apply_raft_conf_change<T: Storage>(
    raw_node: &mut RawNode<T>,
    change: &ConfChangeV2,
) -> StorageResult<raft::eraftpb::ConfState> {
    let current = raw_node.raft.prs().conf().to_conf_state();
    let joint = !current.voters_outgoing.is_empty();

    // Raft rejects entering an already joint configuration and leaving a non-joint one.
    // Membership is saved before actions, so either transition can already be complete
    // when consensus restarts to retry the same unapplied entry.
    if change.leave_joint() && !joint {
        return Ok(current);
    }

    if let Some(auto_leave) = change.enter_joint()
        && joint
        && current.auto_leave == auto_leave
    {
        let mut checked = HashSet::new();
        let applied = change.changes.iter().rev().all(|single| {
            // A batch may change the same peer more than once; its final change wins
            if !checked.insert(single.node_id) {
                return true;
            }

            let voter = current.voters.contains(&single.node_id);
            let learner = current.learners.contains(&single.node_id)
                || current.learners_next.contains(&single.node_id);
            match single.change_type() {
                ConfChangeType::AddNode => voter && !learner,
                ConfChangeType::AddLearnerNode => learner && !voter,
                ConfChangeType::RemoveNode => !voter && !learner,
            }
        });
        if applied {
            return Ok(current);
        }
    }

    Ok(raw_node.apply_conf_change(change)?)
}

/// Result of comparing the cached state with applied state after an entry, for entry logs
#[derive(Copy, Clone, Debug)]
enum StateCheck {
    Matched,
    Diverged,
    /// Release builds compare a sample of entries
    NotSampled,
    /// Something invalidated the cache during execution, so it reloads before the next entry
    Invalidated,
}

impl fmt::Display for StateCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let check = match self {
            StateCheck::Matched => "matched",
            StateCheck::Diverged => "diverged",
            StateCheck::NotSampled => "not sampled",
            StateCheck::Invalidated => "skipped, cache invalidated",
        };

        f.write_str(check)
    }
}

/// Actions in their one-line summaries, as `[first, second]`
struct ActionList<I>(I);

impl<'a, I> fmt::Display for ActionList<I>
where
    I: IntoIterator<Item = &'a Action> + Clone,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        for (index, action) in self.0.clone().into_iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{action}")?;
        }
        f.write_str("]")
    }
}

/// Raft peer changes, as `[AddLearnerNode(peer=5)]`, followed by a non-default transition
struct ConfChanges<'a>(&'a ConfChangeV2);

impl fmt::Display for ConfChanges<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let change = self.0;

        if change.leave_joint() {
            return f.write_str("[LeaveJoint]");
        }

        f.write_str("[")?;
        for (index, single) in change.changes.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{:?}(peer={})", single.change_type(), single.node_id)?;
        }
        f.write_str("]")?;

        let transition = change.transition();
        if transition != ConfChangeTransition::Auto {
            write!(f, " transition={transition:?}")?;
        }

        Ok(())
    }
}
