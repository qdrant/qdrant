//! Run consensus state machine alongside operation handlers and report divergences

pub mod diff;

#[cfg(test)]
mod tests;

use collection::shards::CollectionId;
use parking_lot::Mutex;
use serde::Deserialize;

use self::diff::ShallowState;
use crate::content_manager::CollectionContainer;
use crate::content_manager::collection_meta_ops::CollectionMetaOperations;
use crate::content_manager::consensus::persistent::Persistent;
use crate::content_manager::consensus_manager::CollectionsSnapshot;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::{
    ApplyOutcome, ClusterState, ConsensusStateMachine,
};
use crate::content_manager::errors::{StorageError, StorageResult};

/// Consensus state machine validation mode
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ShadowMode {
    #[default]
    Disabled,
    Log,
    Panic,
}

impl ShadowMode {
    pub fn build(self) -> Option<Mutex<ShadowStateMachine>> {
        let panic_on_divergence = match self {
            ShadowMode::Disabled => return None,
            ShadowMode::Log => false,
            ShadowMode::Panic => true,
        };

        Some(Mutex::new(ShadowStateMachine::new(panic_on_divergence)))
    }
}

/// Consensus state machine used to check operation handlers
pub struct ShadowStateMachine {
    /// Initialized from applied state on first use and after invalidation
    state_machine: Option<ConsensusStateMachine>,
    panic_on_divergence: bool,
}

impl ShadowStateMachine {
    fn new(panic_on_divergence: bool) -> Self {
        Self {
            state_machine: None,
            panic_on_divergence,
        }
    }

    pub fn invalidate(&mut self) {
        self.state_machine = None;
    }

    pub fn apply(
        &mut self,
        toc: &impl CollectionContainer,
        persistent: &Persistent,
        operation: &ConsensusOperations,
    ) -> ApplyOutcome {
        // Partial snapshot recovery can change payload index schema without a consensus operation.
        // Refresh affected collections so state machine applies next operation to recovered state.
        self.resync(toc, toc.take_dirty_collections());

        let state_machine = self.state_machine.get_or_insert_with(|| {
            let state = read_cluster_state(toc, persistent);
            ConsensusStateMachine::new(state, toc.node_context())
        });

        state_machine.apply(operation)
    }

    pub fn compare(
        &mut self,
        toc: &impl CollectionContainer,
        persistent: &Persistent,
        operation: &ConsensusOperations,
        outcome: &ApplyOutcome,
        result: &StorageResult<bool>,
    ) {
        let Some(report) = self.diff(toc, persistent, operation, outcome, result) else {
            return;
        };

        if self.panic_on_divergence {
            panic!("Consensus state machine diverged from applied state: {report}");
        }

        log::error!("Consensus state machine diverged from applied state: {report}");
    }

    fn diff(
        &mut self,
        toc: &impl CollectionContainer,
        persistent: &Persistent,
        operation: &ConsensusOperations,
        outcome: &ApplyOutcome,
        result: &StorageResult<bool>,
    ) -> Option<String> {
        let Some(state_machine) = &self.state_machine else {
            return None;
        };

        // Handler may have persisted only part of operation before returning a service error,
        // while consensus state machine applied it completely. Invalidate instead of comparing.
        if matches!(result, Err(StorageError::ServiceError { .. })) {
            self.invalidate();
            return None;
        }

        let collections = target_collections(operation, state_machine.state());

        // State machine cannot apply an uncovered operation.
        // Resync collections the operation may have changed.
        //
        // If operation does not name a collection, it may have changed any of them,
        // so invalidate the entire consensus state machine.
        if matches!(outcome, ApplyOutcome::NotCovered) {
            if collections.is_empty() {
                self.invalidate();
            } else {
                self.resync(toc, collections);
            }

            return None;
        }

        // Partial snapshot recovery can change payload index schema
        // after state machine applies an operation but before comparison.
        //
        // Refresh affected collections so recovered state is not reported as a divergence.
        self.resync(toc, toc.take_dirty_collections());

        let Some(state_machine) = &self.state_machine else {
            return None;
        };

        let applied = read_shallow_state(toc, persistent);

        let mut report = Vec::from_iter(diff::outcome(outcome, result));
        report.extend(diff::cluster(state_machine.state(), &applied));

        // Collection state is expensive to read, so limit it to possible changes
        for collection in collections {
            let machine_collection = state_machine.state().collection(&collection);
            let applied_collection = toc.collection_state(&collection);

            if let (Some(machine), Some(applied)) = (machine_collection, applied_collection) {
                report.extend(diff::collection(&collection, machine, &applied));
            }
        }

        if report.is_empty() {
            return None;
        }

        // Consensus state machine has diverged from operation handlers.
        // Invalidate it so it reinitializes from applied state on next operation.
        self.invalidate();

        let report = report.join(", ");
        Some(report)
    }

    fn resync(
        &mut self,
        toc: &impl CollectionContainer,
        collections: impl IntoIterator<Item = CollectionId>,
    ) {
        let Some(state_machine) = &mut self.state_machine else {
            return;
        };

        for collection in collections {
            let state = toc.collection_state(&collection);
            state_machine.resync_collection(&collection, state);
        }
    }
}

/// Read complete applied state to initialize consensus state machine
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

/// Read applied state without loading full collection state
pub fn read_shallow_state(toc: &impl CollectionContainer, persistent: &Persistent) -> ShallowState {
    ShallowState {
        collections: toc.collection_names(),
        aliases: toc.alias_mapping(),
        peer_address_by_id: persistent.peer_address_by_id.read().clone(),
        peer_metadata_by_id: persistent.peer_metadata_by_id.read().clone(),
        cluster_metadata: persistent.cluster_metadata.clone(),
        quota_config: toc.quota_config(),
    }
}

/// Collections an operation might change, including any alias target
fn target_collections(operation: &ConsensusOperations, state: &ClusterState) -> Vec<CollectionId> {
    let ConsensusOperations::CollectionMeta(operation) = operation else {
        return Vec::new();
    };

    let collection = match operation.as_ref() {
        CollectionMetaOperations::CreateCollection(op) => &op.collection_name,
        CollectionMetaOperations::UpdateCollection(op) => &op.collection_name,
        CollectionMetaOperations::DeleteCollection(op) => &op.0,
        CollectionMetaOperations::CreateShardKey(op) => &op.collection_name,
        CollectionMetaOperations::DropShardKey(op) => &op.collection_name,
        CollectionMetaOperations::SetShardReplicaState(op) => &op.collection_name,
        CollectionMetaOperations::Resharding(collection, _) => collection,
        CollectionMetaOperations::TransferShard(collection, _) => collection,
        CollectionMetaOperations::CreateNamedVector(op) => &op.collection_name,
        CollectionMetaOperations::DeleteNamedVector(op) => &op.collection_name,
        CollectionMetaOperations::CreatePayloadIndex(op) => &op.collection_name,
        CollectionMetaOperations::DropPayloadIndex(op) => &op.collection_name,

        CollectionMetaOperations::Nop { .. } | CollectionMetaOperations::ChangeAliases(_) => {
            return Vec::new();
        }

        #[cfg(feature = "staging")]
        CollectionMetaOperations::TestSlowDown(_)
        | CollectionMetaOperations::TestTransientError(_) => return Vec::new(),
    };

    match state.aliases.get(collection) {
        Some(resolved) if collection != resolved => {
            vec![collection.clone(), resolved.clone()]
        }

        _ => vec![collection.clone()],
    }
}
