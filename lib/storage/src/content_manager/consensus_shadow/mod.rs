//! Shadow run of the consensus state machine against the legacy apply path.
//!
//! `TableOfContent` stays authoritative. The machine applies every entry to its own copy of the
//! state, and a compare after the entry reports where the two disagree.

pub mod diff;

#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod tests;

use collection::shards::CollectionId;
use parking_lot::Mutex;
use serde::Deserialize;

use self::diff::ActualState;
use crate::content_manager::CollectionContainer;
use crate::content_manager::collection_meta_ops::CollectionMetaOperations;
use crate::content_manager::consensus::persistent::Persistent;
use crate::content_manager::consensus_manager::CollectionsSnapshot;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::{
    ApplyOutcome, ClusterState, ConsensusStateMachine,
};
use crate::content_manager::errors::{StorageError, StorageResult};

/// State machine applying every entry alongside the legacy handlers
pub struct ShadowStateMachine {
    /// Built from `TableOfContent` on first use, and again after every invalidation
    machine: Option<ConsensusStateMachine>,
    on_divergence: OnDivergence,
}

impl ShadowStateMachine {
    pub fn new(on_divergence: OnDivergence) -> Self {
        Self {
            machine: None,
            on_divergence,
        }
    }

    /// Apply `operation` to the shadow state, building the machine when there is none
    pub fn apply(
        &mut self,
        toc: &impl CollectionContainer,
        persistent: &Persistent,
        operation: &ConsensusOperations,
    ) -> ApplyOutcome {
        let machine = self.machine.get_or_insert_with(|| {
            let state = scrape_cluster_state(toc, persistent);
            ConsensusStateMachine::new(state, toc.node_context())
        });

        machine.apply(operation)
    }

    /// Compare the shadow against the state the authoritative apply left behind.
    ///
    /// The machine is invalidated whenever the two cannot be compared, and whenever they
    /// disagree, so one bug is reported once.
    pub fn compare(
        &mut self,
        toc: &impl CollectionContainer,
        persistent: &Persistent,
        operation: &ConsensusOperations,
        outcome: &ApplyOutcome,
        result: &StorageResult<bool>,
    ) {
        let Some(machine) = &self.machine else {
            return;
        };

        // A service error kills the consensus thread and the entry is applied again after
        // restart. What the failed apply wrote before it gave up is not something the machine
        // predicts.
        if matches!(result, Err(StorageError::ServiceError { .. })) {
            self.machine = None;
            return;
        }

        let collections = compared_collections(operation, machine.state());

        // An operation the machine does not model leaves the state of the collections it names
        // behind, so those are read back instead of compared
        if matches!(outcome, ApplyOutcome::NotCovered) {
            self.resync(toc, collections);
            return;
        }

        let actual = scrape_actual_state(toc, persistent);

        let mut report = Vec::from_iter(diff::outcome(outcome, result));
        report.extend(diff::cluster(machine.state(), &actual));

        for collection in collections {
            let shadow = machine.state().collection(&collection);
            let actual = toc.collection_state(&collection);

            if let (Some(shadow), Some(actual)) = (shadow, actual) {
                report.extend(diff::collection(&collection, shadow, &actual));
            }
        }

        self.report_divergence(report);
    }

    /// Read the state of `collections` back into the machine.
    ///
    /// An operation naming none of them, `RemovePeer` above all, can have changed any
    /// collection, so the machine goes instead.
    fn resync(&mut self, toc: &impl CollectionContainer, collections: Vec<CollectionId>) {
        if collections.is_empty() {
            self.machine = None;
            return;
        }

        let Some(machine) = &mut self.machine else {
            return;
        };

        for collection in collections {
            let state = toc.collection_state(&collection);
            machine.resync_collection(&collection, state);
        }
    }

    /// Report a non-empty `report` and invalidate the machine
    fn report_divergence(&mut self, report: Vec<String>) {
        if report.is_empty() {
            return;
        }

        let report = report.join(", ");

        match self.on_divergence {
            OnDivergence::Log => log::error!("Shadow state machine diverged: {report}"),
            OnDivergence::Panic => panic!("shadow state machine diverged: {report}"),
        }

        self.machine = None;
    }
}

/// Whether to run the shadow, and what to do when it finds a divergence
#[derive(Clone, Copy, Debug, Default, Deserialize)]
pub struct ShadowConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub on_divergence: OnDivergence,
}

impl ShadowConfig {
    /// The shadow to run, `None` when it is disabled
    pub fn build(&self) -> Option<Mutex<ShadowStateMachine>> {
        let &ShadowConfig {
            enabled,
            on_divergence,
        } = self;

        enabled.then(|| Mutex::new(ShadowStateMachine::new(on_divergence)))
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum OnDivergence {
    /// Log the difference and carry on, for production and cloud
    #[default]
    Log,
    /// Fail the peer, for chaos and end-to-end tests
    Panic,
}

/// Read the whole cluster state back, to build a machine that starts from it.
///
/// Reads the state of every collection, so this runs when a machine is built, not per entry.
pub fn scrape_cluster_state(
    toc: &impl CollectionContainer,
    persistent: &Persistent,
) -> ClusterState {
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

/// Read back everything an entry's compare reads, leaving out the contents of collections
pub fn scrape_actual_state(toc: &impl CollectionContainer, persistent: &Persistent) -> ActualState {
    ActualState {
        collections: toc.collection_names(),
        aliases: toc.alias_mapping(),
        peer_address_by_id: persistent.peer_address_by_id.read().clone(),
        peer_metadata_by_id: persistent.peer_metadata_by_id.read().clone(),
        cluster_metadata: persistent.cluster_metadata.clone(),
        quota_config: toc.quota_config(),
    }
}

/// Collections whose state the compare after `operation` reads.
///
/// Both the name the operation carries and the collection it resolves to, since planning
/// resolves aliases and the legacy handlers do so per operation.
fn compared_collections(
    operation: &ConsensusOperations,
    state: &ClusterState,
) -> Vec<CollectionId> {
    let ConsensusOperations::CollectionMeta(operation) = operation else {
        return Vec::new();
    };

    let collection = match &**operation {
        CollectionMetaOperations::CreateCollection(operation) => &operation.collection_name,
        CollectionMetaOperations::UpdateCollection(operation) => &operation.collection_name,
        CollectionMetaOperations::DeleteCollection(operation) => &operation.0,
        CollectionMetaOperations::SetShardReplicaState(operation) => &operation.collection_name,
        CollectionMetaOperations::CreateShardKey(operation) => &operation.collection_name,
        CollectionMetaOperations::DropShardKey(operation) => &operation.collection_name,
        CollectionMetaOperations::CreatePayloadIndex(operation) => &operation.collection_name,
        CollectionMetaOperations::DropPayloadIndex(operation) => &operation.collection_name,
        CollectionMetaOperations::CreateNamedVector(operation) => &operation.collection_name,
        CollectionMetaOperations::DeleteNamedVector(operation) => &operation.collection_name,
        CollectionMetaOperations::Resharding(collection, _) => collection,
        CollectionMetaOperations::TransferShard(collection, _) => collection,

        // Change no collection. Alias changes are covered by the compare of the whole mapping.
        CollectionMetaOperations::ChangeAliases(_) | CollectionMetaOperations::Nop { .. } => {
            return Vec::new();
        }

        #[cfg(feature = "staging")]
        CollectionMetaOperations::TestSlowDown(_)
        | CollectionMetaOperations::TestTransientError(_) => return Vec::new(),
    };

    match state.aliases.get(collection) {
        Some(resolved) if resolved != collection => {
            vec![collection.clone(), resolved.clone()]
        }
        _ => vec![collection.clone()],
    }
}
