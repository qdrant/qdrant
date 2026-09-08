//! Shadow run of the consensus state machine against the legacy apply path.
//!
//! `TableOfContent` stays authoritative. The machine applies every entry to its own copy of the
//! state, and a compare after the entry reports where the two disagree.

pub mod diff;

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
    /// Fail the peer on a divergence, rather than logging it and carrying on
    panic_on_divergence: bool,
}

impl ShadowStateMachine {
    fn new(panic_on_divergence: bool) -> Self {
        Self {
            machine: None,
            panic_on_divergence,
        }
    }

    /// Drop the state, so the next entry builds a machine out of `TableOfContent` again
    pub fn invalidate(&mut self) {
        self.machine = None;
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

    /// Compare the shadow against the state the authoritative apply left behind, and report a
    /// divergence the way this node is configured to
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
            panic!("shadow state machine diverged: {report}");
        }

        log::error!("Shadow state machine diverged: {report}");
    }

    /// How the shadow differs from the state the authoritative apply left behind.
    ///
    /// The machine is invalidated whenever the two cannot be compared, and whenever they
    /// disagree, so one bug is reported once.
    fn diff(
        &mut self,
        toc: &impl CollectionContainer,
        persistent: &Persistent,
        operation: &ConsensusOperations,
        outcome: &ApplyOutcome,
        result: &StorageResult<bool>,
    ) -> Option<String> {
        let Some(machine) = &self.machine else {
            return None;
        };

        // A service error kills the consensus thread and the entry is applied again after
        // restart. What the failed apply wrote before it gave up is not something the machine
        // predicts.
        if matches!(result, Err(StorageError::ServiceError { .. })) {
            self.machine = None;
            return None;
        }

        let collections = compared_collections(operation, machine.state());

        // An operation the machine does not model leaves the state of the collections it names
        // behind, so those are read back instead of compared. One that names none, `RemovePeer`
        // above all, can have changed any of them.
        if matches!(outcome, ApplyOutcome::NotCovered) {
            if collections.is_empty() {
                self.machine = None;
            } else {
                self.resync(toc, collections);
            }

            return None;
        }

        let actual = scrape_actual_state(toc, persistent);

        let mut report = Vec::from_iter(diff::outcome(outcome, result));
        report.extend(diff::cluster(machine.state(), &actual));

        // Reading a collection's state is the expensive part, so only the ones this operation
        // could have changed are read. A collection only one side holds is already reported.
        for collection in collections {
            let shadow = machine.state().collection(&collection);
            let actual = toc.collection_state(&collection);

            if let (Some(shadow), Some(actual)) = (shadow, actual) {
                report.extend(diff::collection(&collection, shadow, &actual));
            }
        }

        if report.is_empty() {
            return None;
        }

        self.machine = None;

        Some(report.join(", "))
    }

    /// Read the state of `collections` back into the machine
    fn resync(&mut self, toc: &impl CollectionContainer, collections: Vec<CollectionId>) {
        let Some(machine) = &mut self.machine else {
            return;
        };

        for collection in collections {
            let state = toc.collection_state(&collection);
            machine.resync_collection(&collection, state);
        }
    }
}

/// Whether to run the shadow, and what it does with a divergence it finds
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ShadowMode {
    /// Do not run the machine at all
    #[default]
    Disabled,
    /// Log the difference and carry on, for production and cloud
    Log,
    /// Fail the peer, for chaos and end-to-end tests
    Panic,
}

impl ShadowMode {
    /// The shadow to run, `None` when it is disabled
    pub fn build(self) -> Option<Mutex<ShadowStateMachine>> {
        let panic_on_divergence = match self {
            ShadowMode::Disabled => return None,
            ShadowMode::Log => false,
            ShadowMode::Panic => true,
        };

        Some(Mutex::new(ShadowStateMachine::new(panic_on_divergence)))
    }
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
