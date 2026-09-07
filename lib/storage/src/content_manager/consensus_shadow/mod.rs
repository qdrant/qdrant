//! Shadow run of the consensus state machine against the legacy apply path.
//!
//! `TableOfContent` stays authoritative. The machine applies every entry to its own copy of the
//! state, and a compare after the entry reports where the two disagree.

pub mod diff;

#[cfg(test)]
mod tests;

use parking_lot::Mutex;
use serde::Deserialize;

use crate::content_manager::CollectionContainer;
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
        outcome: &ApplyOutcome,
        result: &StorageResult<bool>,
    ) {
        let Some(report) = self.diff(toc, persistent, outcome, result) else {
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
        outcome: &ApplyOutcome,
        result: &StorageResult<bool>,
    ) -> Option<String> {
        let Some(machine) = &self.machine else {
            return None;
        };

        // A service error kills the consensus thread and the entry is applied again after
        // restart. What the failed apply wrote before it gave up is not something the machine
        // predicts, and an operation it does not model leaves its state behind entirely.
        let comparable = !matches!(result, Err(StorageError::ServiceError { .. }))
            && !matches!(outcome, ApplyOutcome::NotCovered);

        if !comparable {
            self.machine = None;
            return None;
        }

        let actual = scrape_cluster_state(toc, persistent);
        let answer = diff::outcome(outcome, result);
        let state = diff::cluster(machine.state(), &actual);

        let report: Vec<_> = [answer, state].into_iter().flatten().collect();

        if report.is_empty() {
            return None;
        }

        self.machine = None;

        Some(report.join("; "))
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

/// Read the state consensus decides on back out of the node that applied it.
///
/// Reads the state of every collection, which is what one compare costs.
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
