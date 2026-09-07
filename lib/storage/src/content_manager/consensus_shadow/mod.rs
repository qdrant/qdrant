//! Shadow run of the consensus state machine against the legacy apply path.
//!
//! `TableOfContent` stays authoritative. The machine applies every entry to its own copy of the
//! state, and a compare after the entry reports where the two disagree.

pub mod diff;

#[cfg(test)]
mod tests;

use crate::content_manager::CollectionContainer;
use crate::content_manager::consensus::persistent::Persistent;
use crate::content_manager::consensus_manager::CollectionsSnapshot;
use crate::content_manager::consensus_state_machine::ClusterState;

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
