//! Shadow run of the consensus state machine against the legacy apply path.
//!
//! `TableOfContent` stays authoritative. The machine applies every entry to its own copy of the
//! state, and a compare after the entry reports where the two disagree.

pub mod diff;

#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod tests;

use self::diff::ActualState;
use crate::content_manager::CollectionContainer;
use crate::content_manager::consensus::persistent::Persistent;
use crate::content_manager::consensus_manager::CollectionsSnapshot;
use crate::content_manager::consensus_state_machine::ClusterState;

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
