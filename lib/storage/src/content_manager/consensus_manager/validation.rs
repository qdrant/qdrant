//! Compare the cached consensus state with applied storage state

mod diff;

use std::collections::BTreeSet;

use collection::shards::CollectionId;

use self::diff::ShallowState;
use crate::content_manager::CollectionContainer;
use crate::content_manager::collection_meta_ops::CollectionMetaOperations;
use crate::content_manager::consensus::persistent::Persistent;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::ClusterState;

/// Read global fields once and compare each collection named by the operations once
pub(super) fn compare(
    expected: &ClusterState,
    toc: &impl CollectionContainer,
    persistent: &Persistent,
    operations: &[ConsensusOperations],
) -> Option<String> {
    let applied = read_shallow_state(toc, persistent);
    let mut report = diff::cluster(expected, &applied);
    let collections: BTreeSet<_> = operations
        .iter()
        .flat_map(|operation| target_collections(operation, expected))
        .collect();

    for collection in collections {
        let expected = expected.collection(&collection);
        let applied = toc.collection_state(&collection);

        if let (Some(expected), Some(applied)) = (expected, applied) {
            report.extend(diff::collection(&collection, expected, &applied));
        }
    }

    if report.is_empty() {
        log::debug!("Consensus state machine matches applied state");
        None
    } else {
        Some(report.join(", "))
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
    let operation = match operation {
        ConsensusOperations::RemovePeer(_) => {
            let mut collections: Vec<_> = state.collections.keys().cloned().collect();
            collections.sort();
            return collections;
        }
        ConsensusOperations::CollectionMeta(operation) => operation,
        ConsensusOperations::AddPeer { .. }
        | ConsensusOperations::UpdatePeerMetadata { .. }
        | ConsensusOperations::UpdateClusterMetadata { .. }
        | ConsensusOperations::SetQuotaConfig(_)
        | ConsensusOperations::RequestSnapshot
        | ConsensusOperations::ReportSnapshot { .. } => return Vec::new(),
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
