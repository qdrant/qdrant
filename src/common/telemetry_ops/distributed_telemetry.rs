use std::collections::HashMap;

use collection::operations::types::{ReshardingInfo, ShardStatus, ShardTransferInfo};
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::shard::ShardId;
use collection::shards::telemetry::PartialSnapshotTelemetry;
use collection::telemetry::{CollectionTelemetry, ShardCleanStatusTelemetry};
use itertools::Itertools;
use schemars::JsonSchema;
use segment::types::ShardKey;
use serde::Serialize;
use shard::PeerId;
use storage::content_manager::errors::{StorageError, StorageResult};
use storage::rbac::Access;
use storage::types::{ConsensusThreadStatus, StateRole};

use crate::common::telemetry::TelemetryData;
use crate::common::telemetry_ops::cluster_telemetry::ClusterTelemetry;
use crate::common::telemetry_ops::collections_telemetry::CollectionTelemetryEnum;

#[derive(Serialize, JsonSchema)]
pub struct DistributedTelemetryData {
    collections: HashMap<String, DistributedCollectionTelemetry>,

    #[serde(skip_serializing_if = "Option::is_none")]
    cluster: Option<DistributedClusterTelemetry>,
}

#[derive(Serialize, JsonSchema)]
pub struct DistributedCollectionTelemetry {
    /// Collection name
    id: String,

    /// Shards topology
    #[serde(skip_serializing_if = "Option::is_none")]
    shards: Option<Vec<DistributedShardTelemetry>>,

    /// Ongoing resharding operations
    #[serde(skip_serializing_if = "Option::is_none")]
    reshardings: Option<Vec<ReshardingInfo>>,

    /// Ongoing shard transfers
    #[serde(skip_serializing_if = "Option::is_none")]
    shard_transfers: Option<Vec<ShardTransferInfo>>,
}

#[derive(Serialize, JsonSchema)]
pub struct DistributedShardTelemetry {
    /// Shard ID
    id: ShardId,

    /// Optional shard key
    #[serde(skip_serializing_if = "Option::is_none")]
    key: Option<ShardKey>,

    /// Replica information
    replicas: Vec<DistributedReplicaTelemetry>,
}

#[derive(Serialize, JsonSchema)]
pub struct DistributedReplicaTelemetry {
    /// Peer ID hosting this replica
    peer_id: PeerId,

    /// Consensus state for this replica
    state: ReplicaState,

    // All below are None if the relevant local shard is not found
    /// Shard status
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<ShardStatus>,

    /// Total optimized points
    #[serde(skip_serializing_if = "Option::is_none")]
    total_optimized_points: Option<usize>,

    /// Estimated vectors size in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    vectors_size_bytes: Option<usize>,

    /// Estimated payloads size in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    payloads_size_bytes: Option<usize>,

    /// Approximate number of points
    #[serde(skip_serializing_if = "Option::is_none")]
    num_points: Option<usize>,

    /// Approximate number of vectors
    #[serde(skip_serializing_if = "Option::is_none")]
    num_vectors: Option<usize>,

    /// Approximate number of vectors by name
    #[serde(skip_serializing_if = "Option::is_none")]
    num_vectors_by_name: Option<HashMap<String, usize>>,

    /// Shard cleaning task status.
    /// After a resharding, a cleanup task is performed to remove outdated points from this shard.
    #[serde(skip_serializing_if = "Option::is_none")]
    shard_cleaning_status: Option<ShardCleanStatusTelemetry>,

    /// Partial snapshot telemetry
    #[serde(skip_serializing_if = "Option::is_none")]
    partial_snapshot: Option<PartialSnapshotTelemetry>,
}

#[derive(Serialize, JsonSchema)]
pub struct DistributedClusterTelemetry {
    enabled: bool,
    number_of_peers: Option<u64>,
    peers: HashMap<PeerId, DistributedPeerInfo>,
}

#[derive(Serialize, JsonSchema)]
pub struct DistributedPeerInfo {
    /// URI of the peer
    uri: String,

    /// Whether this peer responded for this request
    responsive: bool,

    /// If responsive, these details should be available
    details: Option<DistributedPeerDetails>,
}

#[derive(Serialize, JsonSchema)]
pub struct DistributedPeerDetails {
    /// Qdrant version
    version: String,

    /// Consensus role for the peer
    role: Option<StateRole>,

    /// Whether it can participate in leader elections
    is_voter: bool,

    /// Election term
    term: u64,

    /// Latest accepted commit
    commit: u64,

    /// Number of operations pending for being applied
    num_pending_operations: u64,

    /// Status of consensus thread
    consensus_thread_status: ConsensusThreadStatus,
}

impl DistributedTelemetryData {
    pub fn resolve_telemetries(
        access: &Access,
        telemetries: Vec<TelemetryData>,
        missing_peers: Vec<PeerId>,
    ) -> StorageResult<Self> {
        // Use the telemetry from node with highest term/commit
        let base_telemetry = newest_telemetry(&telemetries)
            .ok_or_else(|| StorageError::service_error("Could not get telemetry from cluster"))?;

        // Create a map of peer_id -> TelemetryData for quick lookup
        let telemetry_by_peer = &telemetries
            .iter()
            .filter_map(|telemetry| {
                telemetry
                    .cluster
                    .as_ref()
                    .and_then(|c| c.status.as_ref())
                    .and_then(|s| s.peer_id)
                    .map(|peer_id| (peer_id, telemetry))
            })
            .collect::<HashMap<_, _>>();

        let cluster = match access {
            Access::Global(_) => {
                aggregate_cluster_telemetry(base_telemetry, telemetry_by_peer, missing_peers)
            }
            Access::Collection(_) => None,
        };

        // Aggregate collections information
        //
        // The collections should already be filtered by the ones in `access`
        let collections =
            aggregate_collections(telemetry_by_peer, base_telemetry).unwrap_or_default();

        Ok(DistributedTelemetryData {
            collections,
            cluster,
        })
    }
}

fn aggregate_collections(
    telemetry_by_peer: &HashMap<PeerId, &TelemetryData>,
    base_telemetry: &TelemetryData,
) -> Option<HashMap<String, DistributedCollectionTelemetry>> {
    let base_collections = base_telemetry.collections.collections.as_ref()?;
    let mut collections = HashMap::with_capacity(base_collections.len());
    let base_peer_id = base_telemetry.cluster.as_ref()?.status.as_ref()?.peer_id?;

    for collection_enum in base_collections {
        let CollectionTelemetryEnum::Full(collection) = collection_enum else {
            log::error!("Expected collection telemetry enum to be Full variant");
            continue;
        };

        let collection_id = collection.id.clone();

        // Aggregate shard transfers from all peers
        let transfers = aggregate_shard_transfers(base_peer_id, telemetry_by_peer, &collection_id);

        // Aggregate resharding info from all peers
        let reshardings =
            aggregate_resharding_info(&base_peer_id, telemetry_by_peer, &collection_id);

        // Aggregate shards from all peers
        let shards = aggregate_shards(telemetry_by_peer, collection, &collection_id);

        collections.insert(
            collection_id.clone(),
            DistributedCollectionTelemetry {
                id: collection_id,
                shards,
                shard_transfers: (!transfers.is_empty()).then_some(transfers),
                reshardings: (!reshardings.is_empty()).then_some(reshardings),
            },
        );
    }
    Some(collections)
}

fn aggregate_shard_transfers(
    base_peer_id: PeerId,
    telemetry_by_peer: &HashMap<u64, &TelemetryData>,
    collection_id: &String,
) -> Vec<ShardTransferInfo> {
    let get_transfers = |peer_id| {
        let telemetry = telemetry_by_peer.get(&peer_id)?;

        telemetry
            .collections
            .collections
            .as_ref()?
            .iter()
            .find_map(|collection| match collection {
                CollectionTelemetryEnum::Full(coll) if coll.id == *collection_id => {
                    coll.transfers.as_ref()
                }
                CollectionTelemetryEnum::Full(_) | CollectionTelemetryEnum::Aggregated(_) => None,
            })
    };

    let find_transfer = |peer_id, base_transfer: &ShardTransferInfo| {
        let ShardTransferInfo {
            shard_id,
            to_shard_id,
            from,
            to,
            sync: _,
            method: _,
            comment: _,
        } = base_transfer;

        get_transfers(peer_id)?.iter().find(|t| {
            t.from == *from
                && t.to == *to
                && t.shard_id == *shard_id
                && t.to_shard_id == *to_shard_id
        })
    };

    let Some(base_transfers) = get_transfers(base_peer_id) else {
        return Vec::new();
    };

    // Prefer source peer's transfer info, merge destination's comment if available
    base_transfers
        .iter()
        .map(|base_transfer| {
            let mut transfer = find_transfer(base_transfer.from, base_transfer)
                .cloned()
                .unwrap_or_else(|| base_transfer.clone());

            // Append destination peer's comment if available
            if let Some(dst_transfer) = find_transfer(base_transfer.to, base_transfer)
                && let Some(ref dst_comment) = dst_transfer.comment
            {
                transfer.comment = Some(match transfer.comment {
                    Some(src_comment) => format!("{src_comment} | {dst_comment}"),
                    None => dst_comment.clone(),
                });
            }

            transfer
        })
        .collect()
}

fn aggregate_resharding_info(
    base_peer_id: &PeerId,
    telemetry_by_peer: &HashMap<PeerId, &TelemetryData>,
    collection_id: &String,
) -> Vec<ReshardingInfo> {
    let get_reshardings = |peer_id| {
        let telemetry = telemetry_by_peer.get(peer_id)?;

        telemetry
            .collections
            .collections
            .as_ref()?
            .iter()
            .find_map(|collection| match collection {
                CollectionTelemetryEnum::Full(coll) if coll.id == *collection_id => {
                    coll.resharding.as_ref()
                }
                CollectionTelemetryEnum::Full(_) | CollectionTelemetryEnum::Aggregated(_) => None,
            })
    };

    let get_resharding_by_peer_and_uuid =
        |peer_id, uuid| get_reshardings(peer_id)?.iter().find(|r| r.uuid == uuid);

    let Some(base_reshardings) = get_reshardings(base_peer_id) else {
        return Vec::new();
    };

    // Try to use the information from the `peer_id` mentioned in the resharding,
    // otherwise, use the one from the base telemetry
    base_reshardings
        .iter()
        .map(|base_resharding| {
            get_resharding_by_peer_and_uuid(&base_resharding.peer_id, base_resharding.uuid)
                .cloned()
                .unwrap_or_else(|| base_resharding.clone())
        })
        .collect::<Vec<_>>()
}

fn aggregate_shards(
    telemetry_by_peer: &HashMap<PeerId, &TelemetryData>,
    base_collection: &CollectionTelemetry,
    collection_id: &str,
) -> Option<Vec<DistributedShardTelemetry>> {
    let base_replicasets = base_collection.shards.as_ref()?;

    let mut distributed_shards = Vec::new();

    for base_replicaset in base_replicasets {
        // Collect all telemetry for this shard from all peers
        let mut replica_map: HashMap<PeerId, DistributedReplicaTelemetry> = HashMap::new();

        // Use replicate_states as source of truth for which replicas exist
        for (peer_id, replica_state) in &base_replicaset.replicate_states {
            // Try to find local shard info from this peer
            let peer_telemetry = telemetry_by_peer.get(peer_id);
            let local = peer_telemetry.and_then(|t| {
                let collection = get_collection_telemetry(t, collection_id)?;
                let shard_cleaning_status = collection
                    .shard_clean_tasks
                    .as_ref()
                    .and_then(|tasks| tasks.get(&base_replicaset.id).cloned());
                let replicaset = collection
                    .shards
                    .as_ref()?
                    .iter()
                    .find(|s| s.id == base_replicaset.id)?;
                let local_shard = replicaset.local.as_ref()?;
                Some((shard_cleaning_status, replicaset, local_shard))
            });

            let replica = if let Some((shard_cleaning_status, replicaset, local_shard)) = local {
                // Full replica info from local shard
                DistributedReplicaTelemetry {
                    peer_id: *peer_id,
                    state: *replica_state,
                    status: local_shard.status,
                    total_optimized_points: Some(local_shard.total_optimized_points),
                    vectors_size_bytes: local_shard.vectors_size_bytes,
                    payloads_size_bytes: local_shard.payloads_size_bytes,
                    num_points: local_shard.num_points,
                    num_vectors: local_shard.num_vectors,
                    num_vectors_by_name: local_shard.num_vectors_by_name.clone(),
                    partial_snapshot: replicaset.partial_snapshot.and_then(|partial_snapshot| {
                        (!partial_snapshot.is_empty()).then_some(partial_snapshot)
                    }),
                    shard_cleaning_status,
                }
            } else {
                // Minimal info for remote/unresponsive replica
                DistributedReplicaTelemetry {
                    peer_id: *peer_id,
                    state: *replica_state,
                    status: None,
                    total_optimized_points: None,
                    vectors_size_bytes: None,
                    payloads_size_bytes: None,
                    num_points: None,
                    num_vectors: None,
                    num_vectors_by_name: None,
                    shard_cleaning_status: None,
                    partial_snapshot: None,
                }
            };

            replica_map.insert(*peer_id, replica);
        }

        distributed_shards.push(DistributedShardTelemetry {
            id: base_replicaset.id,
            key: base_replicaset.key.clone(),
            replicas: replica_map
                .into_values()
                .sorted_by_key(|replica| replica.peer_id)
                .collect(),
        });
    }

    distributed_shards.sort_by_key(|shard| shard.id);

    Some(distributed_shards)
}

fn get_collection_telemetry<'a>(
    telemetry: &'a TelemetryData,
    collection_id: &str,
) -> Option<&'a CollectionTelemetry> {
    telemetry
        .collections
        .collections
        .as_ref()?
        .iter()
        .find_map(|c| match c {
            CollectionTelemetryEnum::Full(coll) if coll.id == collection_id => {
                Some(Box::as_ref(coll))
            }
            _ => None,
        })
}

fn aggregate_cluster_telemetry(
    base_telemetry: &TelemetryData,
    telemetry_by_peer: &HashMap<u64, &TelemetryData>,
    missing_peers: Vec<PeerId>,
) -> Option<DistributedClusterTelemetry> {
    let base_cluster = base_telemetry.cluster.as_ref()?;

    let number_of_peers = base_cluster
        .status
        .as_ref()
        .map(|status| status.number_of_peers as u64);

    let peers =
        aggregate_peers_info(missing_peers, base_cluster, telemetry_by_peer).unwrap_or_default();

    Some(DistributedClusterTelemetry {
        enabled: base_cluster.enabled,
        number_of_peers,
        peers,
    })
}

fn aggregate_peers_info(
    missing_peers: Vec<u64>,
    base_cluster: &ClusterTelemetry,
    telemetry_by_peer: &HashMap<u64, &TelemetryData>,
) -> Option<HashMap<u64, DistributedPeerInfo>> {
    let all_peers = base_cluster.peers.as_ref()?;

    let mut distributed_peers_info: HashMap<_, _> = all_peers
        .iter()
        .map(|(peer_id, peer_info)| {
            let uri = peer_info.uri.clone();

            let responsive =
                telemetry_by_peer.contains_key(peer_id) && !missing_peers.contains(peer_id);

            let details = telemetry_by_peer.get(peer_id).and_then(|peer_telemetry| {
                let Some(version) = peer_telemetry.app.as_ref().map(|t| t.version.clone()) else {
                    debug_assert!(false, "internal service should include app version");
                    return None;
                };

                let Some(status) = peer_telemetry
                    .cluster
                    .as_ref()
                    .and_then(|c| c.status.as_ref())
                else {
                    debug_assert!(
                        false,
                        "internal service should include cluster status telemetry"
                    );
                    return None;
                };

                Some(DistributedPeerDetails {
                    version,
                    role: status.role,
                    is_voter: status.is_voter,
                    term: status.term,
                    commit: status.commit,
                    num_pending_operations: status.pending_operations as u64,
                    consensus_thread_status: status.consensus_thread_status.clone(),
                })
            });

            (
                *peer_id,
                DistributedPeerInfo {
                    uri,
                    responsive,
                    details,
                },
            )
        })
        .collect();

    // Add any failed peers that aren't in the all_peers list
    for peer_id in missing_peers {
        if distributed_peers_info.contains_key(&peer_id) {
            continue;
        }
        debug_assert!(false, "all missing peers should have been listed already");

        let Some(info) = base_cluster
            .peers
            .as_ref()
            .and_then(|peers| peers.get(&peer_id))
        else {
            continue;
        };

        distributed_peers_info.insert(
            peer_id,
            DistributedPeerInfo {
                uri: info.uri.clone(),
                responsive: false,
                details: None,
            },
        );
    }

    Some(distributed_peers_info)
}

/// Return the telemetry with the highest term/commit
///
/// Picks item in the following order (`Option<(term, commit)>`):
/// - `Some((1, 0))`
/// - `Some((0, 1))`
/// - `Some((0, 0))`
/// - `None`
fn newest_telemetry(telemetries: &[TelemetryData]) -> Option<&TelemetryData> {
    telemetries.iter().max_by_key(|telemetry| {
        telemetry
            .cluster
            .as_ref()
            .and_then(|cluster| cluster.status.as_ref())
            .map(|status| (status.term, status.commit))
    })
}
