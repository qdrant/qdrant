use ahash::{HashMap, HashMapExt};
use collection::operations::types::{ReshardingInfo, ShardTransferInfo};
use schemars::JsonSchema;
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

    /// Ongoing resharding operations
    #[serde(skip_serializing_if = "Option::is_none")]
    reshardings: Option<Vec<ReshardingInfo>>,

    /// Ongoing shard transfers
    #[serde(skip_serializing_if = "Option::is_none")]
    shard_transfers: Option<Vec<ShardTransferInfo>>,
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

        collections.insert(
            collection_id.clone(),
            DistributedCollectionTelemetry {
                id: collection_id,
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

    let get_transfer_from_source = |base_transfer: &ShardTransferInfo| {
        let ShardTransferInfo {
            shard_id,
            to_shard_id,
            from,
            to,
            sync: _,
            method: _,
            comment: _,
        } = base_transfer;

        get_transfers(*from)?.iter().find(|t| {
            t.from == *from
                && t.to == *to
                && t.shard_id == *shard_id
                && t.to_shard_id == *to_shard_id
        })
    };

    let Some(base_transfers) = get_transfers(base_peer_id) else {
        return Vec::new();
    };

    // Try to use the information from the source peer (transfer.from),
    // otherwise, use the one from the base telemetry
    base_transfers
        .iter()
        .map(|base_transfer| {
            get_transfer_from_source(base_transfer)
                .cloned()
                .unwrap_or_else(|| base_transfer.clone())
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

    let mut distributed_peers_info = HashMap::default();
    for (peer_id, peer_info) in all_peers {
        let responsive =
            telemetry_by_peer.contains_key(peer_id) && !missing_peers.contains(peer_id);

        let Some(peer_telemetry) = telemetry_by_peer.get(peer_id) else {
            debug_assert!(missing_peers.contains(peer_id), "{peer_id} should be part of missing peers");
            continue;
        };

        let Some(version) = peer_telemetry.app.as_ref().map(|t| t.version.clone()) else {
                debug_assert!(false, "internal service should include cluster status telemetry");
                continue;
        };

        let Some(status) = peer_telemetry.cluster.as_ref()
            .and_then(|c| c.status.as_ref()) else {
                debug_assert!(false, "internal service should include cluster status telemetry");
                continue;
            };

        distributed_peers_info.insert(
            *peer_id,
            DistributedPeerInfo {
                uri: peer_info.uri.clone(),
                responsive,
                details: Some(DistributedPeerDetails {
                    version,
                    role: status.role,
                    is_voter: status.is_voter,
                    term: status.term,
                    commit: status.commit,
                    num_pending_operations: status.pending_operations as u64,
                    consensus_thread_status: status.consensus_thread_status.clone(),
                }),
            },
        );
    }

    // Add any failed peers that aren't in the all_peers list
    for peer_id in missing_peers {
        debug_assert!(false, "all missing peers should have been listed already");

        if distributed_peers_info.contains_key(&peer_id) {
            continue;
        }

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
