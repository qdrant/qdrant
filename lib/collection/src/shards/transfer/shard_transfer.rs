use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use crate::common::stoppable_task_async::{spawn_async_stoppable, StoppableAsyncTaskHandle};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::channel_service::ChannelService;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::replica_set::ReplicaState;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::{LockedShardHolder, ShardHolder};
use crate::shards::CollectionId;

const TRANSFER_BATCH_SIZE: usize = 100;
const RETRY_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_RETRY_COUNT: usize = 3;

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ShardTransfer {
    pub shard_id: ShardId,
    pub from: PeerId,
    pub to: PeerId,
    /// If this flag is true, the is a replication related transfer of shard from 1 peer to another
    /// Shard on original peer will not be deleted in this case
    pub sync: bool,
}

/// Unique identifier of a transfer
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ShardTransferKey {
    pub shard_id: ShardId,
    pub from: PeerId,
    pub to: PeerId,
}

impl ShardTransferKey {
    pub fn check(&self, transfer: &ShardTransfer) -> bool {
        self.shard_id == transfer.shard_id && self.from == transfer.from && self.to == transfer.to
    }
}

impl ShardTransfer {
    pub fn key(&self) -> ShardTransferKey {
        ShardTransferKey {
            shard_id: self.shard_id,
            from: self.from,
            to: self.to,
        }
    }
}

async fn transfer_batches(
    shard_holder: Arc<LockedShardHolder>,
    shard_id: ShardId,
    stopped: Arc<AtomicBool>,
) -> CollectionResult<()> {
    // Create payload indexes on the remote shard.
    {
        let shard_holder_guard = shard_holder.read().await;
        let transferring_shard_opt = shard_holder_guard.get_shard(&shard_id);
        if let Some(replica_set) = transferring_shard_opt {
            replica_set.transfer_indexes().await?;
        } else {
            // Forward proxy gone?!
            // That would be a programming error.
            return Err(CollectionError::service_error(format!(
                "Shard {} is not a forward proxy shard",
                shard_id
            )));
        }
    }

    // Transfer contents batch by batch
    let initial_offset = None;
    let mut offset = initial_offset;
    loop {
        if stopped.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(CollectionError::Cancelled {
                description: "Transfer cancelled".to_string(),
            });
        }
        let shard_holder_guard = shard_holder.read().await;
        let transferring_shard_opt = shard_holder_guard.get_shard(&shard_id);

        if let Some(replica_set) = transferring_shard_opt {
            offset = replica_set
                .transfer_batch(offset, TRANSFER_BATCH_SIZE)
                .await?;
            if offset.is_none() {
                // That was the last batch, all look good
                break;
            }
        } else {
            // Forward proxy gone?!
            // That would be a programming error.
            return Err(CollectionError::service_error(format!(
                "Shard {} is not found",
                shard_id
            )));
        }
    }
    Ok(())
}

/// Return local shard back from the forward proxy
pub async fn revert_proxy_shard_to_local(
    shard_holder: &ShardHolder,
    shard_id: ShardId,
) -> CollectionResult<bool> {
    let replica_set = match shard_holder.get_shard(&shard_id) {
        None => return Ok(false),
        Some(replica_set) => replica_set,
    };
    replica_set.un_proxify_local().await?;
    Ok(true)
}

pub async fn change_remote_shard_route(
    shard_holder: &ShardHolder,
    shard_id: ShardId,
    old_peer_id: PeerId,
    new_peer_id: PeerId,
    sync: bool,
) -> CollectionResult<bool> {
    let replica_set = match shard_holder.get_shard(&shard_id) {
        None => return Ok(false),
        Some(replica_set) => replica_set,
    };

    if replica_set.this_peer_id() != new_peer_id {
        replica_set
            .add_remote(new_peer_id, ReplicaState::Active)
            .await?;
    }

    if !sync {
        // Transfer was a move, we need to remove the old peer
        replica_set.remove_remote(old_peer_id).await?;
    }
    Ok(true)
}

/// Mark partial shard as ready
///
/// Returns `true` if the shard was promoted, `false` if the shard was not found.
pub async fn finalize_partial_shard(
    shard_holder: &ShardHolder,
    shard_id: ShardId,
) -> CollectionResult<bool> {
    let replica_set = match shard_holder.get_shard(&shard_id) {
        None => return Ok(false),
        Some(replica_set) => replica_set,
    };

    if !replica_set.has_local_shard().await {
        return Ok(false);
    }

    replica_set.set_replica_state(&replica_set.this_peer_id(), ReplicaState::Active)?;
    Ok(true)
}

/// Promotes wrapped local shard to remote shard
///
/// Returns true if the shard was promoted, false if it was already handled
pub async fn handle_transferred_shard_proxy(
    shard_holder: &ShardHolder,
    shard_id: ShardId,
    to: PeerId,
    sync: bool,
) -> CollectionResult<bool> {
    let replica_set = match shard_holder.get_shard(&shard_id) {
        None => return Ok(false),
        Some(replica_set) => replica_set,
    };

    replica_set.add_remote(to, ReplicaState::Active).await?;

    if sync {
        // Keep local shard in the replica set
        replica_set.un_proxify_local().await?;
    } else {
        // Remove local proxy
        replica_set.remove_local().await?;
    }

    Ok(true)
}

#[tracing::instrument(skip_all)]
pub async fn transfer_shard(
    shard_holder: Arc<LockedShardHolder>,
    shard_id: ShardId,
    collection_id: CollectionId,
    peer_id: PeerId,
    channel_service: ChannelService,
    stopped: Arc<AtomicBool>,
) -> CollectionResult<()> {
    // Initiate shard on a remote peer
    let remote_shard = RemoteShard::new(shard_id, collection_id.clone(), peer_id, channel_service);

    // ToDo: Initial fast file-based transfer (optional)
    // * Create shard snapshot - save the latest version of point updates in the snapshot
    // * Initiate shard, use snapshot link for initialization
    // * Transfer difference between snapshot and current shard state

    remote_shard.initiate_transfer().await?;
    {
        let shard_holder_guard = shard_holder.read().await;
        let transferring_shard = shard_holder_guard.get_shard(&shard_id);
        if let Some(replica_set) = transferring_shard {
            replica_set.proxify_local(remote_shard).await?;
        } else {
            return Err(CollectionError::service_error(format!(
                "Shard {} cannot be proxied because it does not exist",
                shard_id
            )));
        }
    };
    // Transfer contents batch by batch
    transfer_batches(shard_holder.clone(), shard_id, stopped.clone()).await
}

/// Confirms that the transfer makes sense with the current state cluster
///
/// Checks:
/// 1. If `from` and `to` exists
/// 2. If `from` have local shard and it is active
/// 3. If there is no active transfer from the same shard and peer
///
/// If validation fails, return `BadRequest` error.
pub fn validate_transfer(
    transfer: &ShardTransfer,
    all_peers: &HashSet<PeerId>,
    shard_state: Option<&HashMap<PeerId, ReplicaState>>,
    current_transfers: &HashSet<ShardTransfer>,
) -> CollectionResult<()> {
    let shard_state = if let Some(shard_state) = shard_state {
        shard_state
    } else {
        return Err(CollectionError::service_error(format!(
            "Shard {} does not exist",
            transfer.shard_id
        )));
    };

    if !all_peers.contains(&transfer.from) {
        return Err(CollectionError::bad_request(format!(
            "Peer {} does not exist",
            transfer.from
        )));
    }

    if !all_peers.contains(&transfer.to) {
        return Err(CollectionError::bad_request(format!(
            "Peer {} does not exist",
            transfer.to
        )));
    }

    if shard_state.get(&transfer.from) != Some(&ReplicaState::Active) {
        return Err(CollectionError::bad_request(format!(
            "Shard {} is not active on peer {}",
            transfer.shard_id, transfer.from
        )));
    }

    if current_transfers
        .iter()
        .any(|t| t.shard_id == transfer.shard_id && t.from == transfer.from)
    {
        return Err(CollectionError::bad_request(format!(
            "Shard {} is already being transferred from peer {}",
            transfer.shard_id, transfer.from
        )));
    }

    Ok(())
}

/// Selects a best peer to transfer shard from.
///
/// Requirements:
/// 1. Peer should have an active replica of the shard
/// 2. There should be no active transfers from this peer with the same shard
/// 3. Prefer peer with the lowest number of active transfers
///
/// If there are no peers that satisfy the requirements, returns `None`.
pub fn suggest_transfer_source(
    shard_id: ShardId,
    target_peer: PeerId,
    current_transfers: &[ShardTransfer],
    shard_peers: &HashMap<PeerId, ReplicaState>,
) -> Option<PeerId> {
    let mut candidates = HashSet::new();
    for (peer_id, state) in shard_peers {
        if *state == ReplicaState::Active && *peer_id != target_peer {
            candidates.insert(*peer_id);
        }
    }

    let currently_transferring = current_transfers
        .iter()
        .filter(|transfer| transfer.shard_id == shard_id)
        .map(|transfer| transfer.from)
        .collect::<HashSet<PeerId>>();

    candidates = candidates
        .difference(&currently_transferring)
        .cloned()
        .collect();

    let transfer_counts = current_transfers
        .iter()
        .fold(HashMap::new(), |mut counts, transfer| {
            *counts.entry(transfer.from).or_insert(0_usize) += 1;
            counts
        });

    // Sort candidates by the number of active transfers
    let mut candidates = candidates
        .into_iter()
        .map(|peer_id| (peer_id, transfer_counts.get(&peer_id).unwrap_or(&0)))
        .collect::<Vec<(PeerId, &usize)>>();
    candidates.sort_unstable_by_key(|(_, count)| **count);

    candidates.first().map(|(peer_id, _)| *peer_id)
}

/// Selects the best peer to add a replica to.
///
/// Requirements:
/// 1. Peer should not have an active replica of the shard
/// 2. Peer should have minimal number of active transfers
pub fn suggest_peer_to_add_replica(
    shard_id: ShardId,
    shard_distribution: HashMap<ShardId, HashSet<PeerId>>,
) -> Option<PeerId> {
    let mut peer_loads: HashMap<PeerId, usize> = HashMap::new();
    for peers in shard_distribution.values() {
        for peer_id in peers {
            *peer_loads.entry(*peer_id).or_insert(0_usize) += 1;
        }
    }
    let peers_with_shard = shard_distribution
        .get(&shard_id)
        .cloned()
        .unwrap_or_default();
    for peer_with_shard in peers_with_shard {
        peer_loads.remove(&peer_with_shard);
    }

    let mut candidates = peer_loads.into_iter().collect::<Vec<(PeerId, usize)>>();
    candidates.sort_unstable_by_key(|(_, count)| *count);
    candidates.first().map(|(peer_id, _)| *peer_id)
}

/// Selects the best peer to remove a replica from.
///
/// Requirements:
/// 1. Peer should have a replica of the shard
/// 2. Peer should maximal number of active shards
/// 3. Shard replica should preferably be non-active
pub fn suggest_peer_to_remove_replica(
    shard_distribution: HashMap<ShardId, HashSet<PeerId>>,
    shard_peers: HashMap<PeerId, ReplicaState>,
) -> Option<PeerId> {
    let mut peer_loads: HashMap<PeerId, usize> = HashMap::new();
    for (_, peers) in shard_distribution {
        for peer_id in peers {
            *peer_loads.entry(peer_id).or_insert(0_usize) += 1;
        }
    }

    let mut candidates: Vec<_> = shard_peers
        .into_iter()
        .map(|(peer_id, status)| {
            (
                peer_id,
                status,
                peer_loads.get(&peer_id).copied().unwrap_or(0),
            )
        })
        .collect();

    candidates.sort_unstable_by(|(_, status1, count1), (_, status2, count2)| {
        match (status1, status2) {
            (ReplicaState::Active, ReplicaState::Active) => count2.cmp(count1),
            (ReplicaState::Active, _) => Ordering::Less,
            (_, ReplicaState::Active) => Ordering::Greater,
            (_, _) => count2.cmp(count1),
        }
    });

    candidates.into_iter().next().map(|(peer_id, _, _)| peer_id)
}

#[tracing::instrument(skip_all)]
pub fn spawn_transfer_task<T, F>(
    shards_holder: Arc<LockedShardHolder>,
    transfer: ShardTransfer,
    collection_id: CollectionId,
    channel_service: ChannelService,
    on_finish: T,
    on_error: F,
) -> StoppableAsyncTaskHandle<bool>
where
    T: Future<Output = ()> + Send + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    spawn_async_stoppable(move |stopped| async move {
        let mut tries = MAX_RETRY_COUNT;
        let mut finished = false;
        while !finished && tries > 0 {
            let transfer_result = transfer_shard(
                shards_holder.clone(),
                transfer.shard_id,
                collection_id.clone(),
                transfer.to,
                channel_service.clone(),
                stopped.clone(),
            )
            .await;
            finished = match transfer_result {
                Ok(()) => true,
                Err(error) => {
                    log::error!(
                        "Failed to transfer shard {} -> {}: {}",
                        transfer.shard_id,
                        transfer.to,
                        error
                    );
                    false
                }
            };
            if !finished {
                tries -= 1;
                log::warn!(
                    "Retrying transfer shard {} -> {} (retry {})",
                    transfer.shard_id,
                    transfer.to,
                    MAX_RETRY_COUNT - tries
                );
                let exp_timeout = RETRY_TIMEOUT * (MAX_RETRY_COUNT - tries) as u32;
                sleep(exp_timeout).await;
            }
        }

        if finished {
            // On the end of transfer, the new shard is active but most likely is under the optimization
            // process. Requests to this node might be slow, but we rely on the assumption that
            // there should be at least one other replica that is not under optimization.
            on_finish.await;
        } else {
            on_error.await;
        }
        finished
    })
}
