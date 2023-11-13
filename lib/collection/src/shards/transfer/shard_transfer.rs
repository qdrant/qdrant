use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use super::snapshot::transfer_snapshot;
use super::stream_records::transfer_stream_records;
use super::ShardTransferConsensus;
use crate::common::stoppable_task_async::{spawn_async_cancellable, CancellableAsyncTaskHandle};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::channel_service::ChannelService;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::replica_set::ReplicaState;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::{LockedShardHolder, ShardHolder};
use crate::shards::CollectionId;

const RETRY_DELAY: Duration = Duration::from_secs(1);
pub(crate) const MAX_RETRY_COUNT: usize = 3;

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ShardTransfer {
    pub shard_id: ShardId,
    pub from: PeerId,
    pub to: PeerId,
    /// If this flag is true, this is a replication related transfer of shard from 1 peer to another
    /// Shard on original peer will not be deleted in this case
    pub sync: bool,
    /// Method to transfer shard with. `None` to choose automatically.
    #[serde(default)]
    pub method: Option<ShardTransferMethod>,
}

/// Unique identifier of a transfer, agnostic of transfer method
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

/// Methods for transferring a shard from one node to another.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ShardTransferMethod {
    /// Stream all shard records in batches until the whole shard is transferred.
    #[default]
    StreamRecords,
    /// Snapshot the shard, transfer and restore it on the receiver.
    Snapshot,
}

/// # Cancel safety
///
/// This function is cancel safe.
#[allow(clippy::too_many_arguments)]
pub async fn transfer_shard(
    transfer_config: ShardTransfer,
    shard_holder: Arc<LockedShardHolder>,
    consensus: &dyn ShardTransferConsensus,
    collection_id: CollectionId,
    collection_name: &str,
    channel_service: ChannelService,
    snapshots_path: &Path,
    temp_dir: &Path,
) -> CollectionResult<()> {
    let shard_id = transfer_config.shard_id;

    // Initiate shard on a remote peer
    let remote_shard = RemoteShard::new(
        shard_id,
        collection_id.clone(),
        transfer_config.to,
        channel_service.clone(),
    );

    // Prepare the remote for receiving the shard, waits for the correct state on the remote
    remote_shard.initiate_transfer().await?;

    match transfer_config.method.unwrap_or_default() {
        // Transfer shard record in batches
        ShardTransferMethod::StreamRecords => {
            transfer_stream_records(shard_holder.clone(), shard_id, remote_shard).await?;
        }

        // Transfer shard as snapshot
        ShardTransferMethod::Snapshot => {
            transfer_snapshot(
                transfer_config,
                shard_holder.clone(),
                shard_id,
                remote_shard,
                channel_service,
                consensus,
                snapshots_path,
                collection_name,
                temp_dir,
            )
            .await?;
        }
    }

    Ok(())
}

/// Return local shard back from the forward proxy
///
/// # Cancel safety
///
/// This function is cancel safe.
pub async fn revert_proxy_shard_to_local(
    shard_holder: &ShardHolder,
    shard_id: ShardId,
) -> CollectionResult<bool> {
    let replica_set = match shard_holder.get_shard(&shard_id) {
        None => return Ok(false),
        Some(replica_set) => replica_set,
    };

    // Revert queue proxy if we still have any and forget all collected updates
    replica_set.revert_queue_proxy_local().await;

    // Un-proxify local shard
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
    // TODO: Ensure cancel safety!

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
        //
        // TODO: Ensure cancel safety!
        replica_set.remove_local().await?;
    }

    Ok(true)
}

pub fn validate_transfer_exists(
    transfer_key: &ShardTransferKey,
    current_transfers: &HashSet<ShardTransfer>,
) -> CollectionResult<()> {
    if !current_transfers.iter().any(|t| &t.key() == transfer_key) {
        return Err(CollectionError::bad_request(format!(
            "There is no transfer for shard {} from {} to {}",
            transfer_key.shard_id, transfer_key.from, transfer_key.to
        )));
    }

    Ok(())
}

/// Confirms that the transfer does not conflict with any other active transfers
///
/// returns `None` if there is no conflicts, otherwise returns conflicting transfer
pub fn check_transfer_conflicts<'a, I>(
    transfer: &ShardTransfer,
    current_transfers: I,
) -> Option<ShardTransfer>
where
    I: Iterator<Item = &'a ShardTransfer>,
{
    let res = current_transfers
        .filter(|t| t.shard_id == transfer.shard_id)
        .find(|t| {
            t.from == transfer.from
                || t.to == transfer.from
                || t.from == transfer.to
                || t.to == transfer.to
        });
    res.cloned()
}

/// Same as `check_transfer_conflicts` but doesn't allow transfers to/from the same peer
/// more than once for the whole collection
pub fn check_transfer_conflicts_strict<'a, I>(
    transfer: &ShardTransfer,
    mut current_transfers: I,
) -> Option<ShardTransfer>
where
    I: Iterator<Item = &'a ShardTransfer>,
{
    let res = current_transfers.find(|t| {
        t.from == transfer.from
            || t.to == transfer.from
            || t.from == transfer.to
            || t.to == transfer.to
    });
    res.cloned()
}

/// Confirms that the transfer makes sense with the current state cluster
///
/// Checks:
/// 1. If `from` and `to` exists
/// 2. If `from` have local shard and it is active
/// 3. If there is no active transfers which involve `from` or `to`
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

    if let Some(existing_transfer) = check_transfer_conflicts(transfer, current_transfers.iter()) {
        return Err(CollectionError::bad_request(format!(
            "Shard {} is already involved in transfer {} -> {}",
            transfer.shard_id, existing_transfer.from, existing_transfer.to
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

    candidates.first().map(|(peer_id, _, _)| *peer_id)
}

#[allow(clippy::too_many_arguments)]
pub fn spawn_transfer_task<T, F>(
    shards_holder: Arc<LockedShardHolder>,
    transfer: ShardTransfer,
    consensus: Box<dyn ShardTransferConsensus>,
    collection_id: CollectionId,
    channel_service: ChannelService,
    snapshots_path: PathBuf,
    collection_name: String,
    temp_dir: PathBuf,
    on_finish: T,
    on_error: F,
) -> CancellableAsyncTaskHandle<bool>
where
    T: Future<Output = ()> + Send + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    spawn_async_cancellable(move |cancel| async move {
        let mut result = Err(cancel::Error::Cancelled);

        for attempt in 0..MAX_RETRY_COUNT {
            let future = async {
                if attempt > 0 {
                    sleep(RETRY_DELAY * attempt as u32).await;

                    log::warn!(
                        "Retrying shard transfer {collection_id}:{} -> {} (retry {attempt})",
                        transfer.shard_id,
                        transfer.to,
                    );
                }

                transfer_shard(
                    transfer.clone(),
                    shards_holder.clone(),
                    consensus.as_ref(),
                    collection_id.clone(),
                    &collection_name,
                    channel_service.clone(),
                    &snapshots_path,
                    &temp_dir,
                )
                .await
            };

            result = cancel::future::cancel_on_token(cancel.clone(), future).await;

            let is_ok = matches!(result, Ok(Ok(())));
            let is_err = matches!(result, Ok(Err(_)));
            let is_cancelled = result.is_err();

            if let Ok(Err(err)) = &result {
                log::error!(
                    "Failed to transfer shard {collection_id}:{} -> {}: {err}",
                    transfer.shard_id,
                    transfer.to,
                );
            }

            if is_err || is_cancelled {
                // Revert queue proxy if we still have any to prepare for the next attempt
                if let Some(shard) = shards_holder.read().await.get_shard(&transfer.shard_id) {
                    shard.revert_queue_proxy_local().await;
                }
            }

            if is_ok || is_cancelled {
                break;
            }
        }

        match &result {
            Ok(Ok(())) => on_finish.await,
            Ok(Err(_)) => on_error.await,
            Err(_) => (), // do nothing, if task was cancelled
        }

        let is_ok = matches!(result, Ok(Ok(())));
        is_ok
    })
}
