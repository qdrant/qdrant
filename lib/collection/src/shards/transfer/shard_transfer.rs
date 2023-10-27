use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use common::defaults;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use super::ShardTransferConsensus;
use crate::common::stoppable_task_async::{spawn_async_stoppable, StoppableAsyncTaskHandle};
use crate::operations::snapshot_ops::SnapshotPriority;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::channel_service::ChannelService;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::replica_set::ReplicaState;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::{LockedShardHolder, ShardHolder};
use crate::shards::CollectionId;

const TRANSFER_BATCH_SIZE: usize = 100;
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
    stopped: Arc<AtomicBool>,
) -> CollectionResult<()> {
    let shard_id = transfer_config.shard_id;

    // Initiate shard on a remote peer
    let remote_shard = RemoteShard::new(
        shard_id,
        collection_id.clone(),
        transfer_config.to,
        channel_service.clone(),
    );

    remote_shard.initiate_transfer().await?;

    match transfer_config.method.unwrap_or_default() {
        // Transfer shard record in batches
        ShardTransferMethod::StreamRecords => {
            transfer_batches(
                shard_holder.clone(),
                shard_id,
                remote_shard,
                stopped.clone(),
            )
            .await
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
                stopped.clone(),
            )
            .await
        }
    }
}

async fn transfer_batches(
    shard_holder: Arc<LockedShardHolder>,
    shard_id: ShardId,
    remote_shard: RemoteShard,
    stopped: Arc<AtomicBool>,
) -> CollectionResult<()> {
    // Proxify local shard and create payload indexes on remote shard
    {
        let shard_holder_guard = shard_holder.read().await;
        let transferring_shard_opt = shard_holder_guard.get_shard(&shard_id);
        let Some(replica_set) = transferring_shard_opt else {
            return Err(CollectionError::service_error(format!(
                "Shard {shard_id} cannot be proxied because it does not exist"
            )));
        };

        replica_set.proxify_local(remote_shard).await?;

        replica_set.transfer_indexes().await?;
    }

    // Transfer contents batch by batch
    let mut offset = None;
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
                "Shard {shard_id} is not found"
            )));
        }
    }
    Ok(())
}

/// Orchestrate shard snapshot transfer
///
/// This is called on the sender and will arrange all that is needed for the shard snapshot
/// transfer process to a receiver.
///
/// The order of operations here is critical for correctness. Explicit synchronization across nodes
/// is used to ensure data consistency.
///
/// Before this function, this has happened:
///
/// - An empty shard is initialized on the remote
/// - Set the remote shard state to `PartialSnapshot`
///   In `PartialSnapshot` state, the remote shard will ignore all operations and other nodes will
///   prevent sending operations to it. This is critical not to modify the shard while it is being
///   recovered from the snapshot.
///
/// During this function, this happens in order:
///
/// - Queue proxy local shard
///   We queue all new operations to the shard for the remote. Once the remote is ready, we can
///   transfer all these operations to it.
/// - Create shard snapshot
///   Snapshot the shard after the queue proxy is initialized. This snapshot will be used to get
///   the shard into the same state on the remote.
/// - Recover shard snapshot on remote
///   Instruct the remote to download the snapshot from this node over HTTP, then recover it.
/// - Set shard state to `Partial`
///   After recovery, we set the shard state from `PartialSnapshot` to `Partial`. We propose an
///   operation to consensus for this. Our logic explicitly confirms that the remote reaches the
///   `Partial` state. That is critical for the remote to accept incoming operations, that also
///   confirms consensus has accepted accepted our proposal. If this fails it will be retried up to
///   three times.
/// - Transfer queued updates to remote, transform into forward proxy
///   Once the remote is in `Partial` state we can transfer all accumulated updates in the queue
///   proxy to the remote. This ensures all operations reach the recovered shard on the remote to
///   make it consistent again. When all updates are transferred, we transform the queue proxy into
///   a forward proxy to start forwarding new updates to the remote right away.
///   We transfer the queue and transform into a forward proxy right now so that we can catch any
///   errors as early as possible. The forward proxy shard we end up with will not error again once
///   we un-proxify.
/// - Wait for Partial state in our replica set
///   Wait for the remote shard to be set to `Partial` in our local replica set. That way we
///   confirm consensus has also propagated on this node.
/// - Synchronize all nodes
///   After confirming consensus propagation on this node, synchronize all nodes to reach the same
///   consensus state before finalizing the transfer. That way, we ensure we have a consistent
///   replica set state across all nodes. All nodes will have the `Partial` state, which makes the
///   shard participate on all nodes.
///
/// After this function, the following will happen:
///
/// - The local shard is un-proxified
/// - The shard transfer is finished
/// - The remote shard state is set to `Active` through consensus
#[allow(clippy::too_many_arguments)]
async fn transfer_snapshot(
    transfer_config: ShardTransfer,
    shard_holder: Arc<LockedShardHolder>,
    shard_id: ShardId,
    remote_shard: RemoteShard,
    channel_service: ChannelService,
    consensus: &dyn ShardTransferConsensus,
    snapshots_path: &Path,
    collection_name: &str,
    temp_dir: &Path,
    _stopped: Arc<AtomicBool>,
) -> CollectionResult<()> {
    let shard_holder_read = shard_holder.read().await;
    let local_rest_address = channel_service.current_rest_address(transfer_config.from)?;

    let transferring_shard = shard_holder_read.get_shard(&shard_id);
    let Some(replica_set) = transferring_shard else {
        return Err(CollectionError::service_error(format!(
            "Shard {shard_id} cannot be queue proxied because it does not exist"
        )));
    };

    // Queue proxy local shard
    replica_set
        .queue_proxify_local(remote_shard.clone())
        .await?;
    debug_assert!(
        replica_set.is_queue_proxy().await,
        "Local shard must be a queue proxy"
    );

    // Create shard snapshot
    log::trace!("Creating snapshot of shard {shard_id} for shard snapshot transfer...");
    let snapshot_description = shard_holder_read
        .create_shard_snapshot(snapshots_path, collection_name, shard_id, temp_dir)
        .await?;

    // Recover shard snapshot on remote
    let mut shard_download_url = local_rest_address;
    shard_download_url.set_path(&format!(
        "/collections/{collection_name}/shards/{shard_id}/snapshots/{}",
        &snapshot_description.name,
    ));
    log::debug!(
        "Transferring and recovering shard {shard_id} snapshot on peer {}...",
        transfer_config.to
    );
    remote_shard
        .recover_shard_snapshot_from_url(
            collection_name,
            shard_id,
            &shard_download_url,
            SnapshotPriority::ShardTransfer,
        )
        .await
        .map_err(|err| {
            CollectionError::service_error(format!(
                "Failed to recover shard snapshot on remote: {err}"
            ))
        })?;

    // Set shard state to Partial
    log::debug!("Shard {shard_id} snapshot recovered on {} for snapshot transfer, switching into next stage through consensus...", transfer_config.to);
    consensus
        .snapshot_recovered_switch_to_partial_confirm_remote(
            &transfer_config,
            collection_name,
            &remote_shard,
        )
        .await
        .map_err(|err| {
            CollectionError::service_error(format!(
                "Can't switch shard {shard_id} to Partial state after snapshot transfer: {err}"
            ))
        })?;

    // Transfer queued updates to remote, transform into forward proxy
    log::trace!("Transfer all queue proxy updates and transform into forward proxy");
    replica_set.queue_proxy_into_forward_proxy().await?;

    // Wait for Partial state in our replica set
    let partial_state = ReplicaState::Partial;
    log::trace!("Wait for local shard to reach {partial_state:?} state");
    replica_set
        .wait_for_state(
            transfer_config.to,
            partial_state,
            defaults::CONSENSUS_META_OP_WAIT,
        )
        .await
        .map_err(|err| {
            CollectionError::service_error(format!(
                "Shard being transferred did not reach {partial_state:?} state in time: {err}",
            ))
        })?;

    // Synchronize all nodes
    await_consensus_sync(consensus, &channel_service, transfer_config.from).await;

    Ok(())
}

/// Await for consensus to synchronize across all peers
///
/// This will take the current consensus state of this node. It then explicitly waits on all other
/// nodes to reach the same (or later) consensus.
///
/// If awaiting on other nodes fails for any reason, this simply continues after the consensus
/// timeout.
async fn await_consensus_sync(
    consensus: &dyn ShardTransferConsensus,
    channel_service: &ChannelService,
    this_peer_id: PeerId,
) {
    let peer_count = channel_service.id_to_address.read().len().saturating_sub(1);
    if peer_count == 0 {
        return;
    }

    let sync_consensus = async {
        let await_result = consensus
            .await_consensus_sync(this_peer_id, channel_service)
            .await;
        if let Err(err) = &await_result {
            log::warn!("All peers failed to synchronize consensus: {err}");
        }
        await_result
    };
    let timeout = sleep(defaults::CONSENSUS_META_OP_WAIT);

    log::trace!(
        "Waiting on {peer_count} peer(s) to reach consensus before finalizing shard snapshot transfer..."
    );
    tokio::select! {
        Ok(_) = sync_consensus => {
            log::trace!("All peers reached consensus");
        }
        _ = timeout => {
            log::warn!("All peers failed to synchronize consensus, continuing after timeout...");
        }
    }
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
                transfer.clone(),
                shards_holder.clone(),
                consensus.as_ref(),
                collection_id.clone(),
                &collection_name,
                channel_service.clone(),
                &snapshots_path,
                &temp_dir,
                stopped.clone(),
            )
            .await;
            finished = match transfer_result {
                Ok(()) => true,
                Err(error) => {
                    if matches!(error, CollectionError::Cancelled { .. }) {
                        return false;
                    }
                    log::error!(
                        "Failed to transfer shard {} -> {}: {error}",
                        transfer.shard_id,
                        transfer.to,
                    );

                    // Revert queue proxy if we still have any to prepare for the next attempt
                    if let Some(replica_set) =
                        shards_holder.read().await.get_shard(&transfer.shard_id)
                    {
                        replica_set.revert_queue_proxy_local().await;
                    }

                    false
                }
            };
            if stopped.load(std::sync::atomic::Ordering::Relaxed) {
                return false;
            }
            if !finished {
                tries -= 1;
                log::warn!(
                    "Retrying shard transfer {} -> {} (retry {})",
                    transfer.shard_id,
                    transfer.to,
                    MAX_RETRY_COUNT - tries
                );
                let exp_timeout = RETRY_DELAY * (MAX_RETRY_COUNT - tries) as u32;
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
