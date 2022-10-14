use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;

use crate::common::stoppable_task_async::{spawn_async_stoppable, StoppableAsyncTaskHandle};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::channel_service::ChannelService;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::replica_set::ReplicaState;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::LockedShardHolder;
use crate::shards::{CollectionId, ShardTransfer};

const TRANSFER_BATCH_SIZE: usize = 100;
const RETRY_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_RETRY_COUNT: usize = 3;
const INDEXED_THRESHOLD: f64 = 0.85;
const OPTIMIZATION_CHECK_INTERVALS: Duration = Duration::from_secs(10);
const MAX_OPTIMIZATION_TIME: Duration = Duration::from_secs(60 * 30); // 30 minutes

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
    shard_holder: Arc<LockedShardHolder>,
    shard_id: ShardId,
) -> CollectionResult<bool> {
    let shard_holder_guard = shard_holder.read().await;
    let replica_set = match shard_holder_guard.get_shard(&shard_id) {
        None => return Ok(false),
        Some(replica_set) => replica_set,
    };
    replica_set.un_proxify_local().await?;
    Ok(true)
}

pub async fn drop_partial_shard(
    shard_holder: Arc<LockedShardHolder>,
    shard_id: ShardId,
) -> CollectionResult<bool> {
    let shard_holder_guard = shard_holder.read().await;
    let replica_set = match shard_holder_guard.get_shard(&shard_id) {
        None => return Ok(false),
        Some(replica_set) => replica_set,
    };
    if replica_set.peer_state(&replica_set.this_peer_id()) == Some(ReplicaState::Partial) {
        replica_set.remove_local().await?;
        Ok(true)
    } else {
        Ok(false)
    }
}

pub async fn change_remote_shard_route(
    shard_holder: Arc<LockedShardHolder>,
    shard_id: ShardId,
    old_peer_id: PeerId,
    new_peer_id: PeerId,
    sync: bool,
) -> CollectionResult<bool> {
    let shard_holder_guard = shard_holder.read().await;
    let replica_set = match shard_holder_guard.get_shard(&shard_id) {
        None => return Ok(false),
        Some(replica_set) => replica_set,
    };

    replica_set
        .add_remote(new_peer_id, ReplicaState::Active)
        .await?;

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
    shard_holder: Arc<LockedShardHolder>,
    shard_id: ShardId,
) -> CollectionResult<bool> {
    let shard_holder = shard_holder.read().await;
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
    shard_holder: Arc<LockedShardHolder>,
    shard_id: ShardId,
    to: PeerId,
    sync: bool,
) -> CollectionResult<bool> {
    let shard_holder = shard_holder.read().await;
    let replica_set = match shard_holder.get_shard(&shard_id) {
        None => return Ok(false),
        Some(replica_set) => replica_set,
    };

    if sync {
        // Keep local shard in the replica set
        replica_set.un_proxify_local().await?;
    } else {
        // Remove local proxy
        replica_set.remove_local().await?;
    }

    replica_set.add_remote(to, ReplicaState::Active).await?;

    Ok(true)
}

/// Activate peer for replica.
///
/// Returns true if the replica was enabled, false otherwise.
pub async fn activate_peer_for_replica(
    shard_holder: Arc<LockedShardHolder>,
    shard_id: ShardId,
    replica_peer: PeerId,
) -> CollectionResult<bool> {
    let mut shard_holder_guard = shard_holder.write().await;
    let shard = shard_holder_guard.get_mut_shard(&shard_id);
    if let Some(replica_set) = shard {
        replica_set.activate_replica(replica_peer)
    } else {
        Ok(false)
    }
}

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
