use std::future::Future;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;

use crate::common::stoppable_task_async::{spawn_async_stoppable, StoppableAsyncTaskHandle};
use crate::operations::types::{
    CollectionError, CollectionResult, CollectionStatus, OptimizersStatus,
};
use crate::shard::forward_proxy_shard::ForwardProxyShard;
use crate::shard::remote_shard::RemoteShard;
use crate::shard::shard_config::ShardConfig;
use crate::shard::shard_holder::LockedShardHolder;
use crate::shard::shard_versioning::drop_old_shards;
use crate::shard::{
    create_shard_dir, ChannelService, CollectionId, PeerId, Shard, ShardId, ShardOperation,
    ShardTransfer,
};

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
        if let Some(Shard::ForwardProxy(transferring_shard)) = transferring_shard_opt {
            transferring_shard.transfer_indexes().await?;
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
        if let Some(Shard::ForwardProxy(transferring_shard)) = transferring_shard_opt {
            offset = transferring_shard
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
                "Shard {} is not a forward proxy shard",
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
    let mut shard_holder_guard = shard_holder.write().await;
    let proxy_shard_opt = shard_holder_guard.remove_shard(shard_id);

    match proxy_shard_opt {
        Some(Shard::ForwardProxy(proxy_shard)) => {
            let (original_shard, _remote_shard) = proxy_shard.deconstruct();
            shard_holder_guard.add_shard(shard_id, Shard::Local(original_shard));
            Ok(true)
        }
        Some(shard) => {
            // Return the shard back
            shard_holder_guard.add_shard(shard_id, shard);
            Ok(false)
        }
        None => Ok(false),
    }
}

pub async fn drop_temporary_shard(
    shard_holder: Arc<LockedShardHolder>,
    shard_id: ShardId,
) -> CollectionResult<bool> {
    let mut shard_holder_guard = shard_holder.write().await;
    if let Some(Shard::Local(mut temp_shard)) = shard_holder_guard.take_temporary_shard(&shard_id) {
        let shard_path = temp_shard.shard_path();
        temp_shard.before_drop().await;

        drop(temp_shard);

        tokio::fs::remove_dir_all(shard_path).await?;
        Ok(true)
    } else {
        Ok(false)
    }
}

pub async fn change_remote_shard_route(
    collection_path: &Path,
    shard_holder: Arc<LockedShardHolder>,
    shard_id: ShardId,
    new_peer_id: PeerId,
) -> CollectionResult<bool> {
    let shard_holder_guard = shard_holder.read().await;

    // Ensure that the shard is a remote shard
    return if let Some(Shard::Remote(remote_shard)) = shard_holder_guard.get_shard(&shard_id) {
        if remote_shard.peer_id != new_peer_id {
            let new_shard_path = create_shard_dir(collection_path, shard_id).await?;
            ShardConfig::new_remote(new_peer_id).save(&new_shard_path)?;

            // Shard is switched on a persistence level

            drop_old_shards(collection_path, shard_id).await?;
            drop(shard_holder_guard);

            let mut shard_holder_guard = shard_holder.write().await;
            if let Some(Shard::Remote(remote_shard)) = shard_holder_guard.get_mut_shard(&shard_id) {
                remote_shard.peer_id = new_peer_id;
            }
            Ok(true)
        } else {
            // Shard is already updated
            Ok(false)
        }
    } else {
        // Shard does not exist or is not a remote shard
        Ok(false)
    };
}

/// Promote temporary shard to local shard
///
/// The temporary shard `shard_id` will replace the current local shard `shard_id`.
/// Returns `true` if the shard was promoted, `false` if the shard was not found.
pub async fn promote_temporary_shard_to_local(
    collection_id: CollectionId,
    collection_path: &Path,
    shard_holder: Arc<LockedShardHolder>,
    shard_id: ShardId,
) -> CollectionResult<bool> {
    {
        let shard_holder = shard_holder.read().await;
        let temp_shard_opt = shard_holder.get_temporary_shard(&shard_id);
        let temp_shard = match temp_shard_opt {
            Some(shard) => shard,
            None => return Ok(false),
        };
        match temp_shard {
            Shard::Local(local_temp_shard) => {
                let shard_path = local_temp_shard.shard_path();
                ShardConfig::new_local().save(&shard_path)?
            }
            _ => {
                debug_assert!(false, "Temporary shard is not local");
                return Ok(false);
            }
        }
    }

    // After ths point, if anything crushes - it will load temp shard as new local shard

    let mut shard_holder_write = shard_holder.write().await;

    match shard_holder_write.remove_temporary_shard(shard_id) {
        None => Ok(false), // no temporary shard to remove
        Some(temporary_shard) => {
            log::info!("Promoting temporary shard {}:{}", collection_id, shard_id);

            // switch shards in place
            let old_shard_opt = shard_holder_write.replace_shard(shard_id, temporary_shard);

            // release write lock to start serving data from temporary shard while cleaning up old shard
            drop(shard_holder_write);
            // After this point, we can receive all requests into already promoted shard
            // All what is left is to recycle old shard

            // cleanup old shard
            if let Some(mut old_shard) = old_shard_opt {
                // finish update tasks
                old_shard.before_drop().await;

                // force drop to release file system resources
                drop(old_shard);
            }

            // Delete all shard versions except for the last one
            drop_old_shards(collection_path, shard_id).await?;

            Ok(true)
        }
    }
}

/// Promotes wrapped local shard to remote shard
///
/// Returns true if the shard was promoted, false if it was already handled
pub async fn promote_proxy_to_remote_shard(
    collection_path: &Path,
    shard_holder: Arc<LockedShardHolder>,
    shard_id: ShardId,
    to: PeerId,
) -> CollectionResult<bool> {
    {
        let shard_holder_guard = shard_holder.read().await;
        if let Some(Shard::ForwardProxy(_)) = shard_holder_guard.get_shard(&shard_id) {
            let new_shard_path = create_shard_dir(collection_path, shard_id).await?;
            ShardConfig::new_remote(to).save(&new_shard_path)?;
        } else {
            return Ok(false);
        }
    }
    // After this point, on a fresh start the service will use new remote shard.

    let mut shard_holder_guard = shard_holder.write().await;
    let proxy_shard_opt = shard_holder_guard.remove_shard(shard_id);
    match proxy_shard_opt {
        Some(Shard::ForwardProxy(proxy_shard)) => {
            let (mut original_shard, remote_shard) = proxy_shard.deconstruct();
            shard_holder_guard.add_shard(shard_id, Shard::Remote(remote_shard));
            drop(shard_holder_guard);

            // New remote shard starts serving now
            original_shard.before_drop().await;
            drop(original_shard);

            // Delete all shard versions except for the last one
            drop_old_shards(collection_path, shard_id).await?;

            Ok(true)
        }
        Some(shard) => {
            // Return shard back
            shard_holder_guard.add_shard(shard_id, shard);
            Ok(false)
        }
        None => Ok(false),
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
        let mut shard_holder_guard = shard_holder.write().await;
        let transferring_shard = shard_holder_guard.remove_shard(shard_id);
        match transferring_shard {
            Some(Shard::Local(local_shard)) => {
                let proxy_shard = ForwardProxyShard::new(local_shard, remote_shard);
                shard_holder_guard.add_shard(shard_id, Shard::ForwardProxy(proxy_shard));
            }
            Some(shard) => {
                // return shard back
                shard_holder_guard.add_shard(shard_id, shard);
                return Err(CollectionError::service_error(format!(
                    "Shard {} does is not local",
                    shard_id
                )));
            }
            None => {
                return Err(CollectionError::service_error(format!(
                    "Local Shard {} does not exist",
                    shard_id
                )));
            }
        }
    };
    // Transfer contents batch by batch
    transfer_batches(shard_holder.clone(), shard_id, stopped.clone()).await?;

    // Validate that the new shard reached a certain level of indexing before promoting it to not slowdown the search requests
    validate_indexing_progress(shard_holder, shard_id, collection_id, peer_id, stopped).await
}

pub async fn validate_indexing_progress(
    shard_holder: Arc<LockedShardHolder>,
    shard_id: ShardId,
    collection_id: CollectionId,
    peer_id: PeerId,
    stopped: Arc<AtomicBool>,
) -> CollectionResult<()> {
    let mut attempt: u64 = 0;
    let max_attempts = MAX_OPTIMIZATION_TIME.as_secs() / OPTIMIZATION_CHECK_INTERVALS.as_secs();

    loop {
        if stopped.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(CollectionError::Cancelled {
                description: "Transfer cancelled".to_string(),
            });
        }

        let (local_info, remote_info) = {
            let shard_holder_guard = shard_holder.read().await;

            let proxy_shard = match shard_holder_guard.get_shard(&shard_id) {
                Some(Shard::ForwardProxy(forward_proxy)) => forward_proxy,
                _ => {
                    return Err(CollectionError::service_error(format!(
                        "Proxy shard is gone: {}, {}",
                        shard_id, collection_id
                    )))
                }
            };

            let local_info = proxy_shard.wrapped_shard.info().await?;
            let remote_info = proxy_shard.remote_shard.info().await?;
            (local_info, remote_info)
        };

        match remote_info.status {
            CollectionStatus::Green => break, // all good
            CollectionStatus::Yellow => {}    // ???, need to dig deeper
            CollectionStatus::Red => {
                // that's a trap!
                return Err(CollectionError::service_error(format!(
                    "Remote shard is red: {}, {}",
                    shard_id, collection_id
                )));
            }
        }

        match remote_info.optimizer_status {
            OptimizersStatus::Ok => {}
            OptimizersStatus::Error(optimizer_error) => {
                return Err(CollectionError::service_error(format!(
                    "Remote shard optimizer error: {} shard_id: {}, collection: {}",
                    optimizer_error, shard_id, collection_id
                )))
            }
        }

        // Now we try heuristics to prevent infinite awaiting of the remote shard to be green

        let expected_indexing_progress =
            (local_info.indexed_vectors_count as f64 * INDEXED_THRESHOLD) as usize;
        if remote_info.indexed_vectors_count >= expected_indexing_progress {
            break;
        }

        let indexed_vector_count = remote_info.indexed_vectors_count;

        // Report progress every 20 attempts (around 10 minutes)
        if attempt.rem_euclid(20) == 0 {
            log::info!("Waiting for optimizer on {}:{} on peer {} to finish transfer. (indexing progress {}/{})", collection_id, shard_id, peer_id, indexed_vector_count, expected_indexing_progress);
        }
        attempt += 1;

        if attempt > max_attempts {
            log::info!("Max waiting for indexing reached on {}:{} on peer {}. (indexing progress {}/{}). Finalizing transfer anyway", collection_id, shard_id, peer_id, indexed_vector_count, expected_indexing_progress);
            break;
        }

        sleep(OPTIMIZATION_CHECK_INTERVALS).await;
    }
    Ok(())
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
            on_finish.await;
        } else {
            on_error.await;
        }
        finished
    })
}
