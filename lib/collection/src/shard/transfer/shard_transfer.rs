use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;

use crate::common::stoppable_task_async::{spawn_async_stoppable, StoppableAsyncTaskHandle};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shard::forward_proxy_shard::ForwardProxyShard;
use crate::shard::remote_shard::RemoteShard;
use crate::shard::shard_holder::LockedShardHolder;
use crate::shard::{ChannelService, CollectionId, PeerId, Shard, ShardId, ShardTransfer};

const TRANSFER_BATCH_SIZE: usize = 100;
const RETRY_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_RETRY_COUNT: usize = 3;

async fn transfer_batches(
    shard_holder: Arc<LockedShardHolder>,
    shard_id: ShardId,
    stopped: Arc<AtomicBool>,
) -> CollectionResult<()> {
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

pub async fn transfer_shard(
    shard_holder: Arc<LockedShardHolder>,
    shard_id: ShardId,
    collection_id: CollectionId,
    peer_id: PeerId,
    channel_service: ChannelService,
    stopped: Arc<AtomicBool>,
) -> CollectionResult<()> {
    // Initiate shard on a remote peer
    let remote_shard = RemoteShard::new(shard_id, collection_id, peer_id, channel_service);

    // ToDo: Initial fast file-based transfer.

    // ToDo: Initialize temporary shard on a remove machine
    {
        let mut shard_holder_guard = shard_holder.write().await;
        let transferring_shard = shard_holder_guard.remove_shard(shard_id);
        if let Some(Shard::Local(local_shard)) = transferring_shard {
            let proxy_shard = ForwardProxyShard::new(local_shard, remote_shard);
            shard_holder_guard.add_shard(shard_id, Shard::ForwardProxy(proxy_shard));
        } else {
            return Err(CollectionError::service_error(format!(
                "Local Shard {} does not exist",
                shard_id
            )));
        }
    }
    // Transfer contents batch by batch
    let transfer_result = transfer_batches(shard_holder.clone(), shard_id, stopped.clone()).await;

    // Finalizing transfer
    {
        let mut shard_holder_guard = shard_holder.write().await;
        let proxy_shard_opt = shard_holder_guard.remove_shard(shard_id);
        if let Some(Shard::ForwardProxy(proxy_shard)) = proxy_shard_opt {
            let (original_shard, remote_shard) = proxy_shard.deconstruct();
            match transfer_result {
                Ok(_) => {
                    shard_holder_guard.add_shard(shard_id, Shard::Remote(remote_shard));
                }
                Err(error) => {
                    shard_holder_guard.add_shard(shard_id, Shard::Local(original_shard));
                    return Err(error);
                }
            }
        } else {
            return Err(CollectionError::service_error(format!(
                "Local Shard {} does not exist",
                shard_id
            )));
        }
    }

    Ok(())
}

pub fn spawn_transfer_task<T, F>(
    shard_holder: Arc<LockedShardHolder>,
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
                shard_holder.clone(),
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
