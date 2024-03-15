use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::time::sleep;

use super::snapshot::transfer_snapshot;
use super::stream_records::transfer_stream_records;
use super::transfer_tasks_pool::TransferTaskProgress;
use super::wal_delta::transfer_wal_delta;
use super::{ShardTransfer, ShardTransferConsensus, ShardTransferMethod};
use crate::common::stoppable_task_async::{spawn_async_cancellable, CancellableAsyncTaskHandle};
use crate::operations::types::CollectionResult;
use crate::shards::channel_service::ChannelService;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::replica_set::ReplicaState;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::{LockedShardHolder, ShardHolder};
use crate::shards::CollectionId;

const RETRY_DELAY: Duration = Duration::from_secs(1);
pub(crate) const MAX_RETRY_COUNT: usize = 3;

/// Drive the shard transfer on the source node based on the given transfer configuration
///
/// Returns `true` if we should finalize the shard transfer. Returns `false` if we should silently
/// drop it, because it is being restarted.
///
/// # Cancel safety
///
/// This function is cancel safe.
#[allow(clippy::too_many_arguments)]
pub async fn transfer_shard(
    transfer_config: ShardTransfer,
    progress: Arc<Mutex<TransferTaskProgress>>,
    shard_holder: Arc<LockedShardHolder>,
    consensus: &dyn ShardTransferConsensus,
    collection_id: CollectionId,
    collection_name: &str,
    channel_service: ChannelService,
    snapshots_path: &Path,
    temp_dir: &Path,
) -> CollectionResult<bool> {
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
            transfer_stream_records(
                shard_holder.clone(),
                progress,
                shard_id,
                remote_shard,
                collection_name,
            )
            .await?;
        }

        // Transfer shard as snapshot
        ShardTransferMethod::Snapshot => {
            transfer_snapshot(
                transfer_config,
                shard_holder,
                progress,
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

        // Attempt to transfer WAL delta
        ShardTransferMethod::WalDelta => {
            let result = transfer_wal_delta(
                transfer_config.clone(),
                shard_holder,
                progress,
                shard_id,
                remote_shard,
                channel_service,
                consensus,
                collection_name,
            )
            .await;

            // Handle failure, fall back to default transfer method or propagate error
            if let Err(err) = result {
                let fallback_shard_transfer_method = ShardTransferMethod::default();
                log::warn!("Failed to do shard diff transfer, falling back to default method {fallback_shard_transfer_method:?}: {err}");
                let did_fall_back = transfer_shard_fallback_default(
                    transfer_config,
                    consensus,
                    collection_name,
                    fallback_shard_transfer_method,
                )
                .await?;
                if did_fall_back {
                    return Ok(false);
                } else {
                    return Err(err);
                }
            }
        }
    }

    Ok(true)
}

/// While in a shard transfer, fall back to the default shard transfer method
///
/// Returns true if we arranged falling back. Returns false if we could not fall back.
pub async fn transfer_shard_fallback_default(
    mut transfer_config: ShardTransfer,
    consensus: &dyn ShardTransferConsensus,
    collection_name: &str,
    fallback_method: ShardTransferMethod,
) -> CollectionResult<bool> {
    // Do not attempt to fall back to the same method
    let old_method = transfer_config.method;
    if old_method.map_or(false, |method| method == fallback_method) {
        log::warn!("Failed shard transfer fallback, because it would use the same transfer method: {fallback_method:?}");
        return Ok(false);
    }

    // Propose to restart transfer with a different method
    transfer_config.method.replace(fallback_method);
    consensus
        .restart_shard_transfer_confirm_and_retry(&transfer_config, collection_name)
        .await?;

    Ok(false)
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

#[allow(clippy::too_many_arguments)]
pub fn spawn_transfer_task<T, F>(
    shards_holder: Arc<LockedShardHolder>,
    progress: Arc<Mutex<TransferTaskProgress>>,
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
                    progress.clone(),
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

            let is_ok = matches!(result, Ok(Ok(true)));
            let is_err = matches!(result, Ok(Err(_)));
            let is_cancelled = result.is_err() || matches!(result, Ok(Ok(false)));

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
            Ok(Ok(true)) => on_finish.await,
            Ok(Ok(false)) => (), // do nothing, we should not finish the task
            Ok(Err(_)) => on_error.await,
            Err(_) => (), // do nothing, if task was cancelled
        }

        let is_ok_and_finish = matches!(result, Ok(Ok(true)));
        is_ok_and_finish
    })
}
