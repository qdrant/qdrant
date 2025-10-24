use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::time::sleep;

use super::resharding_stream_records::transfer_resharding_stream_records;
use super::snapshot::transfer_snapshot;
use super::stream_records::transfer_stream_records;
use super::transfer_tasks_pool::TransferTaskProgress;
use super::wal_delta::transfer_wal_delta;
use super::{ShardTransfer, ShardTransferConsensus, ShardTransferMethod};
use crate::common::stoppable_task_async::{CancellableAsyncTaskHandle, spawn_async_cancellable};
use crate::operations::types::CollectionResult;
use crate::shards::channel_service::ChannelService;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::shard::ShardId;
use crate::shards::shard_holder::{LockedShardHolder, ShardHolder};
use crate::shards::{CollectionId, await_consensus_sync};

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
    channel_service: ChannelService,
    snapshots_path: &Path,
    temp_dir: &Path,
) -> CollectionResult<bool> {
    // The remote might target a different shard ID depending on the shard transfer type
    let local_shard_id = transfer_config.shard_id;
    let remote_shard_id = transfer_config.to_shard_id.unwrap_or(local_shard_id);

    // Initiate shard on a remote peer
    let remote_shard = RemoteShard::new(
        remote_shard_id,
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
                local_shard_id,
                remote_shard,
                &collection_id,
                transfer_config.filter,
            )
            .await?;
        }

        // Transfer shard record in batches for resharding
        ShardTransferMethod::ReshardingStreamRecords => {
            transfer_resharding_stream_records(
                shard_holder.clone(),
                progress,
                local_shard_id,
                remote_shard,
                &collection_id,
            )
            .await?;
        }

        // Transfer shard as snapshot
        ShardTransferMethod::Snapshot => {
            transfer_snapshot(
                transfer_config,
                shard_holder,
                progress,
                local_shard_id,
                remote_shard,
                &channel_service,
                consensus,
                snapshots_path,
                &collection_id,
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
                local_shard_id,
                remote_shard,
                consensus,
                &collection_id,
            )
            .await;

            // Handle failure, fall back to default transfer method or propagate error
            if let Err(err) = result {
                let fallback_shard_transfer_method = ShardTransferMethod::default();
                log::warn!(
                    "Failed to do shard diff transfer, falling back to default method {fallback_shard_transfer_method:?}: {err}",
                );
                let did_fall_back = transfer_shard_fallback_default(
                    transfer_config,
                    consensus,
                    &collection_id,
                    fallback_shard_transfer_method,
                )
                .await?;
                return if did_fall_back { Ok(false) } else { Err(err) };
            }
        }
    }

    // Synchronize all nodes
    // Ensure all peers have reached a state where they'll start sending incoming updates to the
    // remote shard. A lagging peer must not still have the target shard in dead/recovery state.
    // Only then can we destruct the forward proxy.
    await_consensus_sync(consensus, &channel_service).await;

    Ok(true)
}

/// While in a shard transfer, fall back to the default shard transfer method
///
/// Returns true if we arranged falling back. Returns false if we could not fall back.
pub async fn transfer_shard_fallback_default(
    mut transfer_config: ShardTransfer,
    consensus: &dyn ShardTransferConsensus,
    collection_id: &CollectionId,
    fallback_method: ShardTransferMethod,
) -> CollectionResult<bool> {
    // Do not attempt to fall back to the same method
    let old_method = transfer_config.method;
    if old_method.is_some_and(|method| method == fallback_method) {
        log::warn!(
            "Failed shard transfer fallback, because it would use the same transfer method: {fallback_method:?}",
        );
        return Ok(false);
    }

    // Propose to restart transfer with a different method
    transfer_config.method.replace(fallback_method);
    consensus
        .restart_shard_transfer_confirm_and_retry(&transfer_config, collection_id)
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
    let Some(replica_set) = shard_holder.get_shard(shard_id) else {
        return Ok(false);
    };

    // Revert queue proxy if we still have any and forget all collected updates
    replica_set.revert_queue_proxy_local().await;

    // Un-proxify local shard
    replica_set.un_proxify_local().await?;

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
                if let Some(shard) = shards_holder.read().await.get_shard(transfer.shard_id) {
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
