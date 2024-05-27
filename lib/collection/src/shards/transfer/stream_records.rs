use std::sync::Arc;

use parking_lot::Mutex;

use super::transfer_tasks_pool::TransferTaskProgress;
use crate::operations::types::{CollectionError, CollectionResult, CountRequestInternal};
use crate::shards::remote_shard::RemoteShard;
use crate::shards::shard::ShardId;
use crate::shards::shard_holder::LockedShardHolder;

pub(super) const TRANSFER_BATCH_SIZE: usize = 100;

/// Orchestrate shard transfer by streaming records
///
/// This is called on the sender and will arrange all that is needed for the shard transfer
/// process.
///
/// This first transfers configured indices. Then it transfers all point records in batches.
/// Updates to the local shard are forwarded to the remote concurrently.
///
/// # Cancel safety
///
/// This function is cancel safe.
pub(super) async fn transfer_stream_records(
    shard_holder: Arc<LockedShardHolder>,
    progress: Arc<Mutex<TransferTaskProgress>>,
    shard_id: ShardId,
    remote_shard: RemoteShard,
    collection_name: &str,
) -> CollectionResult<()> {
    let remote_peer_id = remote_shard.peer_id;

    log::debug!("Starting shard {shard_id} transfer to peer {remote_peer_id} by streaming records");

    // Proxify local shard and create payload indexes on remote shard
    {
        let shard_holder = shard_holder.read().await;

        let Some(replica_set) = shard_holder.get_shard(&shard_id) else {
            return Err(CollectionError::service_error(format!(
                "Shard {shard_id} cannot be proxied because it does not exist"
            )));
        };

        replica_set.proxify_local(remote_shard.clone()).await?;

        let Some(count_result) = replica_set
            .count_local(Arc::new(CountRequestInternal {
                filter: None,
                exact: true,
            }))
            .await?
        else {
            return Err(CollectionError::service_error(format!(
                "Shard {shard_id} not found"
            )));
        };
        progress.lock().points_total = count_result.count;

        replica_set.transfer_indexes().await?;
    }

    // Transfer contents batch by batch
    log::trace!("Transferring points to shard {shard_id} by streaming records");

    let mut offset = None;

    loop {
        let shard_holder = shard_holder.read().await;

        let Some(replica_set) = shard_holder.get_shard(&shard_id) else {
            // Forward proxy gone?!
            // That would be a programming error.
            return Err(CollectionError::service_error(format!(
                "Shard {shard_id} is not found"
            )));
        };

        offset = replica_set
            .transfer_batch(offset, TRANSFER_BATCH_SIZE, None)
            .await?;

        {
            let mut progress = progress.lock();
            let transferred =
                (progress.points_transferred + TRANSFER_BATCH_SIZE).min(progress.points_total);
            progress.points_transferred = transferred;
            progress.eta.set_progress(transferred);
        }

        // If this is the last batch, finalize
        if offset.is_none() {
            break;
        }
    }

    // Update cutoff point on remote shard, disallow recovery before our current last seen
    {
        let shard_holder = shard_holder.read().await;
        let Some(replica_set) = shard_holder.get_shard(&shard_id) else {
            // Forward proxy gone?!
            // That would be a programming error.
            return Err(CollectionError::service_error(format!(
                "Shard {shard_id} is not found"
            )));
        };

        let cutoff = replica_set.shard_recovery_point().await?;
        let result = remote_shard
            .update_shard_cutoff_point(collection_name, shard_id, &cutoff)
            .await;

        // Warn and ignore if remote shard is running an older version, error otherwise
        // TODO: this is fragile, improve this with stricter matches/checks
        match result {
            // This string match is fragile but there does not seem to be a better way
            Err(err)
                if err.to_string().starts_with(
                    "Service internal error: Tonic status error: status: Unimplemented",
                ) =>
            {
                log::warn!("Cannot update cutoff point on remote shard because it is running an older version, ignoring: {err}");
            }
            Err(err) => return Err(err),
            Ok(()) => {}
        }
    }

    log::debug!("Ending shard {shard_id} transfer to peer {remote_peer_id} by streaming records");

    Ok(())
}
