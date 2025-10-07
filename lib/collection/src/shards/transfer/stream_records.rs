use std::sync::Arc;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use parking_lot::Mutex;
use segment::types::Filter;

use super::transfer_tasks_pool::TransferTaskProgress;
use crate::operations::types::{CollectionError, CollectionResult, CountRequestInternal};
use crate::shards::CollectionId;
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
    collection_id: &CollectionId,
    filter: Option<Filter>,
) -> CollectionResult<()> {
    let remote_peer_id = remote_shard.peer_id;
    let cutoff;

    log::debug!("Starting shard {shard_id} transfer to peer {remote_peer_id} by streaming records");

    // Proxify local shard and create payload indexes on remote shard
    {
        let shard_holder = shard_holder.read().await;

        let Some(replica_set) = shard_holder.get_shard(shard_id) else {
            return Err(CollectionError::service_error(format!(
                "Shard {shard_id} cannot be proxied because it does not exist"
            )));
        };

        replica_set
            .proxify_local(remote_shard.clone(), None, filter)
            .await?;

        // Don't increment hardware usage for internal operations
        let hw_acc = HwMeasurementAcc::disposable();
        let Some(count_result) = replica_set
            .count_local(
                Arc::new(CountRequestInternal {
                    filter: None,
                    exact: false,
                }),
                None, // no timeout
                hw_acc,
            )
            .await?
        else {
            return Err(CollectionError::service_error(format!(
                "Shard {shard_id} not found"
            )));
        };
        progress.lock().set(0, count_result.count);

        replica_set.transfer_indexes().await?;

        // Take our last seen clocks as cutoff point right before doing content batch transfers
        cutoff = replica_set.shard_recovery_point().await?;
    }

    // Transfer contents batch by batch
    log::trace!("Transferring points to shard {shard_id} by streaming records");

    let mut offset = None;

    loop {
        let shard_holder = shard_holder.read().await;

        let Some(replica_set) = shard_holder.get_shard(shard_id) else {
            // Forward proxy gone?!
            // That would be a programming error.
            return Err(CollectionError::service_error(format!(
                "Shard {shard_id} is not found"
            )));
        };

        let (new_offset, count) = replica_set
            .transfer_batch(offset, TRANSFER_BATCH_SIZE, None, false)
            .await?;

        offset = new_offset;
        progress.lock().add(count);

        // If this is the last batch, finalize
        if offset.is_none() {
            break;
        }
    }

    // Update cutoff point on remote shard, disallow recovery before it
    //
    // We provide it our last seen clocks from just before transferrinmg the content batches, and
    // not our current last seen clocks. We're sure that after the transfer the remote must have
    // seen all point data for those clocks. While we cannot guarantee the remote has all point
    // data for our current last seen clocks because some operations may still be in flight.
    // This is a trade-off between being conservative and being too conservative.
    //
    // We must send a cutoff point to the remote so it can learn about all the clocks that exist.
    // If we don't do this it is possible the remote will never see a clock, breaking all future
    // WAL delta transfers.
    remote_shard
        .update_shard_cutoff_point(collection_id, remote_shard.id, &cutoff)
        .await?;

    log::debug!("Ending shard {shard_id} transfer to peer {remote_peer_id} by streaming records");

    Ok(())
}
