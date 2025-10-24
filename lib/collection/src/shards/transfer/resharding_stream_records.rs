use std::sync::Arc;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use parking_lot::Mutex;

use super::transfer_tasks_pool::TransferTaskProgress;
use crate::hash_ring::HashRingRouter;
use crate::operations::types::{CollectionError, CollectionResult, CountRequestInternal};
use crate::shards::CollectionId;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::shard::ShardId;
use crate::shards::shard_holder::LockedShardHolder;
use crate::shards::transfer::stream_records::TRANSFER_BATCH_SIZE;

/// Orchestrate shard transfer by streaming records, but only the points that fall into the new
/// shard.
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
pub(crate) async fn transfer_resharding_stream_records(
    shard_holder: Arc<LockedShardHolder>,
    progress: Arc<Mutex<TransferTaskProgress>>,
    shard_id: ShardId,
    remote_shard: RemoteShard,
    collection_id: &CollectionId,
) -> CollectionResult<()> {
    let remote_peer_id = remote_shard.peer_id;
    let cutoff;
    let hashring;

    log::debug!(
        "Starting shard {shard_id} transfer to peer {remote_peer_id} by reshard streaming records"
    );

    // Proxify local shard and create payload indexes on remote shard
    {
        let shard_holder = shard_holder.read().await;

        let Some(replica_set) = shard_holder.get_shard(shard_id) else {
            return Err(CollectionError::service_error(format!(
                "Shard {shard_id} cannot be proxied because it does not exist"
            )));
        };

        // Derive shard key scope for this transfer from the shard ID, get the hash ring
        let shard_key = shard_holder
            .get_shard_id_to_key_mapping()
            .get(&shard_id)
            .cloned();

        hashring = shard_holder.rings.get(&shard_key).cloned().ok_or_else(|| {
            CollectionError::service_error(format!(
                "Shard {shard_id} cannot be transferred for resharding, failed to get shard hash ring"
            ))
        })?;

        replica_set
            .proxify_local(remote_shard.clone(), Some(hashring.clone()), None)
            .await?;

        let hw_acc = HwMeasurementAcc::disposable();
        let Some(count_result) = replica_set
            .count_local(
                Arc::new(CountRequestInternal {
                    filter: None,
                    exact: false,
                }),
                None,
                hw_acc,
            )
            .await?
        else {
            return Err(CollectionError::service_error(format!(
                "Shard {shard_id} not found"
            )));
        };

        // Resharding up:
        //
        // - shards: 1 -> 2
        //   points: 100 -> 50/50
        //   transfer points of each shard: 50/1 = 50 -> 50/100 = 50%
        //   transfer fraction to each shard: 1/new_shard_count = 1/2 = 0.5
        // - shards: 2 -> 3
        //   points: 50/50 -> 33/33/33
        //   transfer points of each shard: 33/2 = 16.5 -> 16.5/50 = 33%
        //   transfer fraction to each shard: 1/new_shard_count = 1/3 = 0.33
        // - shards: 3 -> 4
        //   points: 33/33/33 -> 25/25/25/25
        //   transfer points of each shard: 25/3 = 8.3 -> 8.3/33 = 25%
        //   transfer fraction to each shard: 1/new_shard_count = 1/4 = 0.25
        //
        // Resharding down:
        //
        // - shards: 2 -> 1
        //   points: 50/50 -> 100
        //   transfer points of each shard: 50/1 = 50 -> 50/50 = 100%
        //   transfer fraction to each shard: 1/new_shard_count = 1/1 = 1.0
        // - shards: 3 -> 2
        //   points: 33/33/33 -> 50/50
        //   transfer points of each shard: 33/2 = 16.5 -> 16.5/33 = 50%
        //   transfer fraction to each shard: 1/new_shard_count = 1/2 = 0.5
        // - shards: 4 -> 3
        //   points: 25/25/25/25 -> 33/33/33
        //   transfer points of each shard: 25/3 = 8.3 -> 8.3/25 = 33%
        //   transfer fraction to each shard: 1/new_shard_count = 1/3 = 0.33
        let new_shard_count = match &hashring {
            HashRingRouter::Single(_) => {
                return Err(CollectionError::service_error(format!(
                    "Failed to do resharding transfer, hash ring for shard {shard_id} not in resharding state",
                )));
            }
            HashRingRouter::Resharding { old, new } => {
                debug_assert!(
                    old.len().abs_diff(new.len()) <= 1,
                    "expects resharding to only move up or down by one shard",
                );
                new.len()
            }
        };
        let transfer_size = count_result.count / new_shard_count;
        progress.lock().set(0, transfer_size);

        replica_set.transfer_indexes().await?;

        // Take our last seen clocks as cutoff point right before doing content batch transfers
        cutoff = replica_set.shard_recovery_point().await?;
    }

    // Transfer contents batch by batch
    log::trace!("Transferring points to shard {shard_id} by reshard streaming records");

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
            .transfer_batch(offset, TRANSFER_BATCH_SIZE, Some(&hashring), true)
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

    log::debug!(
        "Ending shard {shard_id} transfer to peer {remote_peer_id} by reshard streaming records"
    );

    Ok(())
}
