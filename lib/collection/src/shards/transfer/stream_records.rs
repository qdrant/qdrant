use std::sync::Arc;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use parking_lot::Mutex;
use semver::Version;
use shard::count::CountRequestInternal;

use super::transfer_tasks_pool::TransferTaskProgress;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::CollectionId;
use crate::shards::channel_service::ChannelService;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::shard::ShardId;
use crate::shards::shard_holder::SharedShardHolder;
use crate::shards::transfer::{ShardTransfer, ShardTransferConsensus, TransferStage};

pub(super) const TRANSFER_BATCH_SIZE: usize = 100;

/// Minimum version all peers need to be to use the intermediate `ActiveRead` state during transfer
const STATE_ACTIVE_READ_MIN_VERSION: Version = Version::new(1, 16, 0);

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
#[allow(clippy::too_many_arguments)]
pub(super) async fn transfer_stream_records(
    transfer_config: ShardTransfer,
    shard_holder: SharedShardHolder,
    progress: Arc<Mutex<TransferTaskProgress>>,
    shard_id: ShardId,
    remote_shard: RemoteShard,
    channel_service: &ChannelService,
    consensus: &dyn ShardTransferConsensus,
    collection_id: &CollectionId,
) -> CollectionResult<()> {
    let remote_peer_id = remote_shard.peer_id;
    let cutoff;
    let filter = transfer_config.filter;
    let merge_points = filter.is_some();

    #[cfg(feature = "staging")]
    let staging_delay = std::env::var("QDRANT_STAGING_SHARD_TRANSFER_DELAY_SEC")
        .ok()
        .map(|val| {
            std::time::Duration::from_secs_f64(
                val.parse::<f64>()
                    .expect("invalid QDRANT_STAGING_SHARD_TRANSFER_DELAY_SEC value"),
            )
        });

    // Whether we need an intermediate replica state (ActiveRead) during transfer to sync nodes
    // We use this when transferring between different shard IDs to ensure data consistency, this
    // way all readers can be switched to the new shard before any writers
    let sync_intermediate_state = transfer_config
        .to_shard_id
        .is_some_and(|id| transfer_config.shard_id != id);

    // If syncing peers with intermediate replica state, all nodes must have a certain version
    if sync_intermediate_state
        && !channel_service.all_peers_at_version(&STATE_ACTIVE_READ_MIN_VERSION)
    {
        return Err(CollectionError::service_error(format!(
            "Cannot perform shard transfer between different shards using streaming records because not all peers are version {STATE_ACTIVE_READ_MIN_VERSION} or higher"
        )));
    }

    log::debug!("Starting shard {shard_id} transfer to peer {remote_peer_id} by streaming records");

    // Proxify local shard and create payload indexes on remote shard
    progress.lock().set_stage(TransferStage::Proxifying);
    {
        let shard_holder = shard_holder.read().await;

        let Some(replica_set) = shard_holder.get_shard(shard_id) else {
            return Err(CollectionError::service_error(format!(
                "Shard {shard_id} cannot be proxied because it does not exist"
            )));
        };

        replica_set
            .proxify_local(remote_shard.clone(), None, filter.clone())
            .await?;

        // Plunge all pending operations in update queue
        // Required to ensure all operations that were in flight before the transfer are included
        // in the transfer
        progress.lock().set_stage(TransferStage::Plunging);
        let Some(plunger) = replica_set.plunge_local_async().await? else {
            return Err(CollectionError::service_error(format!(
                "Shard {shard_id} cannot be proxied because it does not exist"
            )));
        };
        plunger.await?;

        // Don't increment hardware usage for internal operations
        let hw_acc = HwMeasurementAcc::disposable();
        let Some(count_result) = replica_set
            .count_local(
                Arc::new(CountRequestInternal {
                    filter,
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
    progress.lock().set_stage(TransferStage::Transferring);
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

        let result = replica_set
            .transfer_batch(offset, TRANSFER_BATCH_SIZE, None, merge_points)
            .await?;

        offset = result.next_page_offset;
        progress.lock().add(result.count);

        #[cfg(feature = "staging")]
        if let Some(delay) = staging_delay {
            tokio::time::sleep(delay).await;
        }

        // If this is the last batch, finalize
        if offset.is_none() {
            break;
        }
    }

    // Sync all peers with intermediate replica state, switch to ActiveRead and sync all peers
    if sync_intermediate_state {
        progress.lock().set_stage(TransferStage::WaitingConsensus);
        log::trace!(
            "Shard {shard_id} recovered on {remote_peer_id} for stream records transfer, switching into next stage through consensus",
        );
        consensus
            .switch_partial_to_read_active_confirm_peers(
                channel_service,
                collection_id,
                &remote_shard,
            )
            .await
            .map_err(|err| {
                CollectionError::service_error(format!(
                    "Can't switch shard {shard_id} to ActiveRead state after stream records transfer: {err}"
                ))
            })?;
    }

    // Update cutoff point on remote shard, disallow recovery before it
    //
    // We provide it our last seen clocks from just before transferring the content batches, and
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
