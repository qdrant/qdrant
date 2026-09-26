use std::sync::Arc;

use common::defaults;
use parking_lot::Mutex;

use super::transfer_tasks_pool::TransferTaskProgress;
use super::{ShardTransfer, ShardTransferConsensus, TransferStage, bucketed_snapshot};
use crate::operations::snapshot_ops::SnapshotPriority;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::CollectionId;
use crate::shards::channel_service::ChannelService;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::shard::ShardId;
use crate::shards::shard_holder::SharedShardHolder;

/// Orchestrate a bucketed shard snapshot transfer
///
/// Identical choreography to [`super::snapshot::transfer_snapshot`] (see that function's
/// doc comment and sequence diagram for the full state-machine detail: queue-proxy, recover
/// on remote, switch to `Partial` through consensus, flush the queue, synchronize, switch
/// to `Active`). The only difference is *how* the shard's data reaches the remote:
///
/// Rather than materializing one full tar of the shard and transferring it over a single
/// connection (what [`super::snapshot::transfer_snapshot`] does), this partitions the
/// shard's on-disk entries (segment directories, WAL, and the handful of other top-level
/// files) into a fixed number of buckets by file identity -- see
/// [`bucketed_snapshot::assign_buckets`] for the size-aware bin-packing this uses -- and has
/// the remote fetch each bucket as its own independent, self-contained tar, live, over its
/// own connection. On a WAN link where a single TCP flow can't saturate the available
/// bandwidth, this can transfer substantially faster: each bucket gets its own congestion
/// window, so the aggregate throughput approaches what several concurrent connections can
/// sustain rather than being capped by one.
///
/// # Cancel safety
///
/// This function is cancel safe.
///
/// If cancelled - the remote shard may only be partially recovered/transferred and the local
/// shard may be left in an unexpected state. This must be resolved manually in case of
/// cancellation.
#[allow(clippy::too_many_arguments)]
pub(super) async fn transfer_snapshot_bucketed(
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

    log::debug!(
        "Starting shard {shard_id} transfer to peer {remote_peer_id} using bucketed snapshot transfer"
    );

    let shard_holder_read = shard_holder.read().await;
    let local_rest_address = channel_service.current_rest_address(transfer_config.from)?;

    let transferring_shard = shard_holder_read.get_shard(shard_id);
    let Some(replica_set) = transferring_shard else {
        return Err(CollectionError::service_error(format!(
            "Shard {shard_id} cannot be queue proxied because it does not exist"
        )));
    };

    // Queue proxy local shard
    progress.lock().set_stage(TransferStage::Proxifying);
    replica_set
        .queue_proxify_local(remote_shard.clone(), None, progress.clone())
        .await?;

    debug_assert!(
        replica_set.is_queue_proxy().await,
        "Local shard must be a queue proxy",
    );

    // Partition the shard's on-disk entries into buckets and point the remote at the
    // bucketed download marker URL, rather than creating a single snapshot file.
    progress.lock().set_stage(TransferStage::CreatingSnapshot);
    log::trace!("Computing bucketed snapshot manifest for shard {shard_id} transfer");

    let shard_path = replica_set.local_shard_path().await.ok_or_else(|| {
        CollectionError::service_error(format!("Shard {shard_id} has no local data to transfer"))
    })?;

    let shard_entries = bucketed_snapshot::list_shard_entries(&shard_path).map_err(|err| {
        CollectionError::service_error(format!(
            "Failed to list shard entries for bucketed transfer: {err}"
        ))
    })?;
    let manifest =
        bucketed_snapshot::assign_buckets(&shard_entries, bucketed_snapshot::BUCKET_COUNT);
    let manifest_json = serde_json::to_string(&manifest).map_err(|err| {
        CollectionError::service_error(format!("Failed to encode bucket manifest: {err}"))
    })?;

    let encoded_collection_name = urlencoding::encode(collection_id);
    let mut shard_download_url = local_rest_address;
    shard_download_url.set_path(&format!(
        "/collections/{encoded_collection_name}/shards/{shard_id}/snapshot-buckets"
    ));
    shard_download_url
        .query_pairs_mut()
        .append_pair("bucket_manifest", &manifest_json);

    // Recover shard snapshot on remote
    progress.lock().set_stage(TransferStage::Recovering);
    log::trace!("Transferring and recovering shard {shard_id} snapshot on peer {remote_peer_id}");

    // Since we are providing access to local instance, any of the API keys can be used
    let local_api_key = channel_service
        .api_key
        .as_deref()
        .or(channel_service.alt_api_key.as_deref());

    remote_shard
        .recover_shard_snapshot_from_url(
            collection_id,
            shard_id,
            &shard_download_url,
            SnapshotPriority::ShardTransfer,
            // Provide API key here so the remote can access our snapshot
            local_api_key,
        )
        .await
        .map_err(|err| {
            CollectionError::service_error(format!(
                "Failed to recover shard snapshot on remote: {err}"
            ))
        })?;

    // Nothing was materialized on the sender for a bucketed transfer -- each bucket was
    // generated live, on demand, per request -- so there is no temp file cleanup to do
    // here, unlike a plain snapshot transfer.

    // Set shard state to Partial
    progress.lock().set_stage(TransferStage::WaitingConsensus);
    log::trace!(
        "Shard {shard_id} snapshot recovered on {remote_peer_id} for snapshot transfer, switching into next stage through consensus",
    );
    consensus
        .recovered_switch_to_partial_confirm_remote(&transfer_config, collection_id, &remote_shard)
        .await
        .map_err(|err| {
            CollectionError::service_error(format!(
                "Can't switch shard {shard_id} to Partial state after snapshot transfer: {err}"
            ))
        })?;

    // Transfer queued updates to remote, transform into forward proxy
    progress.lock().set_stage(TransferStage::FlushingQueue);
    log::trace!("Transfer all queue proxy updates and transform into forward proxy");
    replica_set.queue_proxy_into_forward_proxy().await?;

    // Wait for Partial state in our replica set
    // Consensus sync is done right after this function
    progress.lock().set_stage(TransferStage::WaitingConsensus);
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

    log::debug!(
        "Ending shard {shard_id} transfer to peer {remote_peer_id} using bucketed snapshot transfer"
    );

    Ok(())
}
