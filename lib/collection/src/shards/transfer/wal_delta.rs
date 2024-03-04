use std::sync::Arc;

use common::defaults;
use parking_lot::Mutex;

use super::transfer_tasks_pool::TransferTaskProgress;
use super::{ShardTransfer, ShardTransferConsensus};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::channel_service::ChannelService;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::replica_set::ReplicaState;
use crate::shards::shard::ShardId;
use crate::shards::shard_holder::LockedShardHolder;

/// Orchestrate shard diff transfer
///
/// This is called on the sender and will arrange all that is needed for the shard diff transfer
/// process to a receiver.
///
/// The order of operations here is critical for correctness. Explicit synchronization across nodes
/// is used to ensure data consistency.
///
/// Before this function, this has happened:
///
/// - The existing shard is kept on the remote
/// - Set the remote shard state to `PartialSnapshot`
///   In `PartialSnapshot` state, the remote shard will ignore all operations by default and other
///   nodes will prevent sending operations to it. Only operations that are forced will be
///   accepted. This is critical not to mess with the order of operations while recovery is
///   happening.
///
/// During this function, this happens in order:
///
/// - Request recovery point on remote shard
///   We use the recovery point to try and resolve a WAL delta to transfer to the remote.
/// - Resolve WAL delta locally
///   Find a point in our current WAL to transfer all operations from to the remote. If we cannot
///   resolve a WAL delta, the transfer is aborted.
/// - Queue proxy local shard
///   We queue all operations from the WAL delta point for the remote.
/// - Transfer queued updates to remote, transform into forward proxy
///   We transfer all accumulated updates in the queue proxy to the remote. This ensures all
///   operations reach the recovered shard on the remote to make it consistent again. When all
///   updates are transferred, we transform the queue proxy into a forward proxy to start
///   forwarding new updates to the remote right away. We transfer the queue and transform into a
///   forward proxy right now so that we can catch any errors as early as possible. The forward
///   proxy shard we end up with will not error again once we un-proxify.
/// - Set shard state to `Partial`
///   After recovery, we set the shard state from `PartialSnapshot` to `Partial`. We propose an
///   operation to consensus for this. Our logic explicitly confirms that the remote reaches the
///   `Partial` state.
/// - Wait for Partial state in our replica set
///   Wait for the remote shard to be set to `Partial` in our local replica set. That way we
///   confirm consensus has also propagated on this node.
/// - Synchronize all nodes
///   After confirming consensus propagation on this node, synchronize all nodes to reach the same
///   consensus state before finalizing the transfer. That way, we ensure we have a consistent
///   replica set state across all nodes. All nodes will have the `Partial` state, which makes the
///   shard participate on all nodes.
///
/// After this function, the following will happen:
///
/// - The local shard is un-proxified
/// - The shard transfer is finished
/// - The remote shard state is set to `Active` through consensus
///
/// # Cancel safety
///
/// This function is cancel safe.
///
/// If cancelled - the remote shard may only be partially recovered/transferred and the local shard
/// may be left in an unexpected state. This must be resolved manually in case of cancellation.
pub(super) async fn transfer_wal_delta(
    transfer_config: ShardTransfer,
    shard_holder: Arc<LockedShardHolder>,
    progress: Arc<Mutex<TransferTaskProgress>>,
    shard_id: ShardId,
    remote_shard: RemoteShard,
    channel_service: ChannelService,
    consensus: &dyn ShardTransferConsensus,
    collection_name: &str,
) -> CollectionResult<()> {
    let remote_peer_id = remote_shard.peer_id;

    log::debug!("Starting shard {shard_id} transfer to peer {remote_peer_id} using diff transfer");

    // Ask remote shard on failed node for recovery point
    let recovery_point = remote_shard
        .shard_recovery_point(collection_name, shard_id)
        .await
        .map_err(|err| {
            CollectionError::service_error(format!(
                "Failed to request recovery point from remote shard: {err}"
            ))
        })?;

    let shard_holder_read = shard_holder.read().await;

    let transferring_shard = shard_holder_read.get_shard(&shard_id);
    let Some(replica_set) = transferring_shard else {
        return Err(CollectionError::service_error(format!(
            "Shard {shard_id} cannot be queue proxied because it does not exist"
        )));
    };

    // Resolve WAL delta, get the version to start the diff from
    let wal_delta_version = replica_set
        .resolve_wal_delta(recovery_point)
        .await
        .map_err(|err| {
            CollectionError::service_error(format!("Failed to resolve shard diff: {err}"))
        })?;

    if let Some(wal_delta_version) = wal_delta_version {
        // Queue proxy local shard
        replica_set
            .queue_proxify_local(
                remote_shard.clone(),
                Some(wal_delta_version),
                Some(progress),
            )
            .await?;

        debug_assert!(
            replica_set.is_queue_proxy().await,
            "Local shard must be a queue proxy",
        );

        log::trace!("Transfer WAL diff by transferring all current queue proxy updates");
        replica_set.queue_proxy_flush().await?;
    } else {
        log::trace!("Shard is already up-to-date as WAL diff if zero records");
    }

    // Set shard state to Partial
    log::trace!("Shard {shard_id} diff transferred to {remote_peer_id} for diff transfer, switching into next stage through consensus");
    consensus
        // Note: once we migrate from partial snapshot to recovery, we give this method a proper name
        .snapshot_recovered_switch_to_partial_confirm_remote(
            &transfer_config,
            collection_name,
            &remote_shard,
        )
        .await
        .map_err(|err| {
            CollectionError::service_error(format!(
                "Can't switch shard {shard_id} to Partial state after diff transfer: {err}"
            ))
        })?;

    // Transform queue proxy into forward proxy, transfer any remaining updates that just came in
    // After this returns, the complete WAL diff is transferred
    log::trace!("Transform queue proxy into forward proxy, transferring any remaining records");
    replica_set.queue_proxy_into_forward_proxy().await?;

    // Wait for Partial state in our replica set
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

    // Synchronize all nodes
    super::await_consensus_sync(consensus, &channel_service, transfer_config.from).await;

    log::debug!("Ending shard {shard_id} transfer to peer {remote_peer_id} using diff transfer");

    Ok(())
}
