use std::collections::HashSet;
use std::sync::Arc;

use common::defaults;
use parking_lot::Mutex;
use segment::types::PointIdType;

use super::orphan_check::log_other_replica_clocks;
use super::overwrite_points::overwrite_points;
use super::transfer_tasks_pool::TransferTaskProgress;
use super::{ShardTransfer, ShardTransferConsensus, TransferStage};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::CollectionId;
use crate::shards::local_shard::clock_map::RecoveryPoint;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::replica_set::ShardReplicaSet;
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::shard::ShardId;
use crate::shards::shard_holder::SharedShardHolder;
use crate::wal_delta::ahead_clocks::AheadClocks;

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
/// - Set the remote shard state to `Recovery`
///   In `Recovery` state, the remote shard will ignore all operations by default and other nodes
///   will prevent sending operations to it. Only operations that are forced will be accepted. This
///   is critical not to mess with the order of operations while recovery is happening.
///
/// During this function, this happens in order:
///
/// - Request recovery point on remote shard
///   We use the recovery point to try and resolve a WAL delta to transfer to the remote.
/// - Resolve WAL delta locally
///   Find a point in our current WAL to transfer all operations from to the remote. If we cannot
///   resolve a WAL delta, the transfer is aborted. If the resolved delta is empty, we start from
///   our last WAL entry to ensure the remote does not miss any new updates. If the remote is ahead
///   of us on its own clocks because of orphan operations, we resolve the delta from before them,
///   and overwrite the points they changed later. See [`resolve_wal_delta`].
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
///   After recovery, we set the shard state from `Recovery` to `Partial`. We propose an operation
///   to consensus for this. Our logic explicitly confirms that the remote reaches the `Partial`
///   state.
/// - Overwrite points changed by orphan operations
///   If the remote has orphan operations, we overwrite the points they changed with our versions
///   of them, through the forward proxy. The remote is in `Partial` state by now, so it accepts
///   these updates without clock tags. See [`overwrite_points`].
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
    shard_holder: SharedShardHolder,
    progress: Arc<Mutex<TransferTaskProgress>>,
    shard_id: ShardId,
    remote_shard: RemoteShard,
    consensus: &dyn ShardTransferConsensus,
    collection_id: &CollectionId,
) -> CollectionResult<()> {
    let remote_peer_id = remote_shard.peer_id;

    log::debug!("Starting shard {shard_id} transfer to peer {remote_peer_id} using diff transfer");

    // Ask remote shard on failed node for recovery point
    let recovery_point = remote_shard
        .shard_recovery_point(collection_id, shard_id)
        .await
        .map_err(|err| {
            CollectionError::service_error(format!(
                "Failed to request recovery point from remote shard: {err}"
            ))
        })?;

    let replica_set = shard_holder.read().await.get_shard(shard_id).cloned();
    let Some(replica_set) = replica_set else {
        return Err(CollectionError::service_error(format!(
            "Shard {shard_id} cannot be queue proxied because it does not exist"
        )));
    };

    // Resolve WAL delta, get the version to start the diff from
    let next_wal_version = replica_set.wal_version().await?.map(|n| n + 1);
    let ResolvedWalDelta {
        version,
        orphan_point_ids,
    } = resolve_wal_delta(
        &replica_set,
        &remote_shard,
        collection_id,
        shard_id,
        recovery_point,
    )
    .await
    .map_err(|err| {
        CollectionError::service_error(format!("Failed to resolve shard diff: {err}"))
    })?;

    // If diff is empty, queue and forward from our version to prevent losing new updates
    // See: <https://github.com/qdrant/qdrant/pull/5271>
    let wal_delta_version = version.or_else(|| {
        log::trace!("Remote shard is up-to-date and WAL diff is empty, queueing newly incoming updates (version: {next_wal_version:?})");
        next_wal_version
    });

    // Queue proxy local shard, start flushing updates to remote
    progress.lock().set_stage(TransferStage::Proxifying);
    replica_set
        .queue_proxify_local(remote_shard.clone(), wal_delta_version, progress.clone())
        .await?;
    debug_assert!(
        replica_set.is_queue_proxy().await,
        "Local shard must be a queue proxy",
    );
    progress.lock().set_stage(TransferStage::Transferring);
    log::trace!("Transfer WAL diff by transferring all current queue proxy updates");
    replica_set.queue_proxy_flush().await?;

    // Set shard state to Partial
    progress.lock().set_stage(TransferStage::WaitingConsensus);
    log::trace!(
        "Shard {shard_id} diff transferred to {remote_peer_id} for diff transfer, switching into next stage through consensus",
    );
    consensus
        .recovered_switch_to_partial_confirm_remote(&transfer_config, collection_id, &remote_shard)
        .await
        .map_err(|err| {
            CollectionError::service_error(format!(
                "Can't switch shard {shard_id} to Partial state after diff transfer: {err}"
            ))
        })?;

    // Transform queue proxy into forward proxy, transfer any remaining updates that just came in
    // After this returns, the complete WAL diff is transferred
    progress.lock().set_stage(TransferStage::FlushingQueue);
    log::trace!("Transform queue proxy into forward proxy, transferring any remaining records");
    replica_set.queue_proxy_into_forward_proxy().await?;

    // Overwrite points changed by orphan operations on the remote shard, now that we forward new
    // updates to it
    if !orphan_point_ids.is_empty() {
        progress.lock().set_stage(TransferStage::OverwritingPoints);
        log::debug!(
            "Overwriting {} points changed by orphan operations on peer {remote_peer_id}",
            orphan_point_ids.len(),
        );

        let orphan_point_ids: Vec<_> = orphan_point_ids.into_iter().collect();

        overwrite_points(&replica_set, &remote_shard, &orphan_point_ids, &progress)
            .await
            .map_err(|err| {
                CollectionError::service_error(format!(
                    "Failed to overwrite points changed by orphan operations: {err}"
                ))
            })?;
    }

    // Wait for Partial state in our replica set
    // Consensus sync is done right after this function
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

    log::debug!("Ending shard {shard_id} transfer to peer {remote_peer_id} using diff transfer");

    Ok(())
}

/// WAL delta resolved for a remote shard
struct ResolvedWalDelta {
    /// WAL record to transfer from, `None` if the remote shard has all our records
    version: Option<u64>,
    /// Points changed by orphan operations on the remote shard, to overwrite with our versions
    orphan_point_ids: HashSet<PointIdType>,
}

/// Resolve the WAL delta for the recovery point of the remote shard
///
/// The remote shard may be ahead of us on its own clocks, because it has orphan operations: its
/// peer wrote them to its local WAL as update leader, and died before sending them to the other
/// replicas. Then we ask the remote shard which points its orphan operations changed, lower its
/// recovery point to before them, and resolve the delta for that.
///
/// If the remote shard is ahead of us on clocks of other peers, we can't tell why, and fail.
async fn resolve_wal_delta(
    replica_set: &ShardReplicaSet,
    remote_shard: &RemoteShard,
    collection_id: &CollectionId,
    shard_id: ShardId,
    mut recovery_point: RecoveryPoint,
) -> CollectionResult<ResolvedWalDelta> {
    let newest_clocks = replica_set.shard_recovery_point().await?;

    let ahead_clocks = AheadClocks::new(&recovery_point, &newest_clocks, remote_shard.peer_id);

    let orphan_point_ids = match ahead_clocks {
        AheadClocks::None => HashSet::new(),

        AheadClocks::OtherPeers(ahead_clocks) => {
            return Err(CollectionError::service_error(format!(
                "Remote shard is ahead on clocks of other peers: {ahead_clocks}",
            )));
        }

        AheadClocks::Own(ahead_clocks) => {
            // Report if other active replicas have the operations we are about to treat as orphans
            log_other_replica_clocks(
                replica_set,
                remote_shard,
                collection_id,
                shard_id,
                &ahead_clocks,
                &newest_clocks,
            )
            .await;

            // Our tick for each clock the remote shard is ahead on, tick 0 for clocks we don't know
            let source_clocks = ahead_clocks.same_clocks_in(&newest_clocks);

            let orphan_points = remote_shard
                .shard_orphan_points(collection_id, shard_id, &source_clocks)
                .await?;

            // Lower the recovery point to before the orphan operations. Drop the clocks we don't
            // know. Set the others to the remote shard's last record at or below our tick, so
            // resolving the delta verifies that record against our WAL. A clock the remote shard
            // returns no record for stays ahead of ours, and resolving the delta fails.
            for clock_tag in source_clocks.iter_as_clock_tags() {
                if clock_tag.clock_tick == 0 {
                    recovery_point.remove_clock(clock_tag);
                }
            }

            for clock_tag in orphan_points.clocks {
                recovery_point.set_clock(clock_tag);
            }

            log::debug!(
                "Remote shard is ahead on its own clocks {ahead_clocks}, \
                 orphan operations changed {} points",
                orphan_points.point_ids.len(),
            );

            orphan_points.point_ids
        }
    };

    let version = replica_set.resolve_wal_delta(recovery_point).await?;

    Ok(ResolvedWalDelta {
        version,
        orphan_point_ids,
    })
}
