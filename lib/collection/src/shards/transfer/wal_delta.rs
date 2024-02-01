use std::sync::Arc;

use common::defaults;

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
/// TODO: describe what happens in this function, similar to our snapshot transfer description
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
    shard_id: ShardId,
    remote_shard: RemoteShard,
    channel_service: ChannelService,
    consensus: &dyn ShardTransferConsensus,
) -> CollectionResult<()> {
    let remote_peer_id = remote_shard.peer_id;

    log::debug!("Starting shard {shard_id} transfer to peer {remote_peer_id} using diff transfer");

    // TODO: ask remote for recovery point

    let shard_holder_read = shard_holder.read().await;

    let transferring_shard = shard_holder_read.get_shard(&shard_id);
    let Some(replica_set) = transferring_shard else {
        return Err(CollectionError::service_error(format!(
            "Shard {shard_id} cannot be queue proxied because it does not exist"
        )));
    };

    // TODO: resolve diff point, define proper version here!
    let from_version = 0;

    // TODO: send our last seen clock map to remote, set it as truncation point

    // Queue proxy local shard
    // TODO: we might want a different proxy type here
    replica_set
        .queue_proxify_local(remote_shard.clone(), Some(from_version))
        .await?;

    debug_assert!(
        replica_set.is_queue_proxy().await,
        "Local shard must be a queue proxy",
    );

    // Transfer queued updates to remote, transform into forward proxy
    // This way we send a complete WAL diff
    log::trace!("Transfer WAL diff by transferring all queue proxy updates and transform into forward proxy");
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
