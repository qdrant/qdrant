use std::sync::Arc;

use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::remote_shard::RemoteShard;
use crate::shards::shard::ShardId;
use crate::shards::shard_holder::LockedShardHolder;

const TRANSFER_BATCH_SIZE: usize = 100;

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
    shard_id: ShardId,
    remote_shard: RemoteShard,
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

        replica_set.proxify_local(remote_shard).await?;

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
            .transfer_batch(offset, TRANSFER_BATCH_SIZE)
            .await?;

        if offset.is_none() {
            // That was the last batch, all look good
            break;
        }
    }

    log::debug!("Ending shard {shard_id} transfer to peer {remote_peer_id} by streaming records");

    Ok(())
}
