pub mod channel_service;
pub mod collection_shard_distribution;
mod conversions;
pub mod dummy_shard;
pub mod forward_proxy_shard;
pub mod local_shard;
pub mod proxy_shard;
pub mod queue_proxy_shard;
pub mod remote_shard;
pub mod replica_set;
pub mod resharding;
pub mod resolve;
pub mod shard;
pub mod shard_config;
pub mod shard_holder;
pub mod shard_trait;
pub mod shard_versioning;
pub mod telemetry;
pub mod transfer;
pub mod update_tracker;

#[cfg(test)]
mod test;

use std::path::{Path, PathBuf};

use channel_service::ChannelService;
use common::defaults;
use shard::ShardId;
use tokio::time::{sleep_until, timeout_at};
use transfer::ShardTransferConsensus;

use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::shard_versioning::versioned_shard_path;

pub type CollectionId = String;

pub type ShardVersion = usize;

pub async fn create_shard_dir(
    collection_path: &Path,
    shard_id: ShardId,
) -> CollectionResult<PathBuf> {
    let shard_path = versioned_shard_path(collection_path, shard_id, 0);
    match tokio::fs::create_dir(&shard_path).await {
        Ok(_) => Ok(shard_path),
        // If the directory already exists, remove it and create it again
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            log::warn!("Shard path already exists, removing and creating again: {shard_path:?}");
            tokio::fs::remove_dir_all(&shard_path)
                .await
                .map_err(CollectionError::from)?;
            tokio::fs::create_dir(&shard_path)
                .await
                .map_err(CollectionError::from)?;
            Ok(shard_path)
        }
        Err(e) => Err(CollectionError::from(e)),
    }
}

/// Await for consensus to synchronize across all peers
///
/// This will take the current consensus state of this node. It then explicitly waits on all other
/// nodes to reach the same (or later) consensus.
///
/// If awaiting on other nodes fails for any reason, this simply continues after the consensus
/// timeout.
///
/// # Cancel safety
///
/// This function is cancel safe.
async fn await_consensus_sync(
    consensus: &dyn ShardTransferConsensus,
    channel_service: &ChannelService,
) {
    let wait_until = tokio::time::Instant::now() + defaults::CONSENSUS_META_OP_WAIT;
    let sync_consensus =
        timeout_at(wait_until, consensus.await_consensus_sync(channel_service)).await;

    match sync_consensus {
        Ok(Ok(_)) => log::trace!("All peers reached consensus"),
        // Failed to sync explicitly, waiting until timeout to assume synchronization
        Ok(Err(err)) => {
            log::warn!("All peers failed to synchronize consensus, waiting until timeout: {err}");
            sleep_until(wait_until).await;
        }
        // Reached timeout, assume consensus is synchronized
        Err(err) => {
            log::warn!(
                "All peers failed to synchronize consensus, continuing after timeout: {err}"
            );
        }
    }
}
