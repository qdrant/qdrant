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
pub mod telemetry;
pub mod transfer;
pub mod update_tracker;

#[cfg(test)]
mod test;

use std::path::{Path, PathBuf};

use channel_service::ChannelService;
use common::defaults;
use fs_err::tokio as tokio_fs;
use shard::ShardId;
use tokio::time::{sleep_until, timeout_at};
use transfer::ShardTransferConsensus;

use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::shard_config::ShardConfig;

pub type CollectionId = String;

/// Path to a shard directory
pub fn shard_path(collection_path: &Path, shard_id: ShardId) -> PathBuf {
    collection_path.join(shard_id.to_string())
}

/// Path to a shard directory
pub fn shard_initializing_flag_path(collection_path: &Path, shard_id: ShardId) -> PathBuf {
    collection_path.join(format!("shard_{shard_id}.initializing"))
}

/// Remove the shard initializing flag if it exists.
///
/// Missing the flag is success: snapshot restore and transfer setup can both try
/// to clear it, and a concurrent remover must not turn that race into a service
/// error (`NotFound` / `ENOENT`).
pub async fn remove_shard_initializing_flag(shard_flag: &Path) -> CollectionResult<()> {
    match tokio_fs::remove_file(shard_flag).await {
        Ok(()) => {
            log::debug!("Removed shard initializing flag {shard_flag:?}");
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(CollectionError::from(err)),
    }
}

/// Verify that a shard exists by loading its configuration.
/// Returns the path to the shard if it exists.
pub async fn check_shard_path(
    collection_path: &Path,
    shard_id: ShardId,
) -> CollectionResult<PathBuf> {
    let path = shard_path(collection_path, shard_id);
    let shard_config_opt = ShardConfig::load(&path)?;
    if shard_config_opt.is_some() {
        Ok(path)
    } else {
        Err(CollectionError::service_error(format!(
            "No shard found: {shard_id} at {collection_path}",
            shard_id = shard_id,
            collection_path = collection_path.display()
        )))
    }
}

pub async fn create_shard_dir(
    collection_path: &Path,
    shard_id: ShardId,
) -> CollectionResult<PathBuf> {
    let shard_path = shard_path(collection_path, shard_id);
    match tokio_fs::create_dir(&shard_path).await {
        Ok(_) => Ok(shard_path),
        // If the directory already exists, remove it and create it again
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            log::warn!("Shard path already exists, removing and creating again: {shard_path:?}");
            tokio_fs::remove_dir_all(&shard_path)
                .await
                .map_err(CollectionError::from)?;
            tokio_fs::create_dir(&shard_path)
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
    let wait_until = tokio::time::Instant::now() + defaults::CONSENSUS_META_OP_WAIT * 2;
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

#[cfg(test)]
mod remove_initializing_flag_tests {
    use super::*;

    #[tokio::test]
    async fn remove_missing_initializing_flag_is_ok() {
        let dir = tempfile::Builder::new()
            .prefix("remove-initializing-flag-")
            .tempdir()
            .unwrap();
        let flag = shard_initializing_flag_path(dir.path(), 0);

        assert!(!flag.exists());
        remove_shard_initializing_flag(&flag).await.unwrap();
        assert!(!flag.exists());
    }

    #[tokio::test]
    async fn remove_existing_initializing_flag() {
        let dir = tempfile::Builder::new()
            .prefix("remove-initializing-flag-")
            .tempdir()
            .unwrap();
        let flag = shard_initializing_flag_path(dir.path(), 7);

        tokio_fs::File::create(&flag).await.unwrap();
        assert!(flag.exists());

        remove_shard_initializing_flag(&flag).await.unwrap();
        assert!(!flag.exists());

        // Second remove (concurrent-cleaner race) must also succeed.
        remove_shard_initializing_flag(&flag).await.unwrap();
    }
}
