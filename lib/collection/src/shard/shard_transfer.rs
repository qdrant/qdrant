use crate::{
    CollectionConfig, CollectionError, CollectionId, CollectionResult, LocalShard, ShardId,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

/// ShardTransfer
///
/// ShardTransfer holds the state an ongoing shard transfer on the receiving end.
pub struct ShardTransfer {
    pub shard_id: ShardId,
    temporary_shard: LocalShard,
}

impl ShardTransfer {
    pub async fn new(
        shard_id: ShardId,
        collection_id: CollectionId,
        collection_path: &Path,
        shared_config: Arc<RwLock<CollectionConfig>>,
    ) -> CollectionResult<Self> {
        let shard_path = Self::create_temporary_shard_dir(collection_path, shard_id).await?;
        let temporary_shard =
            LocalShard::build(shard_id, collection_id, &shard_path, shared_config).await?;
        Ok(Self {
            shard_id,
            temporary_shard,
        })
    }

    pub fn inner_shard(&self) -> &LocalShard {
        &self.temporary_shard
    }

    /// Create directory to hold the temporary data for shard with `shard_id`
    pub async fn create_temporary_shard_dir(
        collection_path: &Path,
        shard_id: ShardId,
    ) -> CollectionResult<PathBuf> {
        let shard_path = collection_path.join(format!("{shard_id}-temp"));
        tokio::fs::create_dir_all(&shard_path)
            .await
            .map_err(|err| CollectionError::ServiceError {
                error: format!("Can't create shard {shard_id} directory. Error: {}", err),
            })?;
        Ok(shard_path)
    }

    pub async fn before_drop(&mut self) {
        self.temporary_shard.before_drop().await;
    }
}
