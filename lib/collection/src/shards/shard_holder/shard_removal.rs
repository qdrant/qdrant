use std::sync::Arc;

use common::fs::sync_parent_dir_async;
use fs_err::tokio as tokio_fs;
use segment::types::ShardKey;
use tokio::sync::Mutex;

use super::SharedShardHolder;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::replica_set::ShardReplicaSet;
use crate::shards::shard_config::ShardConfig;
use crate::shards::{remove_shard_initializing_flag, shard_initializing_flag_path};

pub(super) struct ShardKeyRemoval {
    pub(super) shards: Vec<Arc<ShardReplicaSet>>,
    next: Mutex<usize>,
}

impl ShardKeyRemoval {
    pub(super) fn new(shards: Vec<Arc<ShardReplicaSet>>) -> Self {
        Self {
            shards,
            next: Mutex::new(0),
        }
    }

    async fn remove(&self) -> CollectionResult<()> {
        // Serialize retries without holding the collection's shard-holder lock.
        let mut next = self.next.lock().await;
        while let Some(shard) = self.shards.get(*next) {
            remove_shard(shard).await?;
            *next += 1;
        }
        Ok(())
    }
}

impl SharedShardHolder {
    pub(crate) async fn remove_shard_key(&self, key: &ShardKey) -> CollectionResult<()> {
        let removal = self.write().await.detach_shard_key(key)?;
        self.complete_shard_key_removal(key, removal).await
    }

    /// Finish earlier removals before metadata operations can reuse their shard IDs or keys.
    pub(crate) async fn finish_shard_key_removals(&self) -> CollectionResult<()> {
        let removals: Vec<_> = self
            .read()
            .await
            .shard_key_removals
            .iter()
            .map(|(key, removal)| (key.clone(), Arc::clone(removal)))
            .collect();
        for (key, removal) in removals {
            self.complete_shard_key_removal(&key, removal).await?;
        }
        Ok(())
    }

    async fn complete_shard_key_removal(
        &self,
        key: &ShardKey,
        removal: Arc<ShardKeyRemoval>,
    ) -> CollectionResult<()> {
        removal.remove().await?;
        let mut holder = self.write().await;
        if holder
            .shard_key_removals
            .get(key)
            .is_some_and(|pending| Arc::ptr_eq(pending, &removal))
        {
            holder.shard_key_removals.remove(key);
        }
        Ok(())
    }
}

pub(super) async fn remove_shard(replica_set: &ShardReplicaSet) -> CollectionResult<()> {
    let shard_id = replica_set.shard_id;
    let shard_path = &replica_set.shard_path;
    replica_set.stop_gracefully().await;

    // Remove the config and sync its parent first, so an interrupted cleanup cannot
    // leave a loadable shard after its routing mapping has been removed.
    let shard_config_path = ShardConfig::get_config_path(shard_path);
    if let Err(err) = tokio_fs::remove_file(&shard_config_path).await {
        log::error!(
            "Failed to remove shard config file before removing the rest of the files: {err}"
        );
    }
    // A previous deletion may have completed before its future was dropped.
    if tokio_fs::try_exists(shard_path).await? {
        sync_parent_dir_async(&shard_config_path).await?;
    }

    let Some(collection_path) = shard_path.parent() else {
        return Err(CollectionError::service_error(format!(
            "Failed to get collection path of shard {shard_id} at {shard_path:?}",
        )));
    };
    // Clear the initializing flag after the config but before deleting shard data,
    // preserving crash-safety ordering and allowing this ID to be reused later.
    let shard_flag = shard_initializing_flag_path(collection_path, shard_id);
    remove_shard_initializing_flag(&shard_flag, None).await?;
    match tokio_fs::remove_dir_all(shard_path).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(CollectionError::from(err)),
    }
    Ok(())
}
