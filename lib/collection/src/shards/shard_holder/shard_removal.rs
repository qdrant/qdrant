use std::sync::Arc;

use common::fs::sync_parent_dir_async;
use fs_err::tokio as tokio_fs;
use segment::types::ShardKey;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use super::SharedShardHolder;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::replica_set::ShardReplicaSet;
use crate::shards::shard_config::ShardConfig;
use crate::shards::{remove_shard_initializing_flag, shard_initializing_flag_path};

pub(super) struct ShardKeyRemoval {
    pub(super) shards: Vec<Arc<ShardReplicaSet>>,
    next: Mutex<usize>,
    task: Mutex<Option<JoinHandle<CollectionResult<()>>>>,
}

impl ShardKeyRemoval {
    pub(super) fn new(shards: Vec<Arc<ShardReplicaSet>>) -> Self {
        Self {
            shards,
            next: Mutex::new(0),
            task: Mutex::new(None),
        }
    }

    fn spawn(self: &Arc<Self>) -> JoinHandle<CollectionResult<()>> {
        let removal = Arc::clone(self);
        tokio::spawn(async move {
            let result = removal.run().await;
            if let Err(err) = &result {
                log::error!("Detached shard cleanup failed and will be retained for retry: {err}");
            }
            result
        })
    }

    fn start(self: &Arc<Self>) {
        // A waiter holding this mutex already owns the running cleanup. Apply
        // must not wait for that cleanup, including when replaying the deletion.
        let Ok(mut task) = self.task.try_lock() else {
            return;
        };
        if task.as_ref().is_none_or(JoinHandle::is_finished) {
            *task = Some(self.spawn());
        }
    }

    pub(super) async fn remove(self: &Arc<Self>) -> CollectionResult<()> {
        let mut task = self.task.lock().await;
        // Await the stored handle by reference: cancellation drops only this waiter,
        // leaving the same cleanup task (including any blocking filesystem work) alive.
        let result = task.get_or_insert_with(|| self.spawn()).await;
        task.take();
        result.map_err(|err| {
            CollectionError::service_error(format!("Detached shard cleanup task failed: {err}"))
        })?
    }

    async fn run(&self) -> CollectionResult<()> {
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
        // Consensus apply commits the detach; slow shutdown and filesystem deletion
        // continue in a retained task instead of blocking the apply thread.
        removal.start();
        Ok(())
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_waiter_retains_in_flight_filesystem_task() {
        let dir = tempfile::tempdir().unwrap();
        let shard_path = dir.path().join("shard");
        fs_err::create_dir(&shard_path).unwrap();
        fs_err::write(shard_path.join("old"), []).unwrap();
        let removal = Arc::new(ShardKeyRemoval::new(Vec::new()));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let deletion_path = shard_path.clone();

        // Model tokio::fs's blocking deletion: it continues independently of
        // the future awaiting it. Pause it deterministically before touching files.
        let task = tokio::spawn(async move {
            tokio::task::spawn_blocking(move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                fs_err::remove_dir_all(deletion_path)
            })
            .await??;
            CollectionResult::Ok(())
        });
        let task_id = task.id();
        *removal.task.lock().await = Some(task);
        started_rx.await.unwrap();

        let mut first_waiter = Box::pin(removal.remove());
        let first_pending = futures::poll!(&mut first_waiter).is_pending();
        drop(first_waiter);
        let retained_task_id = removal.task.lock().await.as_ref().unwrap().id();

        let mut retry = Box::pin(removal.remove());
        let retry_pending = futures::poll!(&mut retry).is_pending();
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), retry)
            .await
            .unwrap()
            .unwrap();

        assert!(first_pending);
        assert!(
            retry_pending,
            "retry completed while the original deletion was in flight"
        );
        assert_eq!(retained_task_id, task_id);
        assert!(!shard_path.exists());
        fs_err::create_dir(&shard_path).unwrap();
        fs_err::write(shard_path.join("replacement"), []).unwrap();
        removal.remove().await.unwrap();
        assert!(shard_path.join("replacement").exists());
    }
}
