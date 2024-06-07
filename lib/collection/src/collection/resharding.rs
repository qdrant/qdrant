use std::path::PathBuf;
use std::sync::Arc;

use futures::Future;
use parking_lot::Mutex;

use super::Collection;
use crate::operations::types::CollectionResult;
use crate::shards::replica_set::ReplicaState;
use crate::shards::resharding::tasks_pool::{ReshardTaskItem, ReshardTaskProgress};
use crate::shards::resharding::{self, ReshardKey, ReshardState};
use crate::shards::transfer::ShardTransferConsensus;

impl Collection {
    pub async fn resharding_state(&self) -> Option<ReshardState> {
        self.shards_holder
            .read()
            .await
            .resharding_state
            .read()
            .clone()
    }

    pub async fn start_resharding<T, F>(
        &self,
        reshard_key: ReshardKey,
        consensus: Box<dyn ShardTransferConsensus>,
        temp_dir: PathBuf,
        on_finish: T,
        on_error: F,
    ) -> CollectionResult<()>
    where
        T: Future<Output = ()> + Send + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        let mut shard_holder = self.shards_holder.write().await;

        shard_holder.check_start_resharding(&reshard_key)?;

        let replica_set = self
            .create_replica_set(
                reshard_key.shard_id,
                &[reshard_key.peer_id],
                Some(ReplicaState::Resharding),
            )
            .await?;

        shard_holder.start_resharding_unchecked(reshard_key.clone(), replica_set)?;

        // If this peer is responsible for driving the resharding, start the task for it
        if reshard_key.peer_id == self.this_peer_id {
            // Stop any already active resharding task to allow starting a new one
            let mut active_reshard_tasks = self.reshard_tasks.lock().await;
            let task_result = active_reshard_tasks.stop_task(&reshard_key).await;
            debug_assert!(task_result.is_none(), "Reshard task already exists");

            let shard_holder = self.shards_holder.clone();
            let collection_id = self.id.clone();
            let collection_config = Arc::clone(&self.collection_config);
            let channel_service = self.channel_service.clone();
            let progress = Arc::new(Mutex::new(ReshardTaskProgress::new()));
            let spawned_task = resharding::spawn_resharding_task(
                shard_holder,
                progress.clone(),
                reshard_key.clone(),
                consensus,
                collection_id,
                self.path.clone(),
                collection_config,
                channel_service,
                temp_dir,
                on_finish,
                on_error,
            );

            active_reshard_tasks.add_task(
                reshard_key,
                ReshardTaskItem {
                    task: spawned_task,
                    started_at: chrono::Utc::now(),
                    progress,
                },
            );
        }

        Ok(())
    }

    pub async fn commit_hashring(&self, reshard: ReshardKey) -> CollectionResult<()> {
        self.shards_holder.write().await.commit_hashring(reshard)
    }

    pub async fn abort_resharding(&self, reshard_key: ReshardKey) -> CollectionResult<()> {
        self.shards_holder
            .write()
            .await
            .abort_resharding(reshard_key)
            .await
    }
}
