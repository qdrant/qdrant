use std::sync::Arc;

use tokio::sync::mpsc::{self, Receiver};

use crate::operations::types::CollectionResult;
use crate::optimizers_builder::build_optimizers;
use crate::shards::local_shard::LocalShard;
use crate::update_handler::UpdateSignal;

impl LocalShard {
    pub fn trigger_optimizers(&self) {
        // Send a trigger signal and ignore errors because all error cases are acceptable:
        // - If receiver is already dead - we do not care
        // - If channel is full - optimization will be triggered by some other signal
        let _ = self.update_sender.load().try_send(UpdateSignal::Nop);
    }

    /// Stops flush worker only.
    /// This is useful for testing purposes to prevent background flushes.
    #[cfg(feature = "testing")]
    pub async fn stop_flush_worker(&self) {
        let mut update_handler = self.update_handler.lock().await;
        update_handler.stop_flush_worker()
    }

    pub async fn wait_update_workers_stop(
        &self,
    ) -> CollectionResult<Option<Receiver<UpdateSignal>>> {
        let mut update_handler = self.update_handler.lock().await;
        update_handler.wait_workers_stops().await
    }

    /// Handles updates to the optimizer configuration by rebuilding optimizers
    /// and restarting the update handler's workers with the new configuration.
    ///
    /// On failure, the error is recorded as an optimizer error on this shard, so it is exposed in
    /// the collection's optimizer status. This matters especially for background recreation, where
    /// the error would otherwise only be logged with no caller left to propagate it to.
    ///
    /// ## Cancel safety
    ///
    /// This function is **not** cancel safe.
    pub async fn on_optimizer_config_update(&self) -> CollectionResult<()> {
        let result = self.on_optimizer_config_update_impl().await;

        // Surface a failed recreation as an optimizer error, so it shows up in the optimizer status
        // instead of being silently lost on the background recreation path.
        if let Err(err) = &result {
            self.segments
                .write()
                .report_optimizer_error(format!("Failed to recreate optimizers: {err}"));
        }

        result
    }

    async fn on_optimizer_config_update_impl(&self) -> CollectionResult<()> {
        let mut update_handler = self.update_handler.lock().await;

        // Signal all workers to stop
        update_handler.stop_flush_worker();
        update_handler.stop_update_worker();

        // Wait for workers to finish and get pending operations from the old channel.
        //
        // This can take a long time, because in-flight optimizations are awaited before they stop.
        // We deliberately do not hold the `collection_config` read lock across this wait, so a
        // concurrent collection config update is not blocked on acquiring the config write lock.
        let update_receiver = update_handler.wait_workers_stops().await?;

        let update_receiver = match update_receiver {
            Some(receiver) => receiver,
            None => {
                // Receiver is destroyed, create a new channel.
                // It's not expected to happen in normal operation,
                // Because it means that there were no update workers running.
                // Just in case, we create a new channel to avoid panics in update handler.
                debug_assert!(
                    false,
                    "Update receiver was None during optimizer config update"
                );
                let (update_sender, update_receiver) =
                    mpsc::channel(self.shared_storage_config.update_queue_size);
                let _old_sender = self.update_sender.swap(Arc::new(update_sender));
                update_receiver
            }
        };

        // Read the latest config now that the workers have stopped, and build new optimizers from
        // it. Reading it here (rather than before the wait) also ensures we pick up the most recent
        // config if it changed again while we were waiting.
        let config = self.collection_config.read().await;

        let new_optimizers = build_optimizers(
            &self.path,
            self.collection_config.clone(),
            &config.params,
            &config.optimizer_config,
            &config.hnsw_config,
            &self.shared_storage_config.hnsw_global_config,
            &config.quantization_config,
        );

        update_handler.optimizers = new_optimizers.clone();
        update_handler.flush_interval_sec = config.optimizer_config.flush_interval_sec;
        update_handler.max_optimization_threads = config.optimizer_config.max_optimization_threads;
        update_handler.prevent_unoptimized = config
            .optimizer_config
            .prevent_unoptimized
            .unwrap_or_default();

        drop(config);

        update_handler.run_workers(update_receiver);

        self.optimizers.store(new_optimizers);

        self.segments.write().optimizer_errors = None;

        self.update_sender.load().send(UpdateSignal::Nop).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::budget::ResourceBudget;
    use common::save_on_disk::SaveOnDisk;
    use tempfile::Builder;
    use tokio::runtime::Handle;
    use tokio::sync::RwLock;

    use super::LocalShard;
    use crate::common::adaptive_handle::AdaptiveSearchHandle;
    use crate::shards::shard_trait::ShardOperation;
    use crate::tests::fixtures::create_collection_config;

    #[tokio::test(flavor = "multi_thread")]
    async fn optimizer_config_update_refreshes_prevent_unoptimized_flag() {
        let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
        let payload_index_schema_dir = Builder::new().prefix("qdrant-test").tempdir().unwrap();
        let payload_index_schema_file = payload_index_schema_dir.path().join("payload-schema.json");
        let payload_index_schema =
            Arc::new(SaveOnDisk::load_or_init_default(payload_index_schema_file).unwrap());

        let mut config = create_collection_config();
        config.optimizer_config.prevent_unoptimized = Some(false);

        let update_runtime = Handle::current();
        let search_runtime = AdaptiveSearchHandle::current_for_tests();
        let shard = LocalShard::build(
            0,
            "test".to_string(),
            collection_dir.path(),
            Arc::new(RwLock::new(config.clone())),
            Arc::new(Default::default()),
            payload_index_schema,
            update_runtime.clone(),
            search_runtime.clone(),
            ResourceBudget::default(),
            config.optimizer_config.clone(),
        )
        .await
        .unwrap();

        assert!(!shard.update_handler.lock().await.prevent_unoptimized);

        {
            let mut shard_config = shard.collection_config.write().await;
            shard_config.optimizer_config.prevent_unoptimized = Some(true);
        }

        shard.on_optimizer_config_update().await.unwrap();

        assert!(shard.update_handler.lock().await.prevent_unoptimized);

        {
            let mut shard_config = shard.collection_config.write().await;
            shard_config.optimizer_config.prevent_unoptimized = Some(false);
        }

        shard.on_optimizer_config_update().await.unwrap();

        assert!(!shard.update_handler.lock().await.prevent_unoptimized);

        shard.stop_gracefully().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn optimizer_config_update_clears_optimizer_errors() {
        let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
        let payload_index_schema_dir = Builder::new().prefix("qdrant-test").tempdir().unwrap();
        let payload_index_schema_file = payload_index_schema_dir.path().join("payload-schema.json");
        let payload_index_schema =
            Arc::new(SaveOnDisk::load_or_init_default(payload_index_schema_file).unwrap());

        let config = create_collection_config();

        let update_runtime = Handle::current();
        let search_runtime = AdaptiveSearchHandle::current_for_tests();
        let shard = LocalShard::build(
            0,
            "test".to_string(),
            collection_dir.path(),
            Arc::new(RwLock::new(config.clone())),
            Arc::new(Default::default()),
            payload_index_schema,
            update_runtime.clone(),
            search_runtime.clone(),
            ResourceBudget::default(),
            config.optimizer_config.clone(),
        )
        .await
        .unwrap();

        assert!(shard.segments.read().optimizer_errors.is_none());

        shard
            .segments
            .write()
            .report_optimizer_error("test optimizer error");
        assert!(shard.segments.read().optimizer_errors.is_some());

        shard.on_optimizer_config_update().await.unwrap();

        assert!(
            shard.segments.read().optimizer_errors.is_none(),
            "optimizer errors should be cleared after config update"
        );

        shard.stop_gracefully().await;
    }
}
