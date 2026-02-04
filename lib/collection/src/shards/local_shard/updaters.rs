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

    /// Stops update workers and returns any pending update operations.
    pub async fn stop_update_workers(&self) -> CollectionResult<Option<Receiver<UpdateSignal>>> {
        let mut update_handler = self.update_handler.lock().await;
        update_handler.stop_flush_worker();
        update_handler.stop_update_worker();
        update_handler.wait_workers_stops().await
    }

    /// Handles updates to the optimizer configuration by rebuilding optimizers
    /// and restarting the update handler's workers with the new configuration.
    ///
    /// ## Cancel safety
    ///
    /// This function is **not** cancel safe.
    pub async fn on_optimizer_config_update(&self) -> CollectionResult<()> {
        let config = self.collection_config.read().await;
        let mut update_handler = self.update_handler.lock().await;

        let (update_sender, update_receiver) =
            mpsc::channel(self.shared_storage_config.update_queue_size);
        // Swap to new sender - new operations will go to the new channel
        let _old_sender = self.update_sender.swap(Arc::new(update_sender));
        // Signal all workers to stop
        update_handler.stop_flush_worker();
        update_handler.stop_update_worker();

        // Wait for workers to finish and get pending operations from the old channel
        let pending_receiver = update_handler.wait_workers_stops().await?;

        // Forward pending operations from old receiver to new channel
        if let Some(mut old_receiver) = pending_receiver {
            let sender = self.update_sender.load();
            while let Ok(signal) = old_receiver.try_recv() {
                // Forward pending operations to new channel
                // Use try_send to avoid blocking - if channel is full, operations are dropped
                let _ = sender.try_send(signal);
            }
        }

        let new_optimizers = build_optimizers(
            &self.path,
            &config.params,
            &config.optimizer_config,
            &config.hnsw_config,
            &self.shared_storage_config.hnsw_global_config,
            &config.quantization_config,
        );
        update_handler.optimizers = new_optimizers.clone();
        update_handler.flush_interval_sec = config.optimizer_config.flush_interval_sec;
        update_handler.max_optimization_threads = config.optimizer_config.max_optimization_threads;
        update_handler.run_workers(update_receiver);

        self.optimizers.store(new_optimizers);

        self.update_sender.load().send(UpdateSignal::Nop).await?;

        Ok(())
    }
}
