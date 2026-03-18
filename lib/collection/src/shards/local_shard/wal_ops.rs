use std::sync::Arc;

use tokio::sync::{Mutex, mpsc};

use crate::operations::types::CollectionResult;
use crate::shards::local_shard::LocalShard;
use crate::update_handler::UpdateSignal;

impl LocalShard {
    /// WAL is keeping more data, even if truncated.
    /// Useful if we expect to read old WAL records soon.
    pub async fn set_extended_wal_retention(&self) {
        self.wal.set_extended_retention().await;
    }

    /// WAL is keeping normal amount of data after truncation.
    pub async fn set_normal_wal_retention(&self) {
        self.wal.set_normal_retention().await;
    }

    /// Truncate unapplied WAL records.
    /// Returns amount of removed records.
    pub async fn truncate_unapplied_wal(&self) -> CollectionResult<usize> {
        // First, lock the `update_lock` to prevent new updates while dropping WAL.
        let _update_lock = self.update_lock.write().await;
        let mut update_handler = self.update_handler.lock().await;

        // Create new channel - new operations will go to the new channel
        let (update_sender, update_receiver) =
            mpsc::channel(self.shared_storage_config.update_queue_size);
        let _old_sender = self.update_sender.swap(Arc::new(update_sender));

        // Signal all workers to stop
        update_handler.stop_flush_worker();
        update_handler.stop_update_worker();

        // Wait for workers to finish and get pending operations from the old channel
        let pending_receiver = update_handler.wait_workers_stops().await?;

        // Find first pending operation's op_num - this is where we truncate from
        let truncate_from_op_num = pending_receiver.and_then(|mut receiver| {
            std::iter::from_fn(|| receiver.try_recv().ok()).find_map(|signal| match signal {
                UpdateSignal::Operation(op_data) => Some(op_data.op_num),
                UpdateSignal::Nop | UpdateSignal::Plunger(_) => None,
            })
        });

        // Lock WAL and perform truncation
        let mut wal_lock = Mutex::lock_owned(self.wal.wal.clone()).await;
        let last_wal_op_num = wal_lock.last_index();

        let truncation_result: CollectionResult<usize> =
            if let Some(truncate_from_op_num) = truncate_from_op_num {
                debug_assert!(truncate_from_op_num <= last_wal_op_num);

                wal_lock.drop_from(truncate_from_op_num)?;
                wal_lock.flush()?;

                // To calculate removed records, add 1 because both are inclusive
                Ok((last_wal_op_num + 1 - truncate_from_op_num) as usize)
            } else {
                Ok(0)
            };

        // Release WAL lock before restarting workers
        drop(wal_lock);

        // Restart workers with new channel (pending operations are intentionally not forwarded)
        update_handler.run_workers(update_receiver);

        // Trigger optimizers
        let _ = self.update_sender.load().try_send(UpdateSignal::Nop);

        truncation_result
    }
}
