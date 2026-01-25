use tokio::sync::{Mutex, oneshot};

use crate::operations::types::CollectionResult;
use crate::shards::local_shard::LocalShard;
use crate::update_handler::UpdateSignal;
use crate::update_workers::applied_seq::APPLIED_SEQ_SAVE_INTERVAL;

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

    /// Drop WAL from the last applied sequence number.
    /// Returns amount of removed records.
    pub async fn drop_wal(&self) -> CollectionResult<usize> {
        // First, lock the WAL to prevent new writes.
        let mut wal_lock = Mutex::lock_owned(self.wal.wal.clone()).await;

        // Next, lock the update handler to prevent changes in update handler state.
        let update_handler = self.update_handler.lock().await;

        // If there are no `op_num` in the `applied_seq`, it's unsafe to drop WAL.
        // Because there is no good save sequence number to drop from.
        let applied_seq = update_handler.applied_seq();
        if applied_seq.op_num().is_none() {
            return Ok(0);
        }

        // Send the plunger signal to the update handler.
        // It a marker that all previous updates are processed or skipped.
        let (tx, rx) = oneshot::channel();
        let plunger = UpdateSignal::Plunger(tx);
        self.update_sender.load().send(plunger).await?;

        // Set the skip updates flag to true to prevent processing new updates
        // while cleaning the update worker channel.
        update_handler
            .skip_updates_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);

        // Wait until plunger is processed. Don't propagate errors from update worker because
        // it's important to switch flag back even if plunger failed.
        let plunger_result = rx.await;

        // Update worker channel is now clean.
        // Reset the flag and check the plunger result.
        update_handler
            .skip_updates_flag
            .store(false, std::sync::atomic::Ordering::Relaxed);

        // Now the plunger result can be checked.
        // If something went wrong, WAL won't be dropped.
        plunger_result?;

        // Because WAL is locked, no new updates can be written to it.
        // It's safe to to take the applied sequence number now.
        let removed_records = if let Some(applied_seq_num) = applied_seq.op_num() {
            let last_wal_op_num = wal_lock.last_index();

            // `applied_seq_num` is persisted with `APPLIED_SEQ_SAVE_INTERVAL` step.
            // So WAL can be dropped from `applied_seq_num + APPLIED_SEQ_SAVE_INTERVAL`.
            // Add 1 also because the last applied record must stay in the WAL.
            let safe_drop_seq_num = applied_seq_num + APPLIED_SEQ_SAVE_INTERVAL + 1;

            if safe_drop_seq_num < last_wal_op_num {
                wal_lock.drop_from(safe_drop_seq_num)?;
                wal_lock.flush()?;
                (last_wal_op_num - safe_drop_seq_num) as usize
            } else {
                0
            }
        } else {
            // Unexpected case because `applied_seq_num` is checked above.
            // But to be safe, we do nothing here.
            0
        };

        Ok(removed_records)
    }
}
