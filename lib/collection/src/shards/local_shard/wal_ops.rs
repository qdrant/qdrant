use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::{Mutex, oneshot};

use crate::operations::types::CollectionResult;
use crate::shards::local_shard::LocalShard;
use crate::update_handler::UpdateSignal;
use crate::update_workers::applied_seq::APPLIED_SEQ_SAVE_INTERVAL;

/// Guard that sets an atomic bool to `true` on creation and back to `false` on drop.
/// Ensures the flag is always reset even on early return or panic.
struct SkipUpdatesGuard {
    flag: Arc<AtomicBool>,
}

impl SkipUpdatesGuard {
    fn new(flag: Arc<AtomicBool>) -> Self {
        flag.store(true, Ordering::Relaxed);
        Self { flag }
    }
}

impl Drop for SkipUpdatesGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Relaxed);
    }
}

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
        // Lock also `update_handler` to access its state.
        let _update_lock = self.update_lock.write().await;
        let update_handler = self.update_handler.lock().await;

        // Next, clear the update worker channel.
        // To do this, set the skip updates flag. The guard resets it when dropped.
        // New updates won't be skipped because the update_lock is held until we're done.
        let _skip_updates_guard = SkipUpdatesGuard::new(update_handler.skip_updates.clone());

        // Then, Send the plunger signal to the update handler.
        // It's a marker that all previous updates are processed or skipped.
        let (tx, rx) = oneshot::channel();
        let plunger = UpdateSignal::SkipUpdatesPlunger(tx);
        self.update_sender.load().send(plunger).await?;
        let truncate_from_op_num = rx.await?;

        // The update worker is now idle, and no new updates are being processed.
        // It's safe to lock and drop WAL now.
        let mut wal_lock = Mutex::lock_owned(self.wal.wal.clone()).await;
        let last_wal_op_num = wal_lock.last_index();

        let Some(truncate_from_op_num) = truncate_from_op_num else {
            return Ok(0);
        };

        debug_assert!(truncate_from_op_num <= last_wal_op_num);

        wal_lock.drop_from(truncate_from_op_num)?;
        wal_lock.flush()?;

        // To calculate removed records, add 1 because both `last_wal_op_num` and `truncate_from_op_num` are inclusive.
        let removed_records = (last_wal_op_num + 1 - truncate_from_op_num) as usize;

        Ok(removed_records)
    }
}
