use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::shards::shard::ShardId;
use crate::shards::transfer::RecoveryStage;
use crate::shards::transfer::transfer_tasks_pool::RecoveryProgress;

/// Tracks active snapshot recoveries on a peer (destination side of transfers),
/// keyed by shard id.
///
/// This is a cheaply cloneable handle around a shared map. Recoveries are started
/// via [`ActiveRecoveries::start`], which returns a [`ShardRecoveryGuard`] that
/// removes the entry again on drop.
#[derive(Clone, Default)]
pub struct ActiveRecoveries {
    recoveries: Arc<Mutex<HashMap<ShardId, Arc<Mutex<RecoveryProgress>>>>>,
}

impl ActiveRecoveries {
    /// Start tracking a recovery for `shard_id`, returning a guard that stops
    /// tracking on drop.
    pub fn start(&self, shard_id: ShardId) -> ShardRecoveryGuard {
        let progress = Arc::new(Mutex::new(RecoveryProgress::new()));
        self.recoveries
            .lock()
            .insert(shard_id, Arc::clone(&progress));
        ShardRecoveryGuard {
            active_recoveries: self.clone(),
            shard_id,
            progress,
        }
    }

    /// Format the recovery progress comment for `shard_id`, if a recovery is active.
    pub fn comment(&self, shard_id: ShardId) -> Option<String> {
        self.recoveries
            .lock()
            .get(&shard_id)
            .and_then(|progress| progress.lock().format_comment())
    }

    /// Update the recovery stage for `shard_id`, if a recovery is active.
    pub fn set_stage(&self, shard_id: ShardId, stage: RecoveryStage) {
        if let Some(progress) = self.recoveries.lock().get(&shard_id) {
            progress.lock().set_stage(stage);
        }
    }

    /// Remove the entry for `shard_id`, but only if it still points at `progress`,
    /// so a newer recovery for the same shard is never clobbered.
    fn remove(&self, shard_id: ShardId, progress: &Arc<Mutex<RecoveryProgress>>) {
        let mut recoveries = self.recoveries.lock();
        if let Some(current) = recoveries.get(&shard_id)
            && Arc::ptr_eq(current, progress)
        {
            recoveries.remove(&shard_id);
        }
    }
}

/// RAII guard for tracking an active snapshot recovery on the destination side.
///
/// While held, the recovery progress is registered in [`ActiveRecoveries`] so it can
/// be reported in the transfer status. On drop - including early returns, errors, and
/// cancellation - the entry is removed again, ensuring that stale recovery comments
/// are never reported for subsequent transfers.
pub struct ShardRecoveryGuard {
    active_recoveries: ActiveRecoveries,
    shard_id: ShardId,
    progress: Arc<Mutex<RecoveryProgress>>,
}

impl ShardRecoveryGuard {
    /// Shared recovery progress, used to update the current recovery stage.
    pub fn progress(&self) -> &Arc<Mutex<RecoveryProgress>> {
        &self.progress
    }
}

impl Drop for ShardRecoveryGuard {
    fn drop(&mut self) {
        self.active_recoveries.remove(self.shard_id, &self.progress);
    }
}
