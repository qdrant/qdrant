use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::shards::shard::ShardId;
use crate::shards::transfer::RecoveryStage;
use crate::shards::transfer::transfer_tasks_pool::RecoveryProgress;

/// Shared handle to a single recovery's progress.
///
/// Obtained from a [`ShardRecoveryGuard`] via [`ShardRecoveryGuard::progress_handle`]
/// and threaded into the recovery sub-steps so they update *this* recovery's progress
/// directly, rather than re-resolving the current map entry by shard id (which a newer
/// recovery for the same shard may have already replaced).
pub type RecoveryProgressHandle = Arc<Mutex<RecoveryProgress>>;

/// Tracks active snapshot recoveries on a peer (destination side of transfers),
/// keyed by shard id.
///
/// This is a cheaply cloneable handle around a shared map. Recoveries are started
/// via [`ActiveRecoveries::start`], which returns a [`ShardRecoveryGuard`] that
/// removes the entry again on drop.
#[derive(Clone, Default)]
pub struct ActiveRecoveries {
    recoveries: Arc<Mutex<HashMap<ShardId, RecoveryProgressHandle>>>,
}

impl ActiveRecoveries {
    /// Start tracking a recovery for `shard_id`, returning a guard that stops
    /// tracking on drop.
    ///
    /// Takes ownership of the shard's recovery lock, so a recovery cannot be started
    /// without holding it. See [`ShardReplicaSet::take_snapshot_recovery_lock`].
    ///
    /// [`ShardReplicaSet::take_snapshot_recovery_lock`]: crate::shards::replica_set::ShardReplicaSet::take_snapshot_recovery_lock
    pub fn start(
        &self,
        shard_id: ShardId,
        recovery_lock: tokio::sync::OwnedMutexGuard<()>,
    ) -> ShardRecoveryGuard {
        let progress = Arc::new(Mutex::new(RecoveryProgress::new()));
        self.recoveries
            .lock()
            .insert(shard_id, Arc::clone(&progress));
        ShardRecoveryGuard {
            active_recoveries: self.clone(),
            shard_id,
            progress,
            recovery_lock,
        }
    }

    /// Format the recovery progress comment for `shard_id`, if a recovery is active.
    pub fn comment(&self, shard_id: ShardId) -> Option<String> {
        self.recoveries
            .lock()
            .get(&shard_id)
            .and_then(|progress| progress.lock().format_comment())
    }

    /// Remove the entry for `shard_id`, but only if it still points at `progress`,
    /// so a newer recovery for the same shard is never clobbered.
    fn remove(&self, shard_id: ShardId, progress: &RecoveryProgressHandle) {
        let mut recoveries = self.recoveries.lock();
        if let Some(current) = recoveries.get(&shard_id)
            && Arc::ptr_eq(current, progress)
        {
            recoveries.remove(&shard_id);
        }
    }
}

/// RAII guard for an active snapshot recovery on the destination side.
///
/// While held, this is the only recovery of the shard that can make progress, and its
/// progress is registered in [`ActiveRecoveries`] so it can be reported in the transfer
/// status. On drop - including early returns, errors, and cancellation - the lock is
/// released and the entry removed, so stale recovery comments are never reported for
/// subsequent transfers.
///
/// Must be held for the whole recovery: it is what keeps a recovery that is still in
/// flight from restoring on top of one that already reported success.
pub struct ShardRecoveryGuard {
    active_recoveries: ActiveRecoveries,
    shard_id: ShardId,
    progress: RecoveryProgressHandle,
    /// Released on drop, letting the next queued recovery of this shard start.
    #[expect(dead_code, reason = "held for its `Drop`")]
    recovery_lock: tokio::sync::OwnedMutexGuard<()>,
}

impl ShardRecoveryGuard {
    /// Update the recovery stage of *this* recovery.
    pub fn set_stage(&self, stage: RecoveryStage) {
        self.progress.lock().set_stage(stage);
    }

    /// A shared handle to this recovery's progress, for threading into recovery
    /// sub-steps that need to update the stage. Bound to this guard's own entry, so
    /// updates never leak into a newer recovery for the same shard.
    pub fn progress_handle(&self) -> RecoveryProgressHandle {
        Arc::clone(&self.progress)
    }
}

impl Drop for ShardRecoveryGuard {
    fn drop(&mut self) {
        self.active_recoveries.remove(self.shard_id, &self.progress);
    }
}
