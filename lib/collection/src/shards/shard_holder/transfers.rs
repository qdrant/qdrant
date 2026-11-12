//! Registration and lookup of shard transfers tracked by the [`ShardHolder`].

use super::{ShardHolder, ShardTransferChange};
use crate::operations::types::{CollectionResult, ShardTransferInfo};
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::recovery_guard::ShardRecoveryGuard;
use crate::shards::transfer::transfer_tasks_pool::{TaskResult, TransferTasksPool};
use crate::shards::transfer::{ShardTransfer, ShardTransferKey, ShardTransferMethod};

impl ShardHolder {
    pub fn register_start_shard_transfer(&self, transfer: ShardTransfer) -> CollectionResult<bool> {
        let changed = self
            .shard_transfers
            .write(|transfers| transfers.insert(transfer.clone()))?;
        let _ = self
            .shard_transfer_changes
            .send(ShardTransferChange::Start(transfer));
        Ok(changed)
    }

    /// Update an existing transfer record's method in place, preserving its `sync` flag.
    /// Updating the record completes a shard-transfer restart.
    ///
    /// The record is updated in place, never removed and re-inserted, so it is always present.
    /// When restart is re-applied after a crash, it should always find transfer record
    /// and re-apply cleanly.
    ///
    /// Returns the resulting record:
    /// - No matching record: returns `None`. The record is never removed during a restart,
    ///   so this is a duplicate restart operation (the caller dismisses it).
    /// - Method already updated: returns the record unchanged, without writing.
    ///   This is a replay of operation that was already applied.
    /// - Otherwise: replaces the method and returns updated record.
    pub fn register_restart_transfer(
        &self,
        key: &ShardTransferKey,
        new_method: ShardTransferMethod,
    ) -> CollectionResult<Option<ShardTransfer>> {
        let mut updated = None;

        self.shard_transfers.write_optional(|transfers| {
            let existing = transfers.iter().find(|transfer| key.check(transfer))?;

            // Replay: the method was already updated on a previous apply.
            // Return the record without rewriting the file.
            if existing.method == Some(new_method) {
                updated = Some(existing.clone());
                return None;
            }

            // Transfer restart should only be used for *ordinary* transfers.
            // Ordinary transfers must never specify `to_shard_id` and `filter`.

            let new_transfer = ShardTransfer {
                shard_id: existing.shard_id,
                to_shard_id: None,
                from: existing.from,
                to: existing.to,
                // Preserve the sync flag from the old transfer
                sync: existing.sync,
                method: Some(new_method),
                filter: None,
            };

            let mut transfers = transfers.clone();
            transfers.retain(|transfer| !key.check(transfer));
            transfers.insert(new_transfer.clone());

            updated = Some(new_transfer);

            Some(transfers)
        })?;

        // A restart is equivalent to aborting old transfer, and starting a new one.
        // Notify watchers for `Abort` and `Start` transfer changes.
        if let Some(transfer) = &updated {
            let _ = self
                .shard_transfer_changes
                .send(ShardTransferChange::Abort(*key));

            let _ = self
                .shard_transfer_changes
                .send(ShardTransferChange::Start(transfer.clone()));
        }

        Ok(updated)
    }

    pub fn register_finish_transfer(&self, key: &ShardTransferKey) -> CollectionResult<bool> {
        let any_removed = self
            .shard_transfers
            .write(|transfers| transfers.extract_if(|transfer| key.check(transfer)).count() > 0)?;
        let _ = self
            .shard_transfer_changes
            .send(ShardTransferChange::Finish(*key));
        Ok(any_removed)
    }

    pub fn register_abort_transfer(&self, key: &ShardTransferKey) -> CollectionResult<bool> {
        let any_removed = self
            .shard_transfers
            .write(|transfers| transfers.extract_if(|transfer| key.check(transfer)).count() > 0)?;
        let _ = self
            .shard_transfer_changes
            .send(ShardTransferChange::Abort(*key));
        Ok(any_removed)
    }

    /// The count of incoming and outgoing shard transfers on the given peer
    ///
    /// This only includes shard transfers that are in consensus for the current collection. A
    /// shard transfer that has just been proposed may not be included yet.
    pub fn count_shard_transfer_io(&self, peer_id: PeerId) -> (usize, usize) {
        let (mut incoming, mut outgoing) = (0, 0);

        for transfer in self.shard_transfers.read().iter() {
            incoming += usize::from(transfer.to == peer_id);
            outgoing += usize::from(transfer.from == peer_id);
        }

        (incoming, outgoing)
    }

    /// Start a snapshot recovery of a shard (destination side).
    ///
    /// Requires the shard's recovery lock, which the returned [`ShardRecoveryGuard`]
    /// takes ownership of. Prefer [`Collection::start_shard_recovery`], which acquires
    /// the lock and calls this.
    ///
    /// [`Collection::start_shard_recovery`]: crate::collection::Collection::start_shard_recovery
    pub fn start_shard_recovery(
        &self,
        shard_id: ShardId,
        recovery_lock: tokio::sync::OwnedMutexGuard<()>,
    ) -> ShardRecoveryGuard {
        self.active_recoveries.start(shard_id, recovery_lock)
    }

    pub fn get_shard_transfer_info(
        &self,
        tasks_pool: &TransferTasksPool,
    ) -> Vec<ShardTransferInfo> {
        let mut shard_transfers = vec![];
        for shard_transfer in self.shard_transfers.read().iter() {
            let shard_id = shard_transfer.shard_id;
            let to_shard_id = shard_transfer.to_shard_id;
            let to = shard_transfer.to;
            let from = shard_transfer.from;
            let sync = shard_transfer.sync;
            let method = shard_transfer.method;

            let status = tasks_pool.get_task_status(&shard_transfer.key());
            let failed = status
                .as_ref()
                .is_some_and(|status| status.result == TaskResult::Failed);

            // Check for active recovery on destination shard first, then sender task status
            let target_shard = to_shard_id.unwrap_or(shard_id);
            let comment = self
                .active_recoveries
                .comment(target_shard)
                .or_else(|| status.map(|status| status.comment));

            shard_transfers.push(ShardTransferInfo {
                shard_id,
                to_shard_id,
                from,
                to,
                sync,
                method,
                comment,
                failed,
            })
        }
        shard_transfers.sort_by_key(|k| k.shard_id);
        shard_transfers
    }

    /// Get all transfers related to the given peer and shard ID pair
    pub fn get_related_transfers(&self, peer_id: PeerId, shard_id: ShardId) -> Vec<ShardTransfer> {
        self.get_transfers(|transfer| transfer.is_source_or_target(peer_id, shard_id))
    }

    pub fn check_transfer_exists(&self, transfer_key: &ShardTransferKey) -> bool {
        self.shard_transfers
            .read()
            .iter()
            .any(|transfer| transfer_key.check(transfer))
    }

    pub fn get_transfer(&self, transfer_key: &ShardTransferKey) -> Option<ShardTransfer> {
        self.shard_transfers
            .read()
            .iter()
            .find(|transfer| transfer_key.check(transfer))
            .cloned()
    }

    pub fn get_transfers<F>(&self, mut predicate: F) -> Vec<ShardTransfer>
    where
        F: FnMut(&ShardTransfer) -> bool,
    {
        self.shard_transfers
            .read()
            .iter()
            .filter(|&transfer| predicate(transfer))
            .cloned()
            .collect()
    }

    pub fn get_outgoing_transfers(&self, current_peer_id: PeerId) -> Vec<ShardTransfer> {
        self.get_transfers(|transfer| transfer.from == current_peer_id)
    }
}

/// Transfer registration must notify watchers even when replay leaves the record unchanged.
/// `register_restart_transfer` completes transfer restart by updating its record on disk.
/// These tests assert its replay-safety: the record is never removed, so replaying operation
/// after record is already updated should be a no-op instead of returning "no transfer" error.
#[cfg(test)]
mod transfer_registration_tests {
    use crate::config::ShardingMethod;
    use crate::shards::shard_holder::{ShardHolder, ShardTransferChange};
    use crate::shards::transfer::{ShardTransfer, ShardTransferMethod};

    #[test]
    fn register_transfer_replay_notifies() {
        let (_dir, holder) = make_holder();
        let transfer = wal_delta_transfer();
        let mut changes = holder.shard_transfer_changes.subscribe();

        for _ in 0..2 {
            holder
                .register_start_shard_transfer(transfer.clone())
                .unwrap();

            assert_eq!(
                changes.try_recv().unwrap(),
                ShardTransferChange::Start(transfer.clone()),
            );
            assert!(changes.is_empty());
        }
    }

    #[test]
    fn restart_transfer_replay_notifies() {
        let (_dir, holder) = make_holder();
        let transfer = wal_delta_transfer();
        holder
            .register_start_shard_transfer(transfer.clone())
            .unwrap();
        let mut changes = holder.shard_transfer_changes.subscribe();

        for _ in 0..2 {
            let updated = holder
                .register_restart_transfer(&transfer.key(), ShardTransferMethod::Snapshot)
                .unwrap()
                .unwrap();

            assert_eq!(
                changes.try_recv().unwrap(),
                ShardTransferChange::Abort(transfer.key()),
            );
            assert_eq!(
                changes.try_recv().unwrap(),
                ShardTransferChange::Start(updated),
            );
            assert!(changes.is_empty());
        }
    }

    #[test]
    fn finish_transfer_replay_notifies() {
        unregister_transfer_replay_notifies(true);
    }

    #[test]
    fn abort_transfer_replay_notifies() {
        unregister_transfer_replay_notifies(false);
    }

    fn unregister_transfer_replay_notifies(finish: bool) {
        let (_dir, holder) = make_holder();
        let transfer = wal_delta_transfer();
        let key = transfer.key();
        holder.register_start_shard_transfer(transfer).unwrap();
        let mut changes = holder.shard_transfer_changes.subscribe();
        let expected = if finish {
            ShardTransferChange::Finish(key)
        } else {
            ShardTransferChange::Abort(key)
        };

        for _ in 0..2 {
            if finish {
                holder.register_finish_transfer(&key).unwrap();
            } else {
                holder.register_abort_transfer(&key).unwrap();
            }

            assert_eq!(changes.try_recv().unwrap(), expected);
            assert!(changes.is_empty());
        }
    }

    fn make_holder() -> (tempfile::TempDir, ShardHolder) {
        let dir = tempfile::tempdir().unwrap();
        let holder = ShardHolder::new(dir.path(), ShardingMethod::Auto).unwrap();
        (dir, holder)
    }

    // A WAL-delta transfer, is the only transfer type that gets restarted.
    //
    // `sync: false` and `filter: Some(..)` are chosen so the assertions below
    // fail if restart hardcodes `sync: true` or keeps the old `filter`.
    //
    // `to_shard_id` must stay `None`: it is part of `ShardTransferKey`, so
    // restart resetting a `Some` value would change the record's identity.
    fn wal_delta_transfer() -> ShardTransfer {
        ShardTransfer {
            shard_id: 1,
            to_shard_id: None,
            from: 10,
            to: 20,
            sync: false,
            method: Some(ShardTransferMethod::WalDelta),
            filter: Some(segment::types::Filter::default()),
        }
    }

    #[test]
    fn test_register_restart_transfer_absent_is_none() {
        let (_dir, holder) = make_holder();
        let key = wal_delta_transfer().key();

        // No record: a restart of a transfer that isn't registered is a
        // stale-duplicate signal, reported as None.
        let result = holder
            .register_restart_transfer(&key, ShardTransferMethod::Snapshot)
            .unwrap();
        assert_eq!(
            result, None,
            "restart of an absent transfer must return None"
        );
    }

    #[test]
    fn test_register_restart_transfer_updates_method_in_place() {
        let (_dir, holder) = make_holder();
        let transfer = wal_delta_transfer();
        let key = transfer.key();
        holder
            .register_start_shard_transfer(transfer.clone())
            .unwrap();

        let updated = holder
            .register_restart_transfer(&key, ShardTransferMethod::Snapshot)
            .unwrap()
            .expect("restart must return the updated record");

        assert_eq!(updated.method, Some(ShardTransferMethod::Snapshot));
        assert_eq!(updated.sync, transfer.sync, "sync flag must be preserved");
        assert_eq!(updated.to_shard_id, None);
        assert_eq!(updated.filter, None, "filter must be reset");
        assert_eq!(updated.from, transfer.from);
        assert_eq!(updated.to, transfer.to);
        assert_eq!(updated.shard_id, transfer.shard_id);

        // The record is replaced in place — still exactly one transfer, now with
        // the new method (never removed, so a replay can always find it).
        let transfers = holder.shard_transfers.read();
        assert_eq!(transfers.len(), 1);
        assert_eq!(
            transfers.iter().next().unwrap().method,
            Some(ShardTransferMethod::Snapshot),
        );
    }

    #[test]
    fn test_register_restart_transfer_replay_is_noop() {
        let (dir, holder) = make_holder();
        let transfer = wal_delta_transfer();
        let key = transfer.key();
        holder
            .register_start_shard_transfer(transfer.clone())
            .unwrap();

        // Apply the restart, then replay it (as a crash after the method write
        // would). The replay must report the record (not None) and leave the set
        // unchanged — a single, idempotent no-op.
        holder
            .register_restart_transfer(&key, ShardTransferMethod::Snapshot)
            .unwrap();

        // Reopen the holder from disk, as a crash-restart would, so the replay
        // runs against the reloaded state rather than the same in-memory value.
        drop(holder);
        let holder = ShardHolder::new(dir.path(), ShardingMethod::Auto).unwrap();

        let replay = holder
            .register_restart_transfer(&key, ShardTransferMethod::Snapshot)
            .unwrap()
            .expect("replay must still find the record, not treat it as absent");

        assert_eq!(replay.method, Some(ShardTransferMethod::Snapshot));
        assert_eq!(replay.sync, transfer.sync);
        assert_eq!(holder.shard_transfers.read().len(), 1);
    }
}
