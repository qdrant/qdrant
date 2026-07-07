use std::num::NonZeroU32;

use futures::Future;
use tokio::sync::RwLockWriteGuard;

use super::Collection;
use crate::config::ShardingMethod;
use crate::hash_ring::HashRingRouter;
use crate::operations::cluster_ops::ReshardingDirection;
use crate::operations::types::CollectionResult;
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::resharding::{ReshardKey, ReshardState, ReshardingStage};
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::ReshardingCheck;
use crate::shards::transfer::{ShardTransferConsensus, ShardTransferKey};

/// Which parts of an abort's teardown the caller has reserved for itself and the
/// composition must therefore leave untouched (invariant 1: a conditional that
/// gates a later write must read a field this composition never touches).
#[derive(Copy, Clone, Debug, Default)]
pub struct AbortReshardingScope {
    /// The replica the caller (`SetShardReplicaState`) is about to overwrite with
    /// `Dead`. The down-direction revert-to-`Active` sweep must skip it, so it
    /// reaches the caller's final absolute write untouched and the caller's
    /// `is_resharding()` gate stays replay-stable.
    pub except_replica: Option<(ShardId, PeerId)>,
    /// The transfer record that gated the caller
    /// (`abort_shard_transfer_and_resharding`). Step 7's sweep must not remove it;
    /// the caller removes it *after* this composition clears the resharding state,
    /// so the gating record outlives the state clear and the caller's own replay
    /// stays convergent.
    pub except_transfer: Option<ShardTransferKey>,
}

impl Collection {
    pub async fn resharding_state(&self) -> Option<ReshardState> {
        self.shards_holder
            .read()
            .await
            .resharding_state
            .read()
            .clone()
    }

    /// Start a new resharding operation
    ///
    /// # Cancel safety
    ///
    /// This method is *not* cancel safe.
    pub async fn start_resharding<T, F>(
        &self,
        resharding_key: ReshardKey,
        _consensus: Box<dyn ShardTransferConsensus>,
        _on_finish: T,
        _on_error: F,
    ) -> CollectionResult<()>
    where
        T: Future<Output = ()> + Send + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        {
            let mut shard_holder = self.shards_holder.write().await;

            shard_holder.check_start_resharding(&resharding_key)?;

            // If scaling up, create a new replica set.
            //
            // Idempotent: only create the replica set if one for this shard id
            // does not exist yet. A replica set may already be present as a
            // leftover from a previous partial apply (e.g. crash between shard
            // directory creation and persisting resharding state) or from an
            // earlier successful apply we are now replaying. Recreating it
            // would wipe the existing shard contents.
            let replica_set = if resharding_key.direction == ReshardingDirection::Up
                && !shard_holder.contains_shard(resharding_key.shard_id)
            {
                let replica_set = self
                    .create_replica_set(
                        resharding_key.shard_id,
                        resharding_key.shard_key.clone(),
                        &[resharding_key.peer_id],
                        Some(ReplicaState::Resharding),
                    )
                    .await?;

                Some(replica_set)
            } else {
                None
            };

            shard_holder
                .start_resharding_unchecked(resharding_key.clone(), replica_set)
                .await?;

            if resharding_key.direction == ReshardingDirection::Up {
                let mut config = self.collection_config.write().await;
                match config.params.sharding_method.unwrap_or_default() {
                    // Idempotent: set the shard count to the new shard's id
                    // plus one (shard ids are contiguous from zero, so this is
                    // the count after adding the shard). A replay converges to
                    // the same value instead of incrementing again.
                    ShardingMethod::Auto => {
                        resharding_key
                            .debug_assert_targets_last_shard(config.params.shard_number.get());
                        let new_shard_number = resharding_key
                            .shard_id
                            .checked_add(1)
                            .and_then(NonZeroU32::new)
                            .expect("cannot have more than u32::MAX shards after resharding");
                        if config.params.shard_number != new_shard_number {
                            config.params.shard_number = new_shard_number;
                            config.save(&self.path)?;
                        }
                    }
                    // Custom shards don't use the persisted count, we don't change it
                    ShardingMethod::Custom => {}
                }
            }
        }

        // Drive resharding
        // self.drive_resharding(resharding_key, consensus, false, on_finish, on_error)
        //     .await?;

        Ok(())
    }

    pub async fn commit_read_hashring(&self, resharding_key: &ReshardKey) -> CollectionResult<()> {
        let mut shards_holder = self.shards_holder.write().await;

        // Idempotent: skip if no resharding or already past this stage
        match shards_holder.resharding_state() {
            None => {
                log::warn!("commit_read_hashring: no resharding in progress, skipping");
                return Ok(());
            }
            Some(state)
                if state.matches(resharding_key)
                    && state.stage >= ReshardingStage::ReadHashRingCommitted =>
            {
                log::warn!(
                    "commit_read_hashring: already at stage {:?}, skipping",
                    state.stage,
                );
                return Ok(());
            }
            _ => {}
        }

        shards_holder.commit_read_hashring(resharding_key)?;

        // Invalidate clean state for shards we copied points out of
        // These shards must be cleaned or dropped to ensure they don't contain irrelevant points
        match resharding_key.direction {
            // On resharding up: related shards below new shard key are affected
            ReshardingDirection::Up => match shards_holder.rings.get(&resharding_key.shard_key) {
                Some(HashRingRouter::Resharding { old, new: _ }) => {
                    self.invalidate_clean_local_shards(old.nodes().clone())
                        .await;
                }
                Some(HashRingRouter::Single(ring)) => {
                    debug_assert!(false, "must have resharding hash ring during resharding");
                    self.invalidate_clean_local_shards(ring.nodes().clone())
                        .await;
                }
                None => {
                    debug_assert!(false, "must have hash ring for resharding key");
                }
            },
            // On resharding down: shard we're about to remove is affected
            ReshardingDirection::Down => {
                self.invalidate_clean_local_shards([resharding_key.shard_id])
                    .await;
            }
        }

        Ok(())
    }

    pub async fn commit_write_hashring(&self, resharding_key: &ReshardKey) -> CollectionResult<()> {
        // Idempotent: skip if no resharding or already past this stage
        match self.shards_holder.read().await.resharding_state() {
            None => {
                log::warn!("commit_write_hashring: no resharding in progress, skipping");
                return Ok(());
            }
            Some(state)
                if state.matches(resharding_key)
                    && state.stage >= ReshardingStage::WriteHashRingCommitted =>
            {
                log::warn!(
                    "commit_write_hashring: already at stage {:?}, skipping",
                    state.stage,
                );
                return Ok(());
            }
            _ => {}
        }

        self.shards_holder
            .write()
            .await
            .commit_write_hashring(resharding_key)
    }

    pub async fn finish_resharding(&self, resharding_key: ReshardKey) -> CollectionResult<()> {
        let mut shard_holder = self.shards_holder.write().await;

        // Each step below is individually idempotent, so on replay (e.g. after
        // a crash that applied only part of the operation) we fall through
        // every step to reconcile any partially-applied state instead of
        // short-circuiting on the first condition that already holds.
        shard_holder.check_finish_resharding(&resharding_key)?;
        shard_holder.finish_resharding_unchecked(&resharding_key)?;

        if resharding_key.direction == ReshardingDirection::Down {
            // Remove the shard we've now migrated all points out of
            if let Some(shard_key) = &resharding_key.shard_key {
                shard_holder.remove_shard_from_key_mapping(resharding_key.shard_id, shard_key)?;
            }

            // Decrement and persist shard count *before* dropping shard directory, so crash
            // doesn't leave `shard_number` pointing at deleted dir, which would panic on startup
            {
                let mut config = self.collection_config.write().await;

                match config.params.sharding_method.unwrap_or_default() {
                    // Custom shards don't use persisted count, no need to change it
                    ShardingMethod::Custom => {}

                    // Set shard count to ID of shard being removed, instead of decrementing current
                    // shard count.
                    //
                    // Shard IDs are contiguous, from 0 to N.
                    // During scale-down resharding, we remove shard N.
                    // Once shard N is removed, there will be N shards left, from 0 to N-1.
                    //
                    // If operation is replayed after crash, we select same count every time,
                    // instead of decrementing it twice.
                    ShardingMethod::Auto => {
                        resharding_key
                            .debug_assert_targets_last_shard(config.params.shard_number.get());

                        let new_shard_number = NonZeroU32::new(resharding_key.shard_id)
                            .expect("cannot have zero shards after finishing resharding down");

                        if config.params.shard_number != new_shard_number {
                            config.params.shard_number = new_shard_number;
                            config.save(&self.path)?;
                        }
                    }
                }
            }

            shard_holder
                .drop_and_remove_shard(resharding_key.shard_id)
                .await?;
        }

        Ok(())
    }

    /// Abort a resharding operation, rebuilt as a flat composition of the
    /// `ShardHolder` resharding-abort primitives so the ordering that makes
    /// replay converge is visible in one place.
    ///
    /// Replay walk (invariants 1 & 3): every step below is individually
    /// idempotent and the persisted `resharding_state` is cleared **last**
    /// (step 8), so it gates the whole composition. Crash before (8): replay
    /// finds the state `Some(matching)` → the gate returns `Proceed` and every
    /// primitive re-runs, no-oping or rewriting the same value (transfers
    /// already aborted are simply absent from step 7's sweep). Crash after (8):
    /// replay finds `None` → the gate returns `NoOp` → pure no-op. A
    /// `Some(mismatch)` state is only reachable by a stale-duplicate abort of a
    /// key that a newer resharding replaced; the gate returns `NoOp` there too,
    /// so no destructive primitive (notably `drop_resharding_shard`) ever runs
    /// against the newer resharding's shard (finding I).
    ///
    /// `force` bypasses the stage gate (used when tearing a resharding down
    /// unconditionally, e.g. dropping the shard key or removing the peer).
    pub async fn abort_resharding(
        &self,
        resharding_key: ReshardKey,
        force: bool,
        scope: AbortReshardingScope,
    ) -> CollectionResult<()> {
        log::warn!(
            "Invalidating local cleanup tasks and aborting resharding {resharding_key} (force: {force})"
        );

        // Gate: on `None`/mismatch return early — NEVER fall through to the
        // destructive primitives below, which are not keyed by resharding uuid.
        if !force {
            match self
                .shards_holder
                .read()
                .await
                .check_abort_resharding(&resharding_key)?
            {
                ReshardingCheck::Proceed => {}
                ReshardingCheck::NoOp => return Ok(()),
            }
        } else {
            log::warn!("Force-aborting resharding {resharding_key}");
        }

        // 1. Invalidate clean state for shards affected by the (partial)
        //    resharding. These shards must be cleaned or dropped to ensure they
        //    don't contain irrelevant points. Mem-only.
        {
            let shard_holder = self.shards_holder.read().await;
            match resharding_key.direction {
                // Up: the new shard now has invalid points, and will likely be dropped.
                ReshardingDirection::Up => {
                    self.invalidate_clean_local_shards([resharding_key.shard_id])
                        .await;
                }
                // Down: existing shards may have new points moved into them. Use
                // the router's node set, which works regardless of whether the
                // ring has already been rolled back to `Single` on a replay.
                ReshardingDirection::Down => {
                    if let Some(ring) = shard_holder.rings.get(&resharding_key.shard_key) {
                        self.invalidate_clean_local_shards(ring.nodes().clone())
                            .await;
                    }
                }
            }
        }

        let mut shard_holder = self.shards_holder.write().await;

        // 2. Down: revert sibling replicas still in a `Resharding` state to
        //    `Active`, skipping the replica the caller is about to overwrite with
        //    `Dead` (`scope.except_replica`) — it must reach the caller's final
        //    write untouched (invariant 1). Absolute writes.
        for (shard_id, peer_id) in shard_holder.scale_down_replicas(&resharding_key) {
            if scope.except_replica == Some((shard_id, peer_id)) {
                continue;
            }
            if let Some(shard) = shard_holder.get_shard(shard_id) {
                shard
                    .set_replica_state(peer_id, ReplicaState::Active)
                    .await?;
            }
        }

        // 3. Down: delete points migrated into sibling shards that no longer belong.
        shard_holder
            .scale_down_cleanup_points(&resharding_key)
            .await?;

        // 4. Roll the hash ring back (mem-only).
        shard_holder.revert_resharding_hashring(&resharding_key);

        // 5. Up: persist the decremented shard count *before* dropping the shard
        //    directory (fix H). A crash between the two must never leave
        //    `shard_number` pointing at a shard dir we already deleted: the
        //    auto-sharding loader would then panic on the missing dir at startup.
        //    The reverse window (dir still present but count already decremented)
        //    is benign and reconciled by replay.
        //
        // Decrement and persist shard count *before* aborting resharding, so crash
        // doesn't leave `shard_number` pointing at deleted dir, which would panic on startup
        if resharding_key.direction == ReshardingDirection::Up {
            let mut config = self.collection_config.write().await;

            match config.params.sharding_method.unwrap_or_default() {
                // Custom shards don't use persisted count, no need to change it
                ShardingMethod::Custom => {}

                // Set shard count to ID of shard being aborted, instead of decrementing current
                // shard count.
                //
                // Shard IDs are contiguous, from 0 to N-1.
                // During scale-up resharding, we add shard N.
                // Once shard N is aborted, there will be N shards left, from 0 to N-1.
                //
                // If operation is replayed after crash, we select same count every time,
                // instead of decrementing it twice.
                ShardingMethod::Auto => {
                    resharding_key
                        .debug_assert_targets_last_shard(config.params.shard_number.get());

                    let new_shard_number = NonZeroU32::new(resharding_key.shard_id)
                        .expect("cannot have zero shards after aborting resharding up");

                    if config.params.shard_number != new_shard_number {
                        config.params.shard_number = new_shard_number;
                        config.save(&self.path)?;
                    }
                }
            }
        }

        // 6. Up: drop the shard resharding created.
        shard_holder
            .scale_up_drop_target_shard(&resharding_key)
            .await?;

        let shard_holder = RwLockWriteGuard::downgrade(shard_holder);

        // 7. Abort all transfers related to this specific resharding operation,
        //    except the caller's gating transfer (`scope.except_transfer`), which
        //    the caller removes after this composition (so the gating record
        //    outlives the state clear at step 8).
        let resharding_transfers = shard_holder.get_transfers(|t| {
            t.is_related_to_resharding(&resharding_key) && Some(t.key()) != scope.except_transfer
        });
        for transfer in resharding_transfers {
            self.abort_shard_transfer(transfer, &shard_holder).await?;
        }

        // 8. Clear the persisted resharding state — LAST, the gate of this
        //    composition (see the replay walk above).
        shard_holder.clear_resharding_state(&resharding_key)?;

        drop(shard_holder);

        Ok(())
    }
}
