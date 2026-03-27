use std::num::NonZeroU32;

use futures::Future;

use super::Collection;
use crate::config::ShardingMethod;
use crate::hash_ring::HashRingRouter;
use crate::operations::cluster_ops::ReshardingDirection;
use crate::operations::types::CollectionResult;
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::resharding::{ReshardKey, ReshardState, ReshardingStage};
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

            // Idempotent: if the same resharding is already in progress, this is a
            // re-application of an already-applied consensus entry (e.g. after crash
            // recovery). Skip without error so we don't silently diverge from peers
            // that applied it successfully on the first attempt.
            if let Some(state) = shard_holder.resharding_state() {
                if state.matches(&resharding_key) {
                    log::warn!(
                        "Resharding {resharding_key} is already in progress, \
                         skipping start (idempotent re-apply)"
                    );
                    return Ok(());
                }
            }

            shard_holder.check_start_resharding(&resharding_key)?;

            // If scaling up, create a new replica set
            let replica_set = if resharding_key.direction == ReshardingDirection::Up {
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
                    // If adding a shard, increase persisted count so we load it on restart
                    ShardingMethod::Auto => {
                        debug_assert_eq!(config.params.shard_number.get(), resharding_key.shard_id);

                        config.params.shard_number = config
                            .params
                            .shard_number
                            .checked_add(1)
                            .expect("cannot have more than u32::MAX shards after resharding");
                        if let Err(err) = config.save(&self.path) {
                            log::error!(
                                "Failed to update and save collection config during resharding: {err}",
                            );
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
                log::warn!(
                    "commit_read_hashring: no resharding in progress, \
                     skipping (idempotent)"
                );
                return Ok(());
            }
            Some(state)
                if state.matches(resharding_key)
                    && state.stage >= ReshardingStage::ReadHashRingCommitted =>
            {
                log::warn!(
                    "commit_read_hashring: already at stage {:?}, \
                     skipping (idempotent)",
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
        let shards_holder = self.shards_holder.read().await;

        // Idempotent: skip if no resharding or already past this stage
        match shards_holder.resharding_state() {
            None => {
                log::warn!(
                    "commit_write_hashring: no resharding in progress, \
                     skipping (idempotent)"
                );
                return Ok(());
            }
            Some(state)
                if state.matches(resharding_key)
                    && state.stage >= ReshardingStage::WriteHashRingCommitted =>
            {
                log::warn!(
                    "commit_write_hashring: already at stage {:?}, \
                     skipping (idempotent)",
                    state.stage,
                );
                return Ok(());
            }
            _ => {}
        }

        drop(shards_holder);
        self.shards_holder
            .write()
            .await
            .commit_write_hashring(resharding_key)
    }

    pub async fn finish_resharding(&self, resharding_key: ReshardKey) -> CollectionResult<()> {
        let mut shard_holder = self.shards_holder.write().await;

        // Idempotent: if no resharding is in progress, finish was already applied
        if shard_holder.resharding_state().is_none() {
            log::warn!(
                "finish_resharding: no resharding in progress, \
                 skipping (idempotent)"
            );
            return Ok(());
        }

        shard_holder.check_finish_resharding(&resharding_key)?;
        shard_holder.finish_resharding_unchecked(&resharding_key)?;

        if resharding_key.direction == ReshardingDirection::Down {
            // Remove the shard we've now migrated all points out of
            if let Some(shard_key) = &resharding_key.shard_key {
                shard_holder.remove_shard_from_key_mapping(resharding_key.shard_id, shard_key)?;
            }

            shard_holder
                .drop_and_remove_shard(resharding_key.shard_id)
                .await?;

            {
                let mut config = self.collection_config.write().await;
                match config.params.sharding_method.unwrap_or_default() {
                    // If removing a shard, decrease persisted count so we don't load it on restart
                    ShardingMethod::Auto => {
                        debug_assert_eq!(
                            config.params.shard_number.get() - 1,
                            resharding_key.shard_id,
                        );

                        config.params.shard_number =
                            NonZeroU32::new(config.params.shard_number.get() - 1)
                                .expect("cannot have zero shards after finishing resharding");

                        if let Err(err) = config.save(&self.path) {
                            log::error!(
                                "Failed to update and save collection config during resharding: {err}"
                            );
                        }
                    }
                    // Custom shards don't use the persisted count, we don't change it
                    ShardingMethod::Custom => {}
                }
            }
        }

        Ok(())
    }

    pub async fn abort_resharding(
        &self,
        resharding_key: ReshardKey,
        force: bool,
    ) -> CollectionResult<()> {
        log::warn!(
            "Invalidating local cleanup tasks and aborting resharding {resharding_key} (force: {force})"
        );

        let shard_holder = self.shards_holder.read().await;

        // Idempotent: if no resharding is in progress, abort was already applied
        if shard_holder.resharding_state().is_none() {
            log::warn!(
                "abort_resharding: no resharding in progress, \
                 skipping (idempotent)"
            );
            return Ok(());
        }

        if !force {
            shard_holder.check_abort_resharding(&resharding_key)?;
        } else {
            log::warn!("Force-aborting resharding {resharding_key}");
        }

        // Invalidate clean state for shards we copied new points into
        // These shards must be cleaned or dropped to ensure they don't contain irrelevant points
        match resharding_key.direction {
            // On resharding up: new shard now has invalid points, shard will likely be dropped
            ReshardingDirection::Up => {
                self.invalidate_clean_local_shards([resharding_key.shard_id])
                    .await;
            }
            // On resharding down: existing shards may have new points moved into them
            ReshardingDirection::Down => match shard_holder.rings.get(&resharding_key.shard_key) {
                Some(HashRingRouter::Resharding { old: _, new }) => {
                    self.invalidate_clean_local_shards(new.nodes().clone())
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
        }

        // Abort all resharding transfer related to this specific resharding operation
        let resharding_transfers =
            shard_holder.get_transfers(|t| t.is_related_to_resharding(&resharding_key));
        for transfer in resharding_transfers {
            self.abort_shard_transfer(transfer, &shard_holder).await?;
        }

        drop(shard_holder); // drop the read lock before acquiring write lock
        let mut shard_holder = self.shards_holder.write().await;

        shard_holder
            .abort_resharding(resharding_key.clone(), force)
            .await?;

        // Decrease the persisted shard count, ensures we don't load dropped shard on restart
        if resharding_key.direction == ReshardingDirection::Up {
            let mut config = self.collection_config.write().await;
            match config.params.sharding_method.unwrap_or_default() {
                // If removing a shard, decrease persisted count so we don't load it on restart
                ShardingMethod::Auto => {
                    debug_assert_eq!(
                        config.params.shard_number.get() - 1,
                        resharding_key.shard_id,
                    );

                    config.params.shard_number =
                        NonZeroU32::new(config.params.shard_number.get() - 1)
                            .expect("cannot have zero shards after aborting resharding");

                    if let Err(err) = config.save(&self.path) {
                        log::error!(
                            "Failed to update and save collection config during resharding: {err}"
                        );
                    }
                }
                // Custom shards don't use the persisted count, we don't change it
                ShardingMethod::Custom => {}
            }
        }

        Ok(())
    }
}
