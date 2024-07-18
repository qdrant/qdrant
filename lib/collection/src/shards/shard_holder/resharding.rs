use std::collections::HashMap;
use std::fmt;
use std::ops::Deref as _;
use std::sync::Arc;

use segment::types::{Condition, Filter, ShardKey};

use super::ShardHolder;
use crate::hash_ring::{self, HashRing};
use crate::operations::cluster_ops::ReshardingDirection;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::replica_set::{ReplicaState, ShardReplicaSet};
use crate::shards::resharding::{ReshardKey, ReshardStage, ReshardState};

impl ShardHolder {
    pub fn check_start_resharding(&mut self, resharding_key: &ReshardKey) -> CollectionResult<()> {
        let ReshardKey {
            shard_id,
            shard_key,
            ..
        } = resharding_key;

        let ring = get_ring(&mut self.rings, shard_key)?;

        {
            let state = self.resharding_state.read();
            assert_resharding_state_consistency(&state, ring, shard_key);

            if let Some(state) = state.deref() {
                if state.matches(resharding_key) {
                    return Err(CollectionError::bad_request(format!(
                        "resharding {resharding_key} is already in progress:\n{state:#?}"
                    )));
                } else {
                    return Err(CollectionError::bad_request(format!(
                        "another resharding is in progress:\n{state:#?}"
                    )));
                }
            }
        }

        let has_shard = self.shards.contains_key(shard_id);
        match resharding_key.direction {
            ReshardingDirection::Up => {
                if has_shard {
                    return Err(CollectionError::bad_request(format!(
                        "shard holder already contains shard {shard_id} replica set",
                    )));
                }
            }
            ReshardingDirection::Down => {
                if !has_shard {
                    return Err(CollectionError::bad_request(format!(
                        "shard holder does not contain shard {shard_id} replica set",
                    )));
                }
            }
        }

        // TODO(resharding): Check that peer exists!?

        Ok(())
    }

    // TODO: do not leave broken intermediate state if this fails midway?
    pub fn start_resharding_unchecked(
        &mut self,
        resharding_key: ReshardKey,
        new_shard: Option<ShardReplicaSet>,
    ) -> CollectionResult<()> {
        let ReshardKey {
            peer_id,
            shard_id,
            shard_key,
            direction,
        } = resharding_key;

        // TODO(resharding): Delete shard on error!?

        let ring = get_ring(&mut self.rings, &shard_key)?;
        ring.start_resharding(shard_id, direction);

        // Add new shard if resharding up
        if let Some(new_shard) = new_shard {
            debug_assert_eq!(direction, ReshardingDirection::Up);
            self.add_shard(shard_id, new_shard, shard_key.clone())?;
        }

        self.resharding_state.write(|state| {
            debug_assert!(
                state.is_none(),
                "resharding is already in progress:\n{state:#?}",
            );

            *state = Some(ReshardState::new(peer_id, shard_id, shard_key, direction));
        })?;

        Ok(())
    }

    pub fn commit_read_hashring(&mut self, resharding_key: ReshardKey) -> CollectionResult<()> {
        self.check_resharding(&resharding_key, check_stage(ReshardStage::MigratingPoints))?;

        self.resharding_state.write(|state| {
            let Some(state) = state else {
                unreachable!();
            };

            state.stage = ReshardStage::ReadHashRingCommitted;
        })?;

        Ok(())
    }

    pub fn commit_write_hashring(&mut self, resharding_key: ReshardKey) -> CollectionResult<()> {
        self.check_resharding(
            &resharding_key,
            check_stage(ReshardStage::ReadHashRingCommitted),
        )?;

        let ring = get_ring(&mut self.rings, &resharding_key.shard_key)?;
        ring.commit_resharding();

        self.resharding_state.write(|state| {
            let Some(state) = state else {
                unreachable!();
            };

            state.stage = ReshardStage::WriteHashRingCommitted;
        })?;

        Ok(())
    }

    pub fn check_finish_resharding(&mut self, resharding_key: &ReshardKey) -> CollectionResult<()> {
        self.check_resharding(
            resharding_key,
            check_stage(ReshardStage::WriteHashRingCommitted),
        )?;

        Ok(())
    }

    pub fn finish_resharding_unchecked(&mut self, _: &ReshardKey) -> CollectionResult<()> {
        self.resharding_state.write(|state| {
            debug_assert!(state.is_some(), "resharding is not in progress");
            *state = None;
        })?;

        Ok(())
    }

    fn check_resharding(
        &mut self,
        resharding_key: &ReshardKey,
        check_state: impl Fn(&ReshardState) -> CollectionResult<()>,
    ) -> CollectionResult<()> {
        let ReshardKey {
            shard_id,
            shard_key,
            ..
        } = resharding_key;

        let ring = get_ring(&mut self.rings, shard_key)?;

        let state = self.resharding_state.read();
        assert_resharding_state_consistency(&state, ring, &resharding_key.shard_key);

        match state.deref() {
            Some(state) if state.matches(resharding_key) => {
                check_state(state)?;
            }

            Some(state) => {
                return Err(CollectionError::bad_request(format!(
                    "another resharding is in progress:\n{state:#?}"
                )));
            }

            None => {
                return Err(CollectionError::bad_request(
                    "resharding is not in progress",
                ));
            }
        }

        debug_assert!(
            self.shards.contains_key(shard_id),
            "shard holder does not contain shard {shard_id} replica set"
        );

        // TODO(resharding): Assert that peer exists!?

        Ok(())
    }

    pub async fn abort_resharding(&mut self, resharding_key: ReshardKey) -> CollectionResult<()> {
        let ReshardKey {
            peer_id,
            shard_id,
            direction,
            ref shard_key,
        } = resharding_key;

        let is_in_progress = match self.resharding_state.read().deref() {
            Some(state) if state.matches(&resharding_key) => true,

            Some(state) => {
                log::warn!(
                    "aborting resharding {resharding_key}, \
                     but another resharding is in progress:\n\
                     {state:#?}"
                );

                false
            }

            None => {
                log::warn!(
                    "aborting resharding {resharding_key}, \
                     but resharding is not in progress"
                );

                false
            }
        };

        if let Some(ring) = self.rings.get_mut(shard_key) {
            log::debug!("ending resharding hash ring for shard {shard_id}");
            ring.end_resharding(shard_id, direction);
        } else {
            log::warn!(
                "aborting resharding {resharding_key}, \
                 but {shard_key:?} hashring does not exist"
            );
        }

        // Remove new shard if resharding up
        if direction == ReshardingDirection::Up {
            if let Some(shard) = self.get_shard(&shard_id) {
                match shard.peer_state(&peer_id) {
                    Some(ReplicaState::Resharding) => {
                        log::debug!("removing peer {peer_id} from {shard_id} replica set");
                        shard.remove_peer(peer_id).await?;
                    }

                    Some(ReplicaState::Dead) if is_in_progress => {
                        log::debug!("removing dead peer {peer_id} from {shard_id} replica set");
                        shard.remove_peer(peer_id).await?;
                    }

                    Some(state) => {
                        return Err(CollectionError::bad_request(format!(
                            "peer {peer_id} is in {state:?} state"
                        )));
                    }

                    None => {
                        log::warn!(
                            "aborting resharding {resharding_key}, \
                         but peer {peer_id} does not exist in {shard_id} replica set"
                        );
                    }
                }

                if shard.peers().is_empty() {
                    log::debug!("removing {shard_id} replica set, because replica set is empty");
                    self.drop_and_remove_shard(shard_id).await?;
                }
            } else {
                log::warn!(
                    "aborting resharding {resharding_key}, \
                 but shard holder does not contain {shard_id} replica set",
                );
            }
        }

        if is_in_progress {
            self.resharding_state.write(|state| {
                debug_assert!(
                    state
                        .as_ref()
                        .map_or(false, |state| state.matches(&resharding_key)),
                    "resharding {resharding_key} is not in progress:\n{state:#?}"
                );

                state.take();
            })?;
        }

        Ok(())
    }

    /// A filter that excludes points migrated to a different shard, as part of resharding.
    ///
    /// `None` if resharding is not active or if the read hash ring is not committed yet.
    pub fn resharding_filter(&self) -> Option<Filter> {
        let filter = self.resharding_filter_impl()?;
        let filter = Filter::new_must_not(Condition::Resharding(Arc::new(filter)));
        Some(filter)
    }

    #[inline]
    pub fn resharding_filter_impl(&self) -> Option<hash_ring::Filter> {
        let state = self.resharding_state.read();

        let Some(state) = state.deref() else {
            return None;
        };

        if state.stage < ReshardStage::ReadHashRingCommitted {
            return None;
        }

        let Some(ring) = self.rings.get(&state.shard_key) else {
            return None; // TODO(resharding): Return error?
        };

        let ring = match ring {
            HashRing::Resharding { new, .. } => new,
            HashRing::Single(ring) => ring,
        };

        Some(hash_ring::Filter::new(ring.clone(), state.shard_id))
    }
}

fn get_ring<'a>(
    rings: &'a mut HashMap<Option<ShardKey>, HashRing>,
    shard_key: &'_ Option<ShardKey>,
) -> CollectionResult<&'a mut HashRing> {
    rings.get_mut(shard_key).ok_or_else(|| {
        CollectionError::bad_request(format!(
            "{} hashring does not exist",
            shard_key_fmt(shard_key)
        ))
    })
}

fn assert_resharding_state_consistency(
    state: &Option<ReshardState>,
    ring: &HashRing,
    shard_key: &Option<ShardKey>,
) {
    match state.as_ref().map(|state| state.stage) {
        Some(ReshardStage::MigratingPoints | ReshardStage::ReadHashRingCommitted) => {
            debug_assert!(
                ring.is_resharding(),
                "resharding is in progress, \
                 but {shard_key:?} hashring is not a resharding hashring:\n\
                 {state:#?}"
            );
        }

        Some(ReshardStage::WriteHashRingCommitted) => {
            debug_assert!(
                !ring.is_resharding(),
                "resharding is in progress, \
                 and write hashring has already been committed, \
                 but {shard_key:?} hashring is a resharding hashring:\n\
                 {state:#?}"
            );
        }

        None => {
            debug_assert!(
                !ring.is_resharding(),
                "resharding is not in progress, \
                 but {shard_key:?} hashring is a resharding hashring"
            );
        }
    }
}

fn check_stage(stage: ReshardStage) -> impl Fn(&ReshardState) -> CollectionResult<()> {
    move |state| {
        if state.stage == stage {
            Ok(())
        } else {
            Err(CollectionError::bad_request(format!(
                "expected resharding stage {stage:?}, but resharding is at stage {:?}",
                state.stage,
            )))
        }
    }
}

fn shard_key_fmt(key: &Option<ShardKey>) -> &dyn fmt::Display {
    match key {
        Some(key) => key,
        None => &"default",
    }
}
