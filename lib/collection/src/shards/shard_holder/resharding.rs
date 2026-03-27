use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::Deref as _;
use std::sync::Arc;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::types::DeferredBehavior;
use segment::types::{Condition, CustomIdCheckerCondition as _, Filter, ShardKey};
use shard::operations::point_ops::UpdateMode;

use super::ShardHolder;
use crate::config::ShardingMethod;
use crate::hash_ring::{self, HashRingRouter};
use crate::operations::CollectionUpdateOperations;
use crate::operations::cluster_ops::ReshardingDirection;
use crate::operations::point_ops::{ConditionalInsertOperationInternal, PointOperations};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::replica_set::ShardReplicaSet;
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::resharding::{ReshardKey, ReshardState, ReshardingStage};
use crate::shards::shard::ShardId;

impl ShardHolder {
    pub fn resharding_state(&self) -> Option<ReshardState> {
        self.resharding_state.read().clone()
    }

    pub fn check_start_resharding(&mut self, resharding_key: &ReshardKey) -> CollectionResult<()> {
        let ReshardKey {
            uuid: _,
            direction,
            peer_id: _,
            shard_id,
            shard_key,
        } = resharding_key;

        // Additional shard key check
        // For auto sharding no shard key must be provided, with custom sharding it must be provided
        match self.sharding_method {
            ShardingMethod::Auto => {
                if shard_key.is_some() {
                    return Err(CollectionError::bad_request(format!(
                        "cannot specify shard key {} on collection with auto sharding",
                        shard_key_fmt(shard_key),
                    )));
                }
            }
            ShardingMethod::Custom => {
                if shard_key.is_none() {
                    return Err(CollectionError::bad_request(
                        "must specify shard key on collection with custom sharding",
                    ));
                }
            }
        }

        let ring = get_ring(&mut self.rings, shard_key)?;

        {
            let state = self.resharding_state.read();
            assert_resharding_state_consistency(&state, ring, shard_key);

            if let Some(state) = state.deref() {
                return if state.matches(resharding_key) {
                    Err(CollectionError::bad_request(format!(
                        "resharding {resharding_key} is already in progress:\n{state:#?}"
                    )))
                } else {
                    Err(CollectionError::bad_request(format!(
                        "another resharding is in progress:\n{state:#?}"
                    )))
                };
            }
        }

        // Don't remove the last shard if resharding down
        if matches!(direction, ReshardingDirection::Down) {
            let shard_count = match shard_key {
                Some(shard_key) => self
                    .get_shard_key_to_ids_mapping()
                    .get(shard_key)
                    .map_or(0, |shards| shards.len()),
                None => self.shards.len(),
            };
            if shard_count <= 1 {
                return Err(CollectionError::bad_request(format!(
                    "cannot remove shard {shard_id} by resharding down, it is the last shard",
                )));
            }
        }

        let has_shard = self.shards.contains_key(shard_id);
        match resharding_key.direction {
            ReshardingDirection::Up => {
                if has_shard {
                    // Allow re-application: the shard may exist as a leftover from a
                    // previous incomplete start attempt (e.g. crash after key_mapping
                    // was persisted but before resharding_state was written).
                    // create_shard_dir will clean up the stale directory and add_shard
                    // will evict the old entry.
                    log::warn!(
                        "Shard {shard_id} already exists during resharding start, \
                         likely a leftover from a previous incomplete attempt, \
                         it will be recreated"
                    );
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
    /// ## Cancel safety
    ///
    /// This function is **not** cancel safe.
    pub async fn start_resharding_unchecked(
        &mut self,
        resharding_key: ReshardKey,
        new_shard: Option<ShardReplicaSet>,
    ) -> CollectionResult<()> {
        let ReshardKey {
            uuid,
            direction,
            peer_id,
            shard_id,
            shard_key,
        } = resharding_key;

        // TODO(resharding): Delete shard on error!?

        let ring_res = get_ring(&mut self.rings, &shard_key);
        let ring = match ring_res {
            Ok(ring) => ring,
            Err(err) => {
                if let Some(new_shard) = new_shard {
                    new_shard.stop_gracefully().await;
                }
                return Err(err);
            }
        };
        ring.start_resharding(shard_id, direction);

        // Add new shard if resharding up
        if let Some(new_shard) = new_shard {
            debug_assert_eq!(direction, ReshardingDirection::Up);
            self.add_shard(shard_id, new_shard, shard_key.clone())
                .await?;
        }

        self.resharding_state.write(|state| {
            debug_assert!(
                state.is_none(),
                "resharding is already in progress:\n{state:#?}",
            );

            *state = Some(ReshardState::new(
                uuid, direction, peer_id, shard_id, shard_key,
            ));
        })?;

        Ok(())
    }

    pub fn commit_read_hashring(&mut self, resharding_key: &ReshardKey) -> CollectionResult<()> {
        // Idempotent: if no resharding is active, or the stage is already at/past
        // ReadHashRingCommitted, this entry was already applied. Return Ok to
        // prevent silent state divergence via apply_entries error swallowing.
        match self.resharding_state.read().deref() {
            None => {
                log::warn!(
                    "commit_read_hashring: no resharding in progress for {resharding_key}, \
                     treating as already committed (idempotent)"
                );
                return Ok(());
            }
            Some(state)
                if state.matches(resharding_key)
                    && state.stage >= ReshardingStage::ReadHashRingCommitted =>
            {
                log::warn!(
                    "commit_read_hashring: read hashring already committed for \
                     {resharding_key}, skipping (idempotent)"
                );
                return Ok(());
            }
            _ => {}
        }

        self.check_resharding(
            resharding_key,
            check_stage(ReshardingStage::MigratingPoints),
        )?;

        self.resharding_state.write(|state| {
            let Some(state) = state else {
                unreachable!();
            };

            state.stage = ReshardingStage::ReadHashRingCommitted;
        })?;

        Ok(())
    }

    pub fn commit_write_hashring(&mut self, resharding_key: &ReshardKey) -> CollectionResult<()> {
        // Idempotent: if no resharding is active, or the stage is already at/past
        // WriteHashRingCommitted, this entry was already applied.
        match self.resharding_state.read().deref() {
            None => {
                log::warn!(
                    "commit_write_hashring: no resharding in progress for {resharding_key}, \
                     treating as already committed (idempotent)"
                );
                return Ok(());
            }
            Some(state)
                if state.matches(resharding_key)
                    && state.stage >= ReshardingStage::WriteHashRingCommitted =>
            {
                log::warn!(
                    "commit_write_hashring: write hashring already committed for \
                     {resharding_key}, skipping (idempotent)"
                );
                return Ok(());
            }
            _ => {}
        }

        self.check_resharding(
            resharding_key,
            check_stage(ReshardingStage::ReadHashRingCommitted),
        )?;

        let ring = get_ring(&mut self.rings, &resharding_key.shard_key)?;
        ring.commit_resharding();

        self.resharding_state.write(|state| {
            let Some(state) = state else {
                unreachable!();
            };

            state.stage = ReshardingStage::WriteHashRingCommitted;
        })?;

        Ok(())
    }

    pub fn check_finish_resharding(&mut self, resharding_key: &ReshardKey) -> CollectionResult<()> {
        // Idempotent: if no resharding is active, finish was already applied.
        if self.resharding_state.read().is_none() {
            log::warn!(
                "check_finish_resharding: no resharding in progress for {resharding_key}, \
                 treating as already finished (idempotent)"
            );
            return Ok(());
        }

        self.check_resharding(
            resharding_key,
            check_stage(ReshardingStage::WriteHashRingCommitted),
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

    pub fn check_abort_resharding(&self, resharding_key: &ReshardKey) -> CollectionResult<()> {
        let state = self.resharding_state.read();

        let Some(state) = state.deref() else {
            // Idempotent: no resharding in progress means the abort (or finish)
            // was already applied, or the start was never applied on this node.
            // Returning an error here would be silently swallowed by apply_entries,
            // causing permanent state divergence between peers.
            log::warn!(
                "check_abort_resharding: no resharding in progress for {resharding_key}, \
                 treating as already aborted (idempotent)"
            );
            return Ok(());
        };

        if !state.matches(resharding_key) {
            // Idempotent: a different resharding is active, so the one we're
            // trying to abort was already handled. Same reasoning as above.
            log::warn!(
                "check_abort_resharding: resharding {resharding_key} not found, \
                 current resharding has key {}, treating as already aborted (idempotent)",
                state.key(),
            );
            return Ok(());
        }

        if state.stage < ReshardingStage::ReadHashRingCommitted {
            return Ok(());
        }

        Err(CollectionError::bad_request(format!(
            "can't abort resharding {resharding_key}, \
             because read hash ring has been committed already, \
             resharding must be completed",
        )))
    }

    pub async fn abort_resharding(
        &mut self,
        resharding_key: ReshardKey,
        force: bool,
    ) -> CollectionResult<()> {
        log::warn!("Aborting resharding {resharding_key} (force: {force})");

        let ReshardKey {
            uuid: _,
            direction,
            peer_id: _,
            shard_id,
            ref shard_key,
        } = resharding_key;

        // Cleanup existing shards if resharding down
        if direction == ReshardingDirection::Down {
            for (&id, shard) in self.shards.iter() {
                // Skip shards that does not belong to resharding shard key
                if self.shard_id_to_key_mapping.get(&id) != shard_key.as_ref() {
                    continue;
                }

                // Skip target shard
                if id == shard_id {
                    continue;
                }

                // Revert replicas in `Resharding` state back into `Active` state
                for (peer, state) in shard.peers() {
                    if state.is_resharding() {
                        shard.set_replica_state(peer, ReplicaState::Active).await?;
                    }
                }

                // We only cleanup local shards
                if !shard.is_local().await {
                    continue;
                }

                // Remove any points that might have been transferred from target shard
                // Replica may be dead, so we force the delete operation
                let filter = self.hash_ring_filter(id).expect("hash ring filter");
                let filter = Filter::new_must_not(Condition::new_custom(Arc::new(filter)));
                shard
                    .delete_local_points(
                        filter,
                        // Internal operation, no performance tracking needed
                        HwMeasurementAcc::disposable(),
                        true,
                        DeferredBehavior::IncludeAll,
                    )
                    .await?;
            }
        }

        if let Some(ring) = self.rings.get_mut(shard_key) {
            log::debug!("reverting resharding hashring for shard {shard_id}");
            ring.abort_resharding(shard_id, direction);
        } else {
            log::warn!(
                "aborting resharding {resharding_key}, \
                 but {shard_key:?} hashring does not exist"
            );
        }

        // Remove new shard if resharding up
        if direction == ReshardingDirection::Up {
            if let Some(shard) = self.get_shard(shard_id) {
                // Remove all replicas from shard
                for (peer_id, replica_state) in shard.peers() {
                    log::debug!(
                        "removing peer {peer_id} with state {replica_state:?} from replica set {shard_id}",
                    );
                    shard.remove_peer(peer_id).await?;
                }

                debug_assert!(
                    shard.peers().is_empty(),
                    "replica set {shard_id} must be empty after removing all peers",
                );

                log::debug!("removing replica set {shard_id}");

                // Drop the shard
                if let Some(shard_key) = shard_key {
                    self.key_mapping.write_optional(|key_mapping| {
                        if !key_mapping.contains_key(shard_key) {
                            return None;
                        }

                        let mut key_mapping = key_mapping.clone();
                        key_mapping.get_mut(shard_key).unwrap().remove(&shard_id);

                        Some(key_mapping)
                    })?;
                }

                self.drop_and_remove_shard(shard_id).await?;
                self.shard_id_to_key_mapping.remove(&shard_id);
            } else {
                log::warn!(
                    "aborting resharding {resharding_key}, \
                     but shard holder does not contain {shard_id} replica set",
                );
            }
        }

        self.resharding_state.write(|state| {
            debug_assert!(
                state
                    .as_ref()
                    .is_some_and(|state| state.matches(&resharding_key)),
                "resharding {resharding_key} is not in progress:\n{state:#?}"
            );

            state.take();
        })?;

        Ok(())
    }

    /// Split collection update operation by "update mode":
    /// - update all:
    ///   - "regular" operation
    ///   - `upsert` inserts new points and updates existing ones
    ///   - other update operations return error, if a point does not exist in collection
    /// - update existing:
    ///   - `upsert` does *not* insert new points, only updates existing ones
    ///   - other update operations ignore points that do not exist in collection
    ///
    /// Depends on the current resharding state. If resharding is not active operations are not split.
    pub fn split_by_mode(
        &self,
        shard_id: ShardId,
        operation: CollectionUpdateOperations,
    ) -> OperationsByMode {
        let Some(state) = self.resharding_state() else {
            return OperationsByMode::from(operation);
        };

        // Resharding *UP*
        // ┌────────────┐   ┌──────────┐
        // │            │   │          │
        // │ Shard 1    │   │ Shard 2  │
        // │ Non-Target ├──►│ Target   │
        // │ Sender     │   │ Receiver │
        // │            │   │          │
        // └────────────┘   └──────────┘
        //
        // Resharding *DOWN*
        // ┌────────────┐   ┌──────────┐
        // │            │   │          │
        // │ Shard 1    │   │ Shard 2  │
        // │ Non-Target │◄──┤ Target   │
        // │ Receiver   │   │ Sender   │
        // │            │   │          │
        // └────────────┘   └──────────┘

        // Target shard of the resharding operation. This is the shard that:
        //
        // - *created* during resharding *up*
        // - *deleted* during resharding *down*
        let is_target_shard = shard_id == state.shard_id;

        // Shard that will be *receiving* migrated points during resharding:
        //
        // - *target* shard during resharding *up*
        // - *non* target shards during resharding *down*
        let is_receiver_shard = match state.direction {
            ReshardingDirection::Up => is_target_shard,
            ReshardingDirection::Down => !is_target_shard,
        };

        // Shard that will be *sending* migrated points during resharding:
        //
        // - *non* target shards during resharding *up*
        // - *target* shard during resharding *down*
        let is_sender_shard = !is_receiver_shard;

        // We split update operations:
        //
        // - on *receiver* shards during `MigratingPoints` stage (for all operations except `upsert`)
        // - and on *sender* shards during `ReadHashRingCommitted` stage when resharding *up*

        let should_split_receiver = is_receiver_shard
            && state.stage == ReshardingStage::MigratingPoints
            && !operation.is_upsert_points();

        let should_split_sender = is_sender_shard
            && state.stage >= ReshardingStage::ReadHashRingCommitted
            && state.direction == ReshardingDirection::Up;

        if !should_split_receiver && !should_split_sender {
            return OperationsByMode::from(operation);
        }

        // There's no point splitting delete operations
        if operation.is_delete_points() {
            return OperationsByMode::from(operation);
        }

        let Some(filter) = self.resharding_filter() else {
            return OperationsByMode::from(operation);
        };

        let point_ids = match operation.point_ids() {
            Some(ids) if !ids.is_empty() => ids,
            Some(_) | None => return OperationsByMode::from(operation),
        };

        let target_point_ids: HashSet<_> = point_ids
            .iter()
            .copied()
            .filter(|&point_id| filter.check(point_id))
            .collect();

        if target_point_ids.is_empty() {
            OperationsByMode::from(operation)
        } else if target_point_ids.len() == point_ids.len() {
            OperationsByMode::default().with_update_only_existing(operation)
        } else {
            let mut update_all = operation.clone();
            update_all.retain_point_ids(|point_id| !target_point_ids.contains(point_id));

            let mut update_only_existing = operation;
            update_only_existing.retain_point_ids(|point_id| target_point_ids.contains(point_id));

            OperationsByMode::from(update_all).with_update_only_existing(update_only_existing)
        }
    }

    pub fn resharding_filter(&self) -> Option<hash_ring::HashRingFilter> {
        let shard_id = self.resharding_state.read().as_ref()?.shard_id;
        self.hash_ring_filter(shard_id)
    }

    pub fn hash_ring_router(&self, shard_id: ShardId) -> Option<&HashRingRouter> {
        if !self.contains_shard(shard_id) {
            return None;
        }

        let shard_key = self.shard_id_to_key_mapping.get(&shard_id).cloned();
        let router = self.rings.get(&shard_key).expect("hashring exists");
        Some(router)
    }

    pub fn hash_ring_filter(&self, shard_id: ShardId) -> Option<hash_ring::HashRingFilter> {
        let router = self.hash_ring_router(shard_id)?;
        let ring = match router {
            HashRingRouter::Single(ring) => ring,
            HashRingRouter::Resharding { old, new } => {
                if new.len() > old.len() {
                    new
                } else {
                    old
                }
            }
        };

        Some(hash_ring::HashRingFilter::new(ring.clone(), shard_id))
    }
}

#[derive(Clone, Debug, Default)]
pub struct OperationsByMode {
    pub update_all: Vec<CollectionUpdateOperations>,
    pub update_only_existing: Vec<CollectionUpdateOperations>,
}

impl OperationsByMode {
    pub fn with_update_only_existing(mut self, operation: CollectionUpdateOperations) -> Self {
        self.update_only_existing = match operation {
            CollectionUpdateOperations::PointOperation(point_operation) => match point_operation {
                PointOperations::UpsertPoints(operation) => {
                    vec![CollectionUpdateOperations::PointOperation(
                        PointOperations::UpsertPointsConditional(
                            ConditionalInsertOperationInternal {
                                points_op: operation,
                                condition: Filter::new(), // Always true condition
                                update_mode: Some(UpdateMode::UpdateOnly),
                            },
                        ),
                    )]
                }
                PointOperations::UpsertPointsConditional(operation) => {
                    vec![CollectionUpdateOperations::PointOperation(
                        PointOperations::UpsertPointsConditional(
                            ConditionalInsertOperationInternal {
                                points_op: operation.points_op,
                                condition: operation.condition,
                                update_mode: Some(UpdateMode::UpdateOnly),
                            },
                        ),
                    )]
                }

                PointOperations::DeletePoints { ids } => {
                    vec![CollectionUpdateOperations::PointOperation(
                        PointOperations::DeletePoints { ids },
                    )]
                }
                PointOperations::DeletePointsByFilter(op) => {
                    vec![CollectionUpdateOperations::PointOperation(
                        PointOperations::DeletePointsByFilter(op),
                    )]
                }
                PointOperations::SyncPoints(op) => {
                    vec![CollectionUpdateOperations::PointOperation(
                        PointOperations::SyncPoints(op),
                    )]
                }
            },
            CollectionUpdateOperations::VectorOperation(_)
            | CollectionUpdateOperations::PayloadOperation(_)
            | CollectionUpdateOperations::FieldIndexOperation(_) => {
                vec![operation]
            }
            #[cfg(feature = "staging")]
            CollectionUpdateOperations::StagingOperation(_) => {
                vec![operation]
            }
        };

        self
    }
}

impl From<CollectionUpdateOperations> for OperationsByMode {
    fn from(operation: CollectionUpdateOperations) -> Self {
        Self {
            update_all: vec![operation],
            update_only_existing: Vec::new(),
        }
    }
}

fn get_ring<'a>(
    rings: &'a mut HashMap<Option<ShardKey>, HashRingRouter>,
    shard_key: &'_ Option<ShardKey>,
) -> CollectionResult<&'a mut HashRingRouter> {
    rings.get_mut(shard_key).ok_or_else(|| {
        CollectionError::bad_request(format!(
            "{} hashring does not exist",
            shard_key_fmt(shard_key)
        ))
    })
}

fn assert_resharding_state_consistency(
    state: &Option<ReshardState>,
    ring: &HashRingRouter,
    shard_key: &Option<ShardKey>,
) {
    match state.as_ref().map(|state| state.stage) {
        Some(ReshardingStage::MigratingPoints | ReshardingStage::ReadHashRingCommitted) => {
            debug_assert!(
                ring.is_resharding(),
                "resharding is in progress, \
                 but {shard_key:?} hashring is not a resharding hashring:\n\
                 {state:#?}"
            );
        }

        Some(ReshardingStage::WriteHashRingCommitted) => {
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

fn check_stage(stage: ReshardingStage) -> impl Fn(&ReshardState) -> CollectionResult<()> {
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

/// Resharding consensus operations must be idempotent.
///
/// The consensus apply loop (`apply_entries` in `consensus_manager.rs`) silently
/// swallows all non-`ServiceError` results and marks the entry as applied. Since
/// resharding check functions return `bad_request` (`StorageError::BadRequest`,
/// not `ServiceError`), any validation failure is silently swallowed.
///
/// If local state on a peer diverges for any reason (crash during partial apply,
/// prior swallowed error), the same committed Raft entry produces different
/// outcomes on different peers — succeeding on some, silently swallowed on
/// others — causing permanent resharding state divergence despite identical
/// consensus term and commit.
///
/// These tests verify that resharding operations return `Ok` when the desired
/// post-condition is already met or the operation is inapplicable, rather than
/// returning `bad_request` errors that get silently swallowed.
#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn make_reshard_key(shard_id: ShardId) -> ReshardKey {
        ReshardKey {
            uuid: Uuid::new_v4(),
            direction: ReshardingDirection::Up,
            peer_id: 1,
            shard_id,
            shard_key: None,
        }
    }

    fn make_holder() -> (tempfile::TempDir, ShardHolder) {
        let dir = tempfile::tempdir().unwrap();
        let holder = ShardHolder::new(dir.path(), ShardingMethod::Auto).unwrap();
        (dir, holder)
    }

    // ------------------------------------------------------------------
    // check_abort_resharding idempotency
    // ------------------------------------------------------------------

    #[test]
    fn test_abort_check_idempotent_when_no_resharding_active() {
        let (_dir, holder) = make_holder();
        let key = make_reshard_key(1);

        // No resharding is in progress. This happens when:
        // - The abort was already applied on this node (crash recovery replay)
        // - The start was never applied on this node (prior swallowed error)
        //
        // Returning bad_request here causes silent state divergence: the error
        // is swallowed by apply_entries, the entry is marked as applied, but
        // resharding state is not cleared on this peer while it IS cleared on
        // peers where the operation succeeded.
        let result = holder.check_abort_resharding(&key);
        assert!(
            result.is_ok(),
            "check_abort_resharding must return Ok when no resharding is active \
             (idempotent: already aborted or never started), got error: {result:?}"
        );
    }

    #[test]
    fn test_abort_check_idempotent_when_different_resharding_active() {
        let (_dir, holder) = make_holder();

        // Set up resharding state for shard 1
        let active_key = make_reshard_key(1);
        holder
            .resharding_state
            .write(|state| {
                *state = Some(ReshardState::new(
                    active_key.uuid,
                    active_key.direction,
                    active_key.peer_id,
                    active_key.shard_id,
                    active_key.shard_key.clone(),
                ));
            })
            .unwrap();

        // Try to abort a DIFFERENT resharding (shard 2). This can happen when
        // the original resharding was already aborted and a new one was started.
        let other_key = make_reshard_key(2);
        let result = holder.check_abort_resharding(&other_key);
        assert!(
            result.is_ok(),
            "check_abort_resharding must return Ok when a different resharding is active \
             (the one we're aborting was already handled), got error: {result:?}"
        );
    }

    // ------------------------------------------------------------------
    // commit_read_hashring idempotency
    // ------------------------------------------------------------------

    #[test]
    fn test_commit_read_idempotent_when_no_resharding_active() {
        let (_dir, mut holder) = make_holder();
        let key = make_reshard_key(1);

        // No resharding is in progress. commit_read should be a no-op, not an
        // error that gets silently swallowed causing state divergence.
        let result = holder.commit_read_hashring(&key);
        assert!(
            result.is_ok(),
            "commit_read_hashring must return Ok when no resharding is active \
             (idempotent), got error: {result:?}"
        );
    }

    // ------------------------------------------------------------------
    // commit_write_hashring idempotency
    // ------------------------------------------------------------------

    #[test]
    fn test_commit_write_idempotent_when_no_resharding_active() {
        let (_dir, mut holder) = make_holder();
        let key = make_reshard_key(1);

        let result = holder.commit_write_hashring(&key);
        assert!(
            result.is_ok(),
            "commit_write_hashring must return Ok when no resharding is active \
             (idempotent), got error: {result:?}"
        );
    }

    // ------------------------------------------------------------------
    // check_finish_resharding idempotency
    // ------------------------------------------------------------------

    #[test]
    fn test_finish_check_idempotent_when_no_resharding_active() {
        let (_dir, mut holder) = make_holder();
        let key = make_reshard_key(1);

        let result = holder.check_finish_resharding(&key);
        assert!(
            result.is_ok(),
            "check_finish_resharding must return Ok when no resharding is active \
             (idempotent: already finished), got error: {result:?}"
        );
    }

    // ------------------------------------------------------------------
    // Divergence scenario: proves the bug end-to-end
    // ------------------------------------------------------------------

    /// Simulates two peers processing the same sequence of committed Raft
    /// entries, where peer B missed the initial "start" due to a silently
    /// swallowed error. Every subsequent operation must succeed on BOTH peers
    /// to prevent permanent resharding state divergence.
    #[test]
    fn test_resharding_ops_do_not_cause_divergence_between_peers() {
        let (_dir_a, holder_a) = make_holder();
        let (_dir_b, holder_b) = make_holder();
        let key = make_reshard_key(1);

        // Simulate: "start resharding" was applied on peer A but silently
        // swallowed on peer B (e.g. due to shard already existing from a
        // crashed previous attempt).
        holder_a
            .resharding_state
            .write(|state| {
                *state = Some(ReshardState::new(
                    key.uuid,
                    key.direction,
                    key.peer_id,
                    key.shard_id,
                    key.shard_key.clone(),
                ));
            })
            .unwrap();
        // peer B has no resharding state (start was swallowed)

        // Now an "abort resharding" consensus entry is committed.
        // Both peers must return Ok. If peer B returns bad_request, the error
        // is silently swallowed, state is not modified, and peer A clears its
        // resharding state while peer B remains without it — but a future
        // resharding start would diverge because peer A is clean while peer B
        // might have leftover state from other operations.
        let result_a = holder_a.check_abort_resharding(&key);
        let result_b = holder_b.check_abort_resharding(&key);

        assert!(
            result_a.is_ok(),
            "Peer A (has resharding state) must succeed: {result_a:?}"
        );
        assert!(
            result_b.is_ok(),
            "Peer B (no resharding state) must also succeed to prevent divergence: {result_b:?}"
        );
    }
}
