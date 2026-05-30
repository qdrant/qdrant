use std::ops::Deref as _;
use std::time::Duration;

use futures::stream::FuturesUnordered;
use futures::{FutureExt as _, StreamExt as _};
use itertools::Itertools as _;

use super::{clock_set, ReplicaSetState, ReplicaState, ShardReplicaSet};
use crate::operations::point_ops::WriteOrdering;
use crate::operations::types::{CollectionError, CollectionResult, UpdateResult, UpdateStatus};
use crate::operations::{ClockTag, CollectionUpdateOperations, OperationWithClockTag};
use crate::shards::shard::PeerId;
use crate::shards::shard_trait::ShardOperation as _;

/// Maximum number of attempts for applying an update with a new clock.
///
/// If an update is rejected because of an old clock, we will try again with a new clock. This
/// describes the maximum number of times we try the update.
const UPDATE_MAX_CLOCK_REJECTED_RETRIES: usize = 3;

const DEFAULT_SHARD_DEACTIVATION_TIMEOUT: Duration = Duration::from_secs(30);

impl ShardReplicaSet {
    /// Update local shard if any without forwarding to remote shards
    ///
    /// # Cancel safety
    ///
    /// This method is *not* cancel safe.
    pub async fn update_local(
        &self,
        operation: OperationWithClockTag,
        wait: bool,
    ) -> CollectionResult<Option<UpdateResult>> {
        // `ShardOperations::update` is not guaranteed to be cancel safe, so this method is not
        // cancel safe.

        let local = self.local.read().await;

        if let Some(local_shard) = local.deref() {
            match self.peer_state(&self.this_peer_id()) {
                Some(ReplicaState::Active | ReplicaState::Partial | ReplicaState::Initializing) => {
                    Ok(Some(local_shard.get().update(operation, wait).await?))
                }
                Some(ReplicaState::Listener) => {
                    Ok(Some(local_shard.get().update(operation, false).await?))
                }
                // In recovery state, only allow operations with force flag
                Some(ReplicaState::PartialSnapshot | ReplicaState::Recovery)
                    if operation.clock_tag.map_or(false, |tag| tag.force) =>
                {
                    Ok(Some(local_shard.get().update(operation, wait).await?))
                }
                Some(
                    ReplicaState::PartialSnapshot | ReplicaState::Recovery | ReplicaState::Dead,
                )
                | None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    /// # Cancel safety
    ///
    /// This method is *not* cancel safe.
    pub async fn update_with_consistency(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
        ordering: WriteOrdering,
    ) -> CollectionResult<UpdateResult> {
        // `ShardReplicaSet::update` is not cancel safe, so this method is not cancel safe.

        let Some(leader_peer) = self.leader_peer_for_update(ordering) else {
            return Err(CollectionError::service_error(format!(
                "Cannot update shard {}:{} with {ordering:?} ordering because no leader could be selected",
                self.collection_id, self.shard_id
            )));
        };

        // If we are the leader, run the update from this replica set
        if leader_peer == self.this_peer_id() {
            // Lock updates if ordering is strong or medium
            let _write_ordering_lock = match ordering {
                WriteOrdering::Strong | WriteOrdering::Medium => {
                    Some(self.write_ordering_lock.lock().await)
                }
                WriteOrdering::Weak => None,
            };

            self.update(operation, wait).await
        } else {
            // Forward the update to the designated leader
            self.forward_update(leader_peer, operation, wait, ordering)
                .await
                .map_err(|err| {
                    if err.is_transient() {
                        // Deactivate the peer if forwarding failed with transient error
                        self.add_locally_disabled(leader_peer);

                        // Return service error
                        CollectionError::service_error(format!(
                            "Failed to apply update with {ordering:?} ordering via leader peer {leader_peer}: {err}"
                        ))
                    } else {
                        err
                    }
                })
        }
    }

    /// Designated a leader replica for the update based on the WriteOrdering
    fn leader_peer_for_update(&self, ordering: WriteOrdering) -> Option<PeerId> {
        match ordering {
            WriteOrdering::Weak => Some(self.this_peer_id()), // no requirement for consistency
            WriteOrdering::Medium => self.highest_alive_replica_peer_id(), // consistency with highest alive replica
            WriteOrdering::Strong => self.highest_replica_peer_id(), // consistency with highest replica
        }
    }

    fn highest_alive_replica_peer_id(&self) -> Option<PeerId> {
        let read_lock = self.replica_state.read();
        let peer_ids = read_lock.peers.keys().cloned().collect::<Vec<_>>();
        drop(read_lock);

        peer_ids
            .into_iter()
            .filter(|peer_id| self.peer_is_active(peer_id)) // re-acquire replica_state read lock
            .max()
    }

    fn highest_replica_peer_id(&self) -> Option<PeerId> {
        self.replica_state.read().peers.keys().max().cloned()
    }

    /// # Cancel safety
    ///
    /// This method is *not* cancel safe.
    async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        // `ShardRepilcaSet::update_impl` is not cancel safe, so this method is not cancel safe.

        // TODO: Optimize `remotes`/`local`/`clock` locking for the "happy path"?
        //
        // E.g., refactor `update`/`update_impl`, so that it would be possible to:
        // - lock `remotes`, `local`, `clock` (in specified order!) on the *first* iteration of the loop
        // - then release and lock `remotes` and `local` *only* for all next iterations
        // - but keep initial `clock` for the whole duration of `update`
        let mut clock = self.clock_set.lock().await.get_clock();

        for attempt in 1..=UPDATE_MAX_CLOCK_REJECTED_RETRIES {
            let is_non_zero_tick = clock.current_tick().is_some();

            let res = self
                .update_impl(operation.clone(), wait, &mut clock)
                .await?;

            if let Some(res) = res {
                return Ok(res);
            }

            // Log a warning, if operation was rejected... but only if operation had a non-0 tick,
            // because operations with tick 0 should *always* be rejected and rejection is *expected*.
            if is_non_zero_tick {
                log::warn!(
                    "Operation {operation:?} was rejected by some node(s), retrying... \
                     (attempt {attempt}/{UPDATE_MAX_CLOCK_REJECTED_RETRIES})"
                );
            }
        }

        Err(CollectionError::service_error(format!(
            "Failed to apply operation {operation:?} \
             after {UPDATE_MAX_CLOCK_REJECTED_RETRIES} attempts, \
             all attempts were rejected",
        )))
    }

    /// # Cancel safety
    ///
    /// This method is *not* cancel safe.
    async fn update_impl(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
        clock: &mut clock_set::ClockGuard,
    ) -> CollectionResult<Option<UpdateResult>> {
        // `LocalShard::update` is not guaranteed to be cancel safe and it's impossible to cancel
        // multiple parallel updates in a way that is *guaranteed* not to introduce inconsistencies
        // between nodes, so this method is not cancel safe.

        let remotes = self.remotes.read().await;
        let local = self.local.read().await;

        let this_peer_id = self.this_peer_id();

        // target all remote peers that can receive updates
        let active_remote_shards: Vec<_> = remotes
            .iter()
            .filter(|rs| self.peer_is_active_or_pending(&rs.peer_id))
            .collect();

        // local is defined AND the peer itself can receive updates
        let local_is_updatable = local.is_some() && self.peer_is_active_or_pending(&this_peer_id);

        if active_remote_shards.is_empty() && !local_is_updatable {
            return Err(CollectionError::service_error(format!(
                "The replica set for shard {} on peer {} has no active replica",
                self.shard_id, this_peer_id
            )));
        }

        let current_clock_tick = clock.tick_once();
        let clock_tag = ClockTag::new(this_peer_id, clock.id() as _, current_clock_tick);
        let operation = OperationWithClockTag::new(operation, Some(clock_tag));

        let mut update_futures = Vec::with_capacity(active_remote_shards.len() + 1);

        if let Some(local) = local.deref() {
            if self.peer_is_active_or_pending(&this_peer_id) {
                let local_wait = if self.peer_state(&this_peer_id) == Some(ReplicaState::Listener) {
                    false
                } else {
                    wait
                };

                let operation = operation.clone();

                let local_update = async move {
                    local
                        .get()
                        .update(operation, local_wait)
                        .await
                        .map(|ok| (this_peer_id, ok))
                        .map_err(|err| (this_peer_id, err))
                };

                update_futures.push(local_update.left_future());
            }
        }

        for remote in active_remote_shards {
            let operation = operation.clone();

            let remote_update = async move {
                remote
                    .update(operation, wait)
                    .await
                    .map(|ok| (remote.peer_id, ok))
                    .map_err(|err| (remote.peer_id, err))
            };

            update_futures.push(remote_update.right_future());
        }

        let all_res: Vec<Result<_, _>> = match self.shared_storage_config.update_concurrency {
            Some(concurrency) => {
                futures::stream::iter(update_futures)
                    .buffer_unordered(concurrency.get())
                    .collect()
                    .await
            }

            None => FuturesUnordered::from_iter(update_futures).collect().await,
        };

        drop(remotes);
        drop(local);

        let total_results = all_res.len();

        let write_consistency_factor = self
            .collection_config
            .read()
            .await
            .params
            .write_consistency_factor
            .get() as usize;

        let minimal_success_count = write_consistency_factor.min(total_results);

        let (successes, failures): (Vec<_>, Vec<_>) = all_res.into_iter().partition_result();

        // Advance clock if some replica echoed *newer* tick

        let new_clock_tick = successes
            .iter()
            .filter_map(|(_, result)| {
                let echo_tag = result.clock_tag?;

                if echo_tag.peer_id != clock_tag.peer_id {
                    debug_assert!(
                        false,
                        "Echoed clock tag peer_id does not match the original"
                    );
                    return None;
                }

                if echo_tag.clock_id != clock_tag.clock_id {
                    debug_assert!(
                        false,
                        "Echoed clock tag clock_id does not match the original"
                    );
                    return None;
                }

                Some(echo_tag.clock_tick)
            })
            .max();

        if let Some(new_clock_tick) = new_clock_tick {
            clock.advance_to(new_clock_tick);
        }

        // Notify consensus about failures if:
        // 1. There is at least one success, otherwise it might be a problem of sending node
        // 2. ???

        let failure_error = if let Some((peer_id, collection_error)) = failures.first() {
            format!("Failed peer: {}, error: {}", peer_id, collection_error)
        } else {
            "".to_string()
        };

        if successes.len() >= minimal_success_count {
            let wait_for_deactivation =
                self.handle_failed_replicas(&failures, &self.replica_state.read());

            // report all failing peers to consensus
            if wait && wait_for_deactivation && !failures.is_empty() {
                // ToDo: allow timeout configuration in API
                let timeout = DEFAULT_SHARD_DEACTIVATION_TIMEOUT;

                let replica_state = self.replica_state.clone();
                let peer_ids: Vec<_> = failures.iter().map(|(peer_id, _)| *peer_id).collect();

                let shards_disabled = tokio::task::spawn_blocking(move || {
                    replica_state.wait_for(
                        |state| {
                            peer_ids.iter().all(|peer_id| {
                                state
                                    .peers
                                    .get(peer_id)
                                    .map(|state| state != &ReplicaState::Active)
                                    .unwrap_or(true) // not found means that peer is dead
                            })
                        },
                        DEFAULT_SHARD_DEACTIVATION_TIMEOUT,
                    )
                })
                .await?;

                if !shards_disabled {
                    return Err(CollectionError::service_error(format!(
                        "Some replica of shard {} failed to apply operation and deactivation \
                         timed out after {} seconds. Consistency of this update is not guaranteed. Please retry. {failure_error}",
                        self.shard_id, timeout.as_secs()
                    )));
                }
            }
        }

        if !failures.is_empty() && successes.len() < minimal_success_count {
            // completely failed - report error to user
            let (_peer_id, err) = failures.into_iter().next().expect("failures is not empty");
            return Err(err);
        }

        if !successes
            .iter()
            .any(|(peer_id, _)| self.peer_is_active(peer_id))
        {
            return Err(CollectionError::service_error(format!(
                "Failed to apply operation to at least one `Active` replica. \
                 Consistency of this update is not guaranteed. Please retry. {failure_error}"
            )));
        }

        let is_any_operation_rejected = successes
            .iter()
            .any(|(_, res)| matches!(res.status, UpdateStatus::ClockRejected));

        if is_any_operation_rejected {
            return Ok(None);
        }

        // there are enough successes, return the first one
        let (_, res) = successes
            .into_iter()
            .next()
            .expect("successes is not empty");

        Ok(Some(res))
    }

    fn peer_is_active_or_pending(&self, peer_id: &PeerId) -> bool {
        let res = match self.peer_state(peer_id) {
            Some(ReplicaState::Active) => true,
            Some(ReplicaState::Partial) => true,
            Some(ReplicaState::Initializing) => true,
            Some(ReplicaState::Dead) => false,
            Some(ReplicaState::Listener) => true,
            Some(ReplicaState::PartialSnapshot) => false,
            Some(ReplicaState::Recovery) => false,
            None => false,
        };
        res && !self.is_locally_disabled(peer_id)
    }

    fn handle_failed_replicas(
        &self,
        failures: &Vec<(PeerId, CollectionError)>,
        state: &ReplicaSetState,
    ) -> bool {
        let mut wait_for_deactivation = false;

        for (peer_id, err) in failures {
            log::warn!(
                "Failed to update shard {}:{} on peer {peer_id}, error: {err}",
                self.collection_id,
                self.shard_id,
            );

            let Some(&peer_state) = state.get_peer_state(peer_id) else {
                continue;
            };

            if peer_state != ReplicaState::Active && peer_state != ReplicaState::Initializing {
                continue;
            }

            if peer_state == ReplicaState::Partial
                && matches!(err, CollectionError::PreConditionFailed { .. })
            {
                // Handles a special case where transfer receiver haven't created a shard yet.
                // In this case update should be handled by source shard and forward proxy.
                continue;
            }

            if err.is_transient() || peer_state == ReplicaState::Initializing {
                // If the error is transient, we should not deactivate the peer
                // before allowing other operations to continue.
                // Otherwise, the failed node can become responsive again, before
                // the other nodes deactivate it, so the storage might be inconsistent.
                wait_for_deactivation = true;
            }

            log::debug!(
                "Deactivating peer {peer_id} because of failed update of shard {}:{}",
                self.collection_id,
                self.shard_id
            );

            self.add_locally_disabled(*peer_id);
        }

        wait_for_deactivation
    }

    /// Forward update to the leader replica
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn forward_update(
        &self,
        leader_peer: PeerId,
        operation: CollectionUpdateOperations,
        wait: bool,
        ordering: WriteOrdering,
    ) -> CollectionResult<UpdateResult> {
        // `RemoteShard::forward_update` is cancel safe, so this method is cancel safe.

        let remotes_guard = self.remotes.read().await;

        let Some(remote_leader) = remotes_guard.iter().find(|r| r.peer_id == leader_peer) else {
            return Err(CollectionError::service_error(format!(
                "Cannot forward update to shard {} because was removed from the replica set",
                self.shard_id
            )));
        };

        remote_leader
            .forward_update(OperationWithClockTag::from(operation), wait, ordering) // `clock_tag` *have to* be `None`!
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::num::NonZeroU32;
    use std::sync::Arc;

    use common::cpu::CpuBudget;
    use segment::types::Distance;
    use tempfile::{Builder, TempDir};
    use tokio::runtime::Handle;
    use tokio::sync::RwLock;

    use super::*;
    use crate::config::*;
    use crate::operations::types::VectorsConfig;
    use crate::operations::vector_params_builder::VectorParamsBuilder;
    use crate::optimizers_builder::OptimizersConfig;
    use crate::shards::replica_set::{AbortShardTransfer, ChangePeerState};

    #[tokio::test]
    async fn test_highest_replica_peer_id() {
        let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
        let rs = new_shard_replica_set(&collection_dir).await;

        assert_eq!(rs.highest_replica_peer_id(), Some(5));
        // at build time the replicas are all dead, they need to be activated
        assert_eq!(rs.highest_alive_replica_peer_id(), None);

        rs.set_replica_state(&1, ReplicaState::Active).unwrap();
        rs.set_replica_state(&3, ReplicaState::Active).unwrap();
        rs.set_replica_state(&4, ReplicaState::Active).unwrap();
        rs.set_replica_state(&5, ReplicaState::Partial).unwrap();

        assert_eq!(rs.highest_replica_peer_id(), Some(5));
        assert_eq!(rs.highest_alive_replica_peer_id(), Some(4));
    }

    const TEST_OPTIMIZERS_CONFIG: OptimizersConfig = OptimizersConfig {
        deleted_threshold: 0.9,
        vacuum_min_vector_number: 1000,
        default_segment_number: 2,
        max_segment_size: None,
        memmap_threshold: None,
        indexing_threshold: Some(50_000),
        flush_interval_sec: 30,
        max_optimization_threads: Some(2),
    };

    async fn new_shard_replica_set(collection_dir: &TempDir) -> ShardReplicaSet {
        let update_runtime = Handle::current();
        let search_runtime = Handle::current();

        let wal_config = WalConfig {
            wal_capacity_mb: 1,
            wal_segments_ahead: 0,
        };

        let collection_params = CollectionParams {
            vectors: VectorsConfig::Single(VectorParamsBuilder::new(4, Distance::Dot).build()),
            shard_number: NonZeroU32::new(4).unwrap(),
            replication_factor: NonZeroU32::new(3).unwrap(),
            write_consistency_factor: NonZeroU32::new(2).unwrap(),
            ..CollectionParams::empty()
        };

        let config = CollectionConfig {
            params: collection_params,
            optimizer_config: TEST_OPTIMIZERS_CONFIG.clone(),
            wal_config,
            hnsw_config: Default::default(),
            quantization_config: None,
        };

        let shared_config = Arc::new(RwLock::new(config.clone()));
        let remotes = HashSet::from([2, 3, 4, 5]);
        ShardReplicaSet::build(
            1,
            "test_collection".to_string(),
            1,
            false,
            remotes,
            dummy_on_replica_failure(),
            dummy_abort_shard_transfer(),
            collection_dir.path(),
            shared_config,
            Default::default(),
            Default::default(),
            update_runtime,
            search_runtime,
            CpuBudget::default(),
            None,
        )
        .await
        .unwrap()
    }

    fn dummy_on_replica_failure() -> ChangePeerState {
        Arc::new(move |_peer_id, _shard_id| {})
    }

    fn dummy_abort_shard_transfer() -> AbortShardTransfer {
        Arc::new(|_shard_transfer, _reason| {})
    }
}
