mod clean;
mod collection_ops;
pub mod distance_matrix;
mod facet;
pub mod mmr;
pub mod payload_index_schema;
mod point_ops;
pub mod query;
mod resharding;
mod search;
mod shard_transfer;
mod sharding_keys;
mod snapshots;
mod state_management;

use std::collections::HashMap;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use clean::ShardCleanTasks;
use common::budget::ResourceBudget;
use common::save_on_disk::SaveOnDisk;
use common::types::{DetailsLevel, TelemetryDetail};
use io::storage_version::StorageVersion;
use segment::types::ShardKey;
use semver::Version;
use tokio::runtime::Handle;
use tokio::sync::{Mutex, RwLock, RwLockWriteGuard};

use crate::collection::payload_index_schema::PayloadIndexSchema;
use crate::collection_state::{ShardInfo, State};
use crate::common::collection_size_stats::{
    CollectionSizeAtomicStats, CollectionSizeStats, CollectionSizeStatsCache,
};
use crate::common::is_ready::IsReady;
use crate::config::CollectionConfigInternal;
use crate::operations::config_diff::{DiffConfig, OptimizersConfigDiff};
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{CollectionError, CollectionResult, NodeType, OptimizersStatus};
use crate::optimizers_builder::OptimizersConfig;
use crate::shards::channel_service::ChannelService;
use crate::shards::collection_shard_distribution::CollectionShardDistribution;
use crate::shards::local_shard::clock_map::RecoveryPoint;
use crate::shards::replica_set::ReplicaState::{Active, Dead, Initializing, Listener};
use crate::shards::replica_set::{
    ChangePeerFromState, ChangePeerState, ReplicaState, ShardReplicaSet,
};
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::shard_mapping::ShardKeyMapping;
use crate::shards::shard_holder::{LockedShardHolder, ShardHolder, shard_not_found_error};
use crate::shards::transfer::helpers::check_transfer_conflicts_strict;
use crate::shards::transfer::transfer_tasks_pool::{TaskResult, TransferTasksPool};
use crate::shards::transfer::{ShardTransfer, ShardTransferMethod};
use crate::shards::{CollectionId, replica_set};
use crate::telemetry::{
    CollectionConfigTelemetry, CollectionTelemetry, CollectionsAggregatedTelemetry,
};

/// Collection's data is split into several shards.
pub struct Collection {
    pub(crate) id: CollectionId,
    pub(crate) shards_holder: Arc<LockedShardHolder>,
    pub(crate) collection_config: Arc<RwLock<CollectionConfigInternal>>,
    pub(crate) shared_storage_config: Arc<SharedStorageConfig>,
    payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
    optimizers_overwrite: Option<OptimizersConfigDiff>,
    this_peer_id: PeerId,
    path: PathBuf,
    snapshots_path: PathBuf,
    channel_service: ChannelService,
    transfer_tasks: Mutex<TransferTasksPool>,
    request_shard_transfer_cb: RequestShardTransfer,
    notify_peer_failure_cb: ChangePeerFromState,
    abort_shard_transfer_cb: replica_set::AbortShardTransfer,
    init_time: Duration,
    // One-way boolean flag that is set to true when the collection is fully initialized
    // i.e. all shards are activated for the first time.
    is_initialized: Arc<IsReady>,
    // Lock to temporary block collection update operations while the collection is being migrated.
    // Lock is acquired for read on update operation and can be acquired for write externally,
    // which will block all update operations until the lock is released.
    updates_lock: Arc<RwLock<()>>,
    // Update runtime handle.
    update_runtime: Handle,
    // Search runtime handle.
    search_runtime: Handle,
    optimizer_resource_budget: ResourceBudget,
    // Cached statistics of collection size, may be outdated.
    collection_stats_cache: CollectionSizeStatsCache,
    // Background tasks to clean shards
    shard_clean_tasks: ShardCleanTasks,
}

pub type RequestShardTransfer = Arc<dyn Fn(ShardTransfer) + Send + Sync>;

pub type OnTransferFailure = Arc<dyn Fn(ShardTransfer, CollectionId, &str) + Send + Sync>;
pub type OnTransferSuccess = Arc<dyn Fn(ShardTransfer, CollectionId) + Send + Sync>;

impl Collection {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        name: CollectionId,
        this_peer_id: PeerId,
        path: &Path,
        snapshots_path: &Path,
        collection_config: &CollectionConfigInternal,
        shared_storage_config: Arc<SharedStorageConfig>,
        shard_distribution: CollectionShardDistribution,
        shard_key_mapping: Option<ShardKeyMapping>,
        channel_service: ChannelService,
        on_replica_failure: ChangePeerFromState,
        request_shard_transfer: RequestShardTransfer,
        abort_shard_transfer: replica_set::AbortShardTransfer,
        search_runtime: Option<Handle>,
        update_runtime: Option<Handle>,
        optimizer_resource_budget: ResourceBudget,
        optimizers_overwrite: Option<OptimizersConfigDiff>,
    ) -> CollectionResult<Self> {
        let start_time = std::time::Instant::now();

        let mut shard_holder = ShardHolder::new(path)?;
        shard_holder.set_shard_key_mappings(shard_key_mapping.clone().unwrap_or_default())?;

        let payload_index_schema = Arc::new(Self::load_payload_index_schema(path)?);

        let shared_collection_config = Arc::new(RwLock::new(collection_config.clone()));
        for (shard_id, mut peers) in shard_distribution.shards {
            let is_local = peers.remove(&this_peer_id);

            let mut effective_optimizers_config = collection_config.optimizer_config.clone();
            if let Some(optimizers_overwrite) = optimizers_overwrite.clone() {
                effective_optimizers_config =
                    effective_optimizers_config.update(&optimizers_overwrite);
            }

            let shard_key = shard_key_mapping
                .as_ref()
                .and_then(|mapping| mapping.shard_key(shard_id));
            let replica_set = ShardReplicaSet::build(
                shard_id,
                shard_key.clone(),
                name.clone(),
                this_peer_id,
                is_local,
                peers,
                on_replica_failure.clone(),
                abort_shard_transfer.clone(),
                path,
                shared_collection_config.clone(),
                effective_optimizers_config,
                shared_storage_config.clone(),
                payload_index_schema.clone(),
                channel_service.clone(),
                update_runtime.clone().unwrap_or_else(Handle::current),
                search_runtime.clone().unwrap_or_else(Handle::current),
                optimizer_resource_budget.clone(),
                None,
            )
            .await?;

            shard_holder.add_shard(shard_id, replica_set, shard_key)?;
        }

        let locked_shard_holder = Arc::new(LockedShardHolder::new(shard_holder));

        let collection_stats_cache = CollectionSizeStatsCache::new_with_values(
            Self::estimate_collection_size_stats(&locked_shard_holder).await,
        );

        // Once the config is persisted - the collection is considered to be successfully created.
        CollectionVersion::save(path)?;
        collection_config.save(path)?;

        Ok(Self {
            id: name.clone(),
            shards_holder: locked_shard_holder,
            collection_config: shared_collection_config,
            optimizers_overwrite,
            payload_index_schema,
            shared_storage_config,
            this_peer_id,
            path: path.to_owned(),
            snapshots_path: snapshots_path.to_owned(),
            channel_service,
            transfer_tasks: Mutex::new(TransferTasksPool::new(name.clone())),
            request_shard_transfer_cb: request_shard_transfer.clone(),
            notify_peer_failure_cb: on_replica_failure.clone(),
            abort_shard_transfer_cb: abort_shard_transfer,
            init_time: start_time.elapsed(),
            is_initialized: Default::default(),
            updates_lock: Default::default(),
            update_runtime: update_runtime.unwrap_or_else(Handle::current),
            search_runtime: search_runtime.unwrap_or_else(Handle::current),
            optimizer_resource_budget,
            collection_stats_cache,
            shard_clean_tasks: Default::default(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn load(
        collection_id: CollectionId,
        this_peer_id: PeerId,
        path: &Path,
        snapshots_path: &Path,
        shared_storage_config: Arc<SharedStorageConfig>,
        channel_service: ChannelService,
        on_replica_failure: replica_set::ChangePeerFromState,
        request_shard_transfer: RequestShardTransfer,
        abort_shard_transfer: replica_set::AbortShardTransfer,
        search_runtime: Option<Handle>,
        update_runtime: Option<Handle>,
        optimizer_resource_budget: ResourceBudget,
        optimizers_overwrite: Option<OptimizersConfigDiff>,
    ) -> Self {
        let start_time = std::time::Instant::now();
        let stored_version = CollectionVersion::load(path)
            .expect("Can't read collection version")
            .expect("Collection version is not found");

        let app_version = CollectionVersion::current();

        if stored_version > app_version {
            panic!("Collection version is greater than application version");
        }

        if stored_version != app_version {
            if Self::can_upgrade_storage(&stored_version, &app_version) {
                log::info!("Migrating collection {stored_version} -> {app_version}");
                CollectionVersion::save(path)
                    .unwrap_or_else(|err| panic!("Can't save collection version {err}"));
            } else {
                log::error!("Cannot upgrade version {stored_version} to {app_version}.");
                panic!(
                    "Cannot upgrade version {stored_version} to {app_version}. Try to use older version of Qdrant first.",
                );
            }
        }

        let collection_config = CollectionConfigInternal::load(path).unwrap_or_else(|err| {
            panic!(
                "Can't read collection config due to {}\nat {}",
                err,
                path.to_str().unwrap(),
            )
        });
        collection_config.validate_and_warn();

        let mut shard_holder = ShardHolder::new(path).expect("Can not create shard holder");

        let mut effective_optimizers_config = collection_config.optimizer_config.clone();

        if let Some(optimizers_overwrite) = optimizers_overwrite.clone() {
            effective_optimizers_config = effective_optimizers_config.update(&optimizers_overwrite);
        }

        let shared_collection_config = Arc::new(RwLock::new(collection_config.clone()));

        let payload_index_schema = Arc::new(
            Self::load_payload_index_schema(path)
                .expect("Can't load or initialize payload index schema"),
        );

        shard_holder
            .load_shards(
                path,
                &collection_id,
                shared_collection_config.clone(),
                effective_optimizers_config,
                shared_storage_config.clone(),
                payload_index_schema.clone(),
                channel_service.clone(),
                on_replica_failure.clone(),
                abort_shard_transfer.clone(),
                this_peer_id,
                update_runtime.clone().unwrap_or_else(Handle::current),
                search_runtime.clone().unwrap_or_else(Handle::current),
                optimizer_resource_budget.clone(),
            )
            .await;

        let locked_shard_holder = Arc::new(LockedShardHolder::new(shard_holder));

        let collection_stats_cache = CollectionSizeStatsCache::new_with_values(
            Self::estimate_collection_size_stats(&locked_shard_holder).await,
        );

        Self {
            id: collection_id.clone(),
            shards_holder: locked_shard_holder,
            collection_config: shared_collection_config,
            optimizers_overwrite,
            payload_index_schema,
            shared_storage_config,
            this_peer_id,
            path: path.to_owned(),
            snapshots_path: snapshots_path.to_owned(),
            channel_service,
            transfer_tasks: Mutex::new(TransferTasksPool::new(collection_id.clone())),
            request_shard_transfer_cb: request_shard_transfer.clone(),
            notify_peer_failure_cb: on_replica_failure,
            abort_shard_transfer_cb: abort_shard_transfer,
            init_time: start_time.elapsed(),
            is_initialized: Default::default(),
            updates_lock: Default::default(),
            update_runtime: update_runtime.unwrap_or_else(Handle::current),
            search_runtime: search_runtime.unwrap_or_else(Handle::current),
            optimizer_resource_budget,
            collection_stats_cache,
            shard_clean_tasks: Default::default(),
        }
    }

    /// Check if stored version have consequent version.
    /// If major version is different, then it is not compatible.
    /// If the difference in consecutive versions is greater than 1 in patch,
    /// then the collection is not compatible with the current version.
    ///
    /// Example:
    ///   0.4.0 -> 0.4.1 = true
    ///   0.4.0 -> 0.4.2 = false
    ///   0.4.0 -> 0.5.0 = false
    ///   0.4.0 -> 0.5.1 = false
    pub fn can_upgrade_storage(stored: &Version, app: &Version) -> bool {
        if stored.major != app.major {
            return false;
        }
        if stored.minor != app.minor {
            return false;
        }
        if stored.patch + 1 < app.patch {
            return false;
        }
        true
    }

    pub fn name(&self) -> String {
        self.id.clone()
    }

    pub async fn uuid(&self) -> Option<uuid::Uuid> {
        self.collection_config.read().await.uuid
    }

    pub async fn get_shard_keys(&self) -> Vec<ShardKey> {
        self.shards_holder
            .read()
            .await
            .get_shard_key_to_ids_mapping()
            .keys()
            .cloned()
            .collect()
    }

    /// Return a list of local shards, present on this peer
    pub async fn get_local_shards(&self) -> Vec<ShardId> {
        self.shards_holder.read().await.get_local_shards().await
    }

    pub async fn contains_shard(&self, shard_id: ShardId) -> bool {
        self.shards_holder.read().await.contains_shard(shard_id)
    }

    pub async fn wait_local_shard_replica_state(
        &self,
        shard_id: ShardId,
        state: ReplicaState,
        timeout: Duration,
    ) -> CollectionResult<()> {
        let shard_holder_read = self.shards_holder.read().await;

        let shard = shard_holder_read.get_shard(shard_id);
        let Some(replica_set) = shard else {
            return Err(CollectionError::NotFound {
                what: format!("Shard {shard_id}"),
            });
        };

        replica_set.wait_for_local_state(state, timeout).await
    }

    pub async fn set_shard_replica_state(
        &self,
        shard_id: ShardId,
        peer_id: PeerId,
        new_state: ReplicaState,
        from_state: Option<ReplicaState>,
    ) -> CollectionResult<()> {
        let shard_holder = self.shards_holder.read().await;
        let replica_set = shard_holder
            .get_shard(shard_id)
            .ok_or_else(|| shard_not_found_error(shard_id))?;

        log::debug!(
            "Changing shard {}:{shard_id} replica state from {:?} to {new_state:?}",
            self.id,
            replica_set.peer_state(peer_id),
        );

        let current_state = replica_set.peer_state(peer_id);

        // Validation:
        //
        // 1. Check that peer exists in the cluster (peer might *not* exist, if it was removed from
        //    the cluster right before `SetShardReplicaSet` was proposed)
        let peer_exists = self
            .channel_service
            .id_to_address
            .read()
            .contains_key(&peer_id);

        let replica_exists = replica_set.peer_state(peer_id).is_some();

        if !peer_exists && !replica_exists {
            return Err(CollectionError::bad_input(format!(
                "Can't set replica {peer_id}:{shard_id} state to {new_state:?}, \
                 because replica {peer_id}:{shard_id} does not exist \
                 and peer {peer_id} is not part of the cluster"
            )));
        }

        // 2. Check that `from_state` matches current state
        if from_state.is_some() && current_state != from_state {
            return Err(CollectionError::bad_input(format!(
                "Replica {peer_id} of shard {shard_id} has state {current_state:?}, but expected {from_state:?}"
            )));
        }

        // 3. Do not deactivate the last active replica
        //
        // `is_last_active_replica` counts both `Active` and `ReshardingScaleDown` replicas!
        if replica_set.is_last_source_of_truth_replica(peer_id) && !new_state.is_active() {
            return Err(CollectionError::bad_input(format!(
                "Cannot deactivate the last active replica {peer_id} of shard {shard_id}"
            )));
        }

        // Update replica status
        replica_set
            .ensure_replica_with_state(peer_id, new_state)
            .await?;

        if new_state == ReplicaState::Dead {
            let resharding_state = shard_holder.resharding_state.read().clone();
            let related_transfers = shard_holder.get_related_transfers(shard_id, peer_id);

            // Functions below lock `shard_holder`!
            drop(shard_holder);

            let mut abort_resharding_result = CollectionResult::Ok(());

            // Abort resharding, if resharding shard is marked as `Dead`.
            //
            // This branch should only be triggered, if resharding is currently at `MigratingPoints`
            // stage, because target shard should be marked as `Active`, when all resharding transfers
            // are successfully completed, and so the check *right above* this one would be triggered.
            //
            // So, if resharding reached `ReadHashRingCommitted`, this branch *won't* be triggered,
            // and resharding *won't* be cancelled. The update request should *fail* with "failed to
            // update all replicas of a shard" error.
            //
            // If resharding reached `ReadHashRingCommitted`, and this branch is triggered *somehow*,
            // then `Collection::abort_resharding` call should return an error, so no special handling
            // is needed.
            let is_resharding = current_state
                .as_ref()
                .is_some_and(ReplicaState::is_resharding);
            if is_resharding && let Some(state) = resharding_state {
                abort_resharding_result = self.abort_resharding(state.key(), false).await;
            }

            // Terminate transfer if source or target replicas are now dead
            for transfer in related_transfers {
                self.abort_shard_transfer_and_resharding(transfer.key(), None)
                    .await?;
            }

            // Propagate resharding errors now
            abort_resharding_result?;
        }

        // If not initialized yet, we need to check if it was initialized by this call
        if !self.is_initialized.check_ready() {
            let state = self.state().await;

            let mut is_ready = true;

            for (_shard_id, shard_info) in state.shards {
                let all_replicas_active = shard_info.replicas.into_iter().all(|(_, state)| {
                    matches!(
                        state,
                        ReplicaState::Active | ReplicaState::ReshardingScaleDown
                    )
                });

                if !all_replicas_active {
                    is_ready = false;
                    break;
                }
            }

            if is_ready {
                self.is_initialized.make_ready();
            }
        }

        Ok(())
    }

    pub async fn shard_recovery_point(&self, shard_id: ShardId) -> CollectionResult<RecoveryPoint> {
        let shard_holder_read = self.shards_holder.read().await;

        let shard = shard_holder_read.get_shard(shard_id);
        let Some(replica_set) = shard else {
            return Err(CollectionError::NotFound {
                what: format!("Shard {shard_id}"),
            });
        };

        replica_set.shard_recovery_point().await
    }

    pub async fn update_shard_cutoff_point(
        &self,
        shard_id: ShardId,
        cutoff: &RecoveryPoint,
    ) -> CollectionResult<()> {
        let shard_holder_read = self.shards_holder.read().await;

        let shard = shard_holder_read.get_shard(shard_id);
        let Some(replica_set) = shard else {
            return Err(CollectionError::NotFound {
                what: format!("Shard {shard_id}"),
            });
        };

        replica_set.update_shard_cutoff_point(cutoff).await
    }

    pub async fn state(&self) -> State {
        let shards_holder = self.shards_holder.read().await;
        let transfers = shards_holder.shard_transfers.read().clone();
        let resharding = shards_holder.resharding_state.read().clone();
        State {
            config: self.collection_config.read().await.clone(),
            shards: shards_holder
                .get_shards()
                .map(|(shard_id, replicas)| {
                    let shard_info = ShardInfo {
                        replicas: replicas.peers(),
                    };
                    (shard_id, shard_info)
                })
                .collect(),
            resharding,
            transfers,
            shards_key_mapping: shards_holder.get_shard_key_to_ids_mapping(),
            payload_index_schema: self.payload_index_schema.read().clone(),
        }
    }

    pub async fn remove_shards_at_peer(&self, peer_id: PeerId) -> CollectionResult<()> {
        // Abort resharding, if shards are removed from peer driving resharding
        // (which *usually* means the *peer* is being removed from consensus)
        let resharding_state = self
            .resharding_state()
            .await
            .filter(|state| state.peer_id == peer_id);

        if let Some(state) = resharding_state
            && let Err(err) = self.abort_resharding(state.key(), true).await
        {
            log::error!(
                "Failed to abort resharding {} while removing peer {peer_id}: {err}",
                state.key(),
            );
        }

        for transfer in self.get_related_transfers(peer_id).await {
            self.abort_shard_transfer_and_resharding(transfer.key(), None)
                .await?;
        }

        self.shards_holder
            .read()
            .await
            .remove_shards_at_peer(peer_id)
            .await
    }

    pub async fn sync_local_state(
        &self,
        on_transfer_failure: OnTransferFailure,
        on_transfer_success: OnTransferSuccess,
        on_finish_init: ChangePeerState,
        on_convert_to_listener: ChangePeerState,
        on_convert_from_listener: ChangePeerState,
    ) -> CollectionResult<()> {
        // Check for disabled replicas
        let shard_holder = self.shards_holder.read().await;

        let get_shard_transfers = |shard_id, from| {
            shard_holder
                .get_transfers(|transfer| transfer.shard_id == shard_id && transfer.from == from)
        };

        for replica_set in shard_holder.all_shards() {
            replica_set.sync_local_state(get_shard_transfers)?;
        }

        // Check for un-reported finished transfers
        let outgoing_transfers = shard_holder.get_outgoing_transfers(self.this_peer_id);
        let tasks_lock = self.transfer_tasks.lock().await;
        for transfer in outgoing_transfers {
            match tasks_lock
                .get_task_status(&transfer.key())
                .map(|s| s.result)
            {
                None => {
                    log::debug!(
                        "Transfer {:?} does not exist, but not reported as cancelled. Reporting now.",
                        transfer.key(),
                    );
                    on_transfer_failure(transfer, self.name(), "transfer task does not exist");
                }
                Some(TaskResult::Running) => (),
                Some(TaskResult::Finished) => {
                    log::debug!(
                        "Transfer {:?} is finished successfully, but not reported. Reporting now.",
                        transfer.key(),
                    );
                    on_transfer_success(transfer, self.name());
                }
                Some(TaskResult::Failed) => {
                    log::debug!(
                        "Transfer {:?} is failed, but not reported as failed. Reporting now.",
                        transfer.key(),
                    );
                    on_transfer_failure(transfer, self.name(), "transfer failed");
                }
            }
        }

        // Count how many transfers we are now proposing
        // We must track this here so we can reference it when checking for tranfser limits,
        // because transfers we propose now will not be in the consensus state within the lifetime
        // of this function
        let mut proposed = HashMap::<PeerId, usize>::new();

        // Check for proper replica states
        for replica_set in shard_holder.all_shards() {
            let this_peer_id = replica_set.this_peer_id();
            let shard_id = replica_set.shard_id;

            let peers = replica_set.peers();
            let this_peer_state = peers.get(&this_peer_id).copied();

            if this_peer_state == Some(Initializing) {
                // It is possible, that collection creation didn't report
                // Try to activate shard, as the collection clearly exists
                on_finish_init(this_peer_id, shard_id);
                continue;
            }

            if self.shared_storage_config.node_type == NodeType::Listener {
                // We probably should not switch node type during resharding, so we only check for `Active`,
                // but not `ReshardingScaleDown` replica state here...
                let is_last_active = peers.values().filter(|&&state| state == Active).count() == 1;

                if this_peer_state == Some(Active) && !is_last_active {
                    // Convert active node from active to listener
                    on_convert_to_listener(this_peer_id, shard_id);
                    continue;
                }
            } else if this_peer_state == Some(Listener) {
                // Convert listener node to active
                on_convert_from_listener(this_peer_id, shard_id);
                continue;
            }

            // Don't automatically recover replicas if started in recovery mode
            if self.shared_storage_config.recovery_mode.is_some() {
                continue;
            }

            // Don't recover replicas if not dead
            let is_dead = this_peer_state == Some(Dead);
            if !is_dead {
                continue;
            }

            // Try to find dead replicas with no active transfers
            let transfers = shard_holder.get_transfers(|_| true);

            // Respect shard transfer limit, consider already proposed transfers in our counts
            let (mut incoming, outgoing) = shard_holder.count_shard_transfer_io(this_peer_id);
            incoming += proposed.get(&this_peer_id).copied().unwrap_or(0);
            if self.check_auto_shard_transfer_limit(incoming, outgoing) {
                log::trace!(
                    "Postponing automatic shard {shard_id} transfer to stay below limit on this node (incoming: {incoming}, outgoing: {outgoing})",
                );
                continue;
            }

            // Select shard transfer method, prefer user configured method or choose one now
            // If all peers are 1.8+, we try WAL delta transfer, otherwise we use the default method
            let shard_transfer_method = self
                .shared_storage_config
                .default_shard_transfer_method
                .unwrap_or_else(|| {
                    let all_support_wal_delta = self
                        .channel_service
                        .all_peers_at_version(&Version::new(1, 8, 0));
                    if all_support_wal_delta {
                        ShardTransferMethod::WalDelta
                    } else {
                        ShardTransferMethod::default()
                    }
                });

            // Try to find a replica to transfer from
            //
            // `active_remote_shards` includes `Active` and `ReshardingScaleDown` replicas!
            for replica_id in replica_set.active_remote_shards() {
                let transfer = ShardTransfer {
                    from: replica_id,
                    to: this_peer_id,
                    shard_id,
                    to_shard_id: None,
                    sync: true,
                    // For automatic shard transfers, always select some default method from this point on
                    method: Some(shard_transfer_method),
                    filter: None,
                };

                if check_transfer_conflicts_strict(&transfer, transfers.iter()).is_some() {
                    continue; // this transfer won't work
                }

                // Respect shard transfer limit, consider already proposed transfers in our counts
                let (incoming, mut outgoing) = shard_holder.count_shard_transfer_io(replica_id);
                outgoing += proposed.get(&replica_id).copied().unwrap_or(0);
                if self.check_auto_shard_transfer_limit(incoming, outgoing) {
                    log::trace!(
                        "Postponing automatic shard {shard_id} transfer to stay below limit on peer {replica_id} (incoming: {incoming}, outgoing: {outgoing})",
                    );
                    continue;
                }

                // TODO: Should we, maybe, throttle/backoff this requests a bit?
                if let Err(err) = replica_set.health_check(replica_id).await {
                    // TODO: This is rather verbose, not sure if we want to log this at all... :/
                    log::trace!(
                        "Replica {replica_id}/{}:{} is not available \
                         to request shard transfer from: \
                         {err}",
                        self.id,
                        replica_set.shard_id,
                    );
                    continue;
                }

                log::debug!(
                    "Recovering shard {}:{shard_id} on peer {this_peer_id} by requesting it from {replica_id}",
                    self.name(),
                );

                // Update our counters for proposed transfers, then request (propose) shard transfer
                *proposed.entry(transfer.from).or_default() += 1;
                *proposed.entry(transfer.to).or_default() += 1;
                self.request_shard_transfer(transfer);
                break;
            }
        }

        Ok(())
    }

    pub async fn get_telemetry_data(&self, detail: TelemetryDetail) -> CollectionTelemetry {
        let (shards_telemetry, transfers, resharding) = {
            if detail.level >= DetailsLevel::Level3 {
                let shards_holder = self.shards_holder.read().await;
                let mut shards_telemetry = Vec::new();
                for shard in shards_holder.all_shards() {
                    shards_telemetry.push(shard.get_telemetry_data(detail).await)
                }
                (
                    Some(shards_telemetry),
                    Some(shards_holder.get_shard_transfer_info(&*self.transfer_tasks.lock().await)),
                    Some(
                        shards_holder
                            .get_resharding_operations_info()
                            .unwrap_or_default(),
                    ),
                )
            } else {
                (None, None, None)
            }
        };

        let shard_clean_tasks = self.clean_local_shards_statuses();

        CollectionTelemetry {
            id: self.name(),
            init_time_ms: self.init_time.as_millis() as u64,
            config: CollectionConfigTelemetry::from(self.collection_config.read().await.clone()),
            shards: shards_telemetry,
            transfers,
            resharding,
            shard_clean_tasks: (!shard_clean_tasks.is_empty()).then_some(shard_clean_tasks),
        }
    }

    pub async fn get_aggregated_telemetry_data(&self) -> CollectionsAggregatedTelemetry {
        let shards_holder = self.shards_holder.read().await;

        let mut shard_optimization_statuses = Vec::new();
        let mut vectors = 0;

        for shard in shards_holder.all_shards() {
            let shard_optimization_status = shard
                .get_optimization_status()
                .await
                .unwrap_or(OptimizersStatus::Ok);

            shard_optimization_statuses.push(shard_optimization_status);

            vectors += shard.get_size_stats().await.num_vectors;
        }

        let optimizers_status = shard_optimization_statuses
            .into_iter()
            .max()
            .unwrap_or(OptimizersStatus::Ok);

        CollectionsAggregatedTelemetry {
            vectors,
            optimizers_status,
            params: self.collection_config.read().await.params.clone(),
        }
    }

    pub async fn effective_optimizers_config(&self) -> CollectionResult<OptimizersConfig> {
        let config = self.collection_config.read().await;

        if let Some(optimizers_overwrite) = self.optimizers_overwrite.clone() {
            Ok(config.optimizer_config.update(&optimizers_overwrite))
        } else {
            Ok(config.optimizer_config.clone())
        }
    }

    pub async fn lock_updates(&self) -> RwLockWriteGuard<'_, ()> {
        self.updates_lock.write().await
    }

    pub fn wait_collection_initiated(&self, timeout: Duration) -> bool {
        self.is_initialized.await_ready_for_timeout(timeout)
    }

    pub fn request_shard_transfer(&self, shard_transfer: ShardTransfer) {
        self.request_shard_transfer_cb.deref()(shard_transfer)
    }

    pub fn snapshots_path(&self) -> &Path {
        &self.snapshots_path
    }

    pub fn shards_holder(&self) -> Arc<LockedShardHolder> {
        self.shards_holder.clone()
    }

    pub async fn trigger_optimizers(&self) {
        self.shards_holder.read().await.trigger_optimizers().await;
    }

    async fn estimate_collection_size_stats(
        shards_holder: &Arc<RwLock<ShardHolder>>,
    ) -> Option<CollectionSizeStats> {
        let shard_lock = shards_holder.read().await;
        shard_lock.estimate_collection_size_stats().await
    }

    /// Returns estimations of collection sizes. This values are cached and might be not 100% up to date.
    /// The cache gets updated every 32 calls.
    pub(crate) async fn estimated_collection_stats(&self) -> Option<&CollectionSizeAtomicStats> {
        self.collection_stats_cache
            .get_or_update_cache(|| Self::estimate_collection_size_stats(&self.shards_holder))
            .await
    }
}

struct CollectionVersion;

impl StorageVersion for CollectionVersion {
    fn current_raw() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
}
