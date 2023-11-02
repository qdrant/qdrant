mod collection_ops;
mod point_ops;
mod search;
mod shard_transfer;
mod sharding_keys;
mod snapshots;
mod state_management;

use std::collections::HashSet;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use segment::common::version::StorageVersion;
use semver::Version;
use tokio::runtime::Handle;
use tokio::sync::{Mutex, RwLock, RwLockWriteGuard};

use crate::collection_state::{ShardInfo, State};
use crate::common::is_ready::IsReady;
use crate::config::CollectionConfig;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{CollectionError, CollectionResult, NodeType};
use crate::shards::channel_service::ChannelService;
use crate::shards::collection_shard_distribution::CollectionShardDistribution;
use crate::shards::replica_set::ReplicaState::{Active, Dead, Initializing, Listener};
use crate::shards::replica_set::{ChangePeerState, ReplicaState, ShardReplicaSet};
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::{shard_not_found_error, LockedShardHolder, ShardHolder};
use crate::shards::transfer::shard_transfer::{check_transfer_conflicts_strict, ShardTransfer};
use crate::shards::transfer::transfer_tasks_pool::TransferTasksPool;
use crate::shards::{replica_set, CollectionId};
use crate::telemetry::CollectionTelemetry;

/// Collection's data is split into several shards.
pub struct Collection {
    pub(crate) id: CollectionId,
    pub(crate) shards_holder: Arc<LockedShardHolder>,
    pub(crate) collection_config: Arc<RwLock<CollectionConfig>>,
    pub(crate) shared_storage_config: Arc<SharedStorageConfig>,
    this_peer_id: PeerId,
    path: PathBuf,
    snapshots_path: PathBuf,
    channel_service: ChannelService,
    transfer_tasks: Mutex<TransferTasksPool>,
    request_shard_transfer_cb: RequestShardTransfer,
    #[allow(dead_code)] //Might be useful in case of repartition implementation
    notify_peer_failure_cb: ChangePeerState,
    init_time: Duration,
    // One-way boolean flag that is set to true when the collection is fully initialized
    // i.e. all shards are activated for the first time.
    is_initialized: Arc<IsReady>,
    // Lock to temporary block collection update operations while the collection is being migrated.
    // Lock is acquired for read on update operation and can be acquired for write externally,
    // which will block all update operations until the lock is released.
    updates_lock: RwLock<()>,
    // Update runtime handle.
    update_runtime: Handle,
    // Search runtime handle.
    search_runtime: Handle,
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
        collection_config: &CollectionConfig,
        shared_storage_config: Arc<SharedStorageConfig>,
        shard_distribution: CollectionShardDistribution,
        channel_service: ChannelService,
        on_replica_failure: ChangePeerState,
        request_shard_transfer: RequestShardTransfer,
        search_runtime: Option<Handle>,
        update_runtime: Option<Handle>,
    ) -> Result<Self, CollectionError> {
        let start_time = std::time::Instant::now();

        let mut shard_holder = ShardHolder::new(path)?;

        let shared_collection_config = Arc::new(RwLock::new(collection_config.clone()));
        for (shard_id, mut peers) in shard_distribution.shards {
            let is_local = peers.remove(&this_peer_id);

            let replica_set = ShardReplicaSet::build(
                shard_id,
                name.clone(),
                this_peer_id,
                is_local,
                peers,
                on_replica_failure.clone(),
                path,
                shared_collection_config.clone(),
                shared_storage_config.clone(),
                channel_service.clone(),
                update_runtime.clone().unwrap_or_else(Handle::current),
                search_runtime.clone().unwrap_or_else(Handle::current),
            )
            .await?;

            shard_holder.add_shard(shard_id, replica_set, None)?;
        }

        let locked_shard_holder = Arc::new(LockedShardHolder::new(shard_holder));

        // Once the config is persisted - the collection is considered to be successfully created.
        CollectionVersion::save(path)?;
        collection_config.save(path)?;

        Ok(Self {
            id: name.clone(),
            shards_holder: locked_shard_holder,
            collection_config: shared_collection_config,
            shared_storage_config,
            this_peer_id,
            path: path.to_owned(),
            snapshots_path: snapshots_path.to_owned(),
            channel_service,
            transfer_tasks: Mutex::new(TransferTasksPool::new(name.clone())),
            request_shard_transfer_cb: request_shard_transfer.clone(),
            notify_peer_failure_cb: on_replica_failure.clone(),
            init_time: start_time.elapsed(),
            is_initialized: Arc::new(Default::default()),
            updates_lock: RwLock::new(()),
            update_runtime: update_runtime.unwrap_or_else(Handle::current),
            search_runtime: search_runtime.unwrap_or_else(Handle::current),
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
        on_replica_failure: replica_set::ChangePeerState,
        request_shard_transfer: RequestShardTransfer,
        search_runtime: Option<Handle>,
        update_runtime: Option<Handle>,
    ) -> Self {
        let start_time = std::time::Instant::now();
        let stored_version = CollectionVersion::load(path)
            .expect("Can't read collection version")
            .parse()
            .expect("Failed to parse stored collection version as semver");

        let app_version: Version = CollectionVersion::current()
            .parse()
            .expect("Failed to parse current collection version as semver");

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
                panic!("Cannot upgrade version {stored_version} to {app_version}. Try to use older version of Qdrant first.");
            }
        }

        let collection_config = CollectionConfig::load(path).unwrap_or_else(|err| {
            panic!(
                "Can't read collection config due to {}\nat {}",
                err,
                path.to_str().unwrap(),
            )
        });
        collection_config.validate_and_warn();

        let mut shard_holder = ShardHolder::new(path).expect("Can not create shard holder");

        let shared_collection_config = Arc::new(RwLock::new(collection_config.clone()));

        shard_holder
            .load_shards(
                path,
                &collection_id,
                shared_collection_config.clone(),
                shared_storage_config.clone(),
                channel_service.clone(),
                on_replica_failure.clone(),
                this_peer_id,
                update_runtime.clone().unwrap_or_else(Handle::current),
                search_runtime.clone().unwrap_or_else(Handle::current),
            )
            .await;

        let locked_shard_holder = Arc::new(LockedShardHolder::new(shard_holder));

        Self {
            id: collection_id.clone(),
            shards_holder: locked_shard_holder,
            collection_config: shared_collection_config,
            shared_storage_config,
            this_peer_id,
            path: path.to_owned(),
            snapshots_path: snapshots_path.to_owned(),
            channel_service,
            transfer_tasks: Mutex::new(TransferTasksPool::new(collection_id.clone())),
            request_shard_transfer_cb: request_shard_transfer.clone(),
            notify_peer_failure_cb: on_replica_failure,
            init_time: start_time.elapsed(),
            is_initialized: Arc::new(Default::default()),
            updates_lock: RwLock::new(()),
            update_runtime: update_runtime.unwrap_or_else(Handle::current),
            search_runtime: search_runtime.unwrap_or_else(Handle::current),
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

    /// Return a list of local shards, present on this peer
    pub async fn get_local_shards(&self) -> Vec<ShardId> {
        self.shards_holder.read().await.get_local_shards().await
    }

    pub async fn contains_shard(&self, shard_id: ShardId) -> bool {
        self.shards_holder.read().await.contains_shard(&shard_id)
    }

    pub async fn wait_local_shard_replica_state(
        &self,
        shard_id: ShardId,
        state: ReplicaState,
        timeout: Duration,
    ) -> CollectionResult<()> {
        let shard_holder_read = self.shards_holder.read().await;

        let shard = shard_holder_read.get_shard(&shard_id);
        let Some(replica_set) = shard else {
            return Err(CollectionError::NotFound {
                what: "Shard {shard_id}".into(),
            });
        };

        replica_set.wait_for_local_state(state, timeout).await
    }

    pub async fn set_shard_replica_state(
        &self,
        shard_id: ShardId,
        peer_id: PeerId,
        state: ReplicaState,
        from_state: Option<ReplicaState>,
    ) -> CollectionResult<()> {
        let shard_holder = self.shards_holder.read().await;
        let replica_set = shard_holder
            .get_shard(&shard_id)
            .ok_or_else(|| shard_not_found_error(shard_id))?;

        log::debug!(
            "Changing shard {}:{shard_id} replica state from {:?} to {state:?}",
            self.id,
            replica_set.peer_state(&peer_id),
        );

        // Validation:
        // 0. Check that `from_state` matches current state

        if from_state.is_some() {
            let current_state = replica_set.peer_state(&peer_id);
            if current_state != from_state {
                return Err(CollectionError::bad_input(format!(
                    "Replica {peer_id} of shard {shard_id} has state {current_state:?}, but expected {from_state:?}"
                )));
            }
        }

        // 1. Do not deactivate the last active replica

        if state != ReplicaState::Active {
            let active_replicas: HashSet<_> = replica_set
                .peers()
                .into_iter()
                .filter_map(|(peer, state)| {
                    if state == ReplicaState::Active {
                        Some(peer)
                    } else {
                        None
                    }
                })
                .collect();
            if active_replicas.len() == 1 && active_replicas.contains(&peer_id) {
                return Err(CollectionError::bad_input(format!(
                    "Cannot deactivate the last active replica {peer_id} of shard {shard_id}"
                )));
            }
        }

        replica_set
            .ensure_replica_with_state(&peer_id, state)
            .await?;

        if state == ReplicaState::Dead {
            // Terminate transfer if source or target replicas are now dead
            let related_transfers = shard_holder.get_related_transfers(&shard_id, &peer_id);
            for transfer in related_transfers {
                self._abort_shard_transfer(transfer.key(), &shard_holder)
                    .await?;
            }
        }

        if !self.is_initialized.check_ready() {
            // If not initialized yet, we need to check if it was initialized by this call
            let state = self.state().await;
            let mut is_fully_active = true;
            for (_shard_id, shard_info) in state.shards {
                if shard_info
                    .replicas
                    .into_iter()
                    .any(|(_peer_id, state)| state != ReplicaState::Active)
                {
                    is_fully_active = false;
                    break;
                }
            }
            if is_fully_active {
                self.is_initialized.make_ready();
            }
        }

        // Try to request shard transfer if replicas on the current peer are dead
        if state == ReplicaState::Dead && self.this_peer_id == peer_id {
            let transfer_from = replica_set
                .peers()
                .into_iter()
                .find(|(_, state)| state == &ReplicaState::Active)
                .map(|(peer_id, _)| peer_id);
            if let Some(transfer_from) = transfer_from {
                self.request_shard_transfer(ShardTransfer {
                    shard_id,
                    from: transfer_from,
                    to: self.this_peer_id,
                    sync: true,
                    method: None,
                })
            } else {
                log::warn!("No alive replicas to recover shard {shard_id}");
            }
        }

        Ok(())
    }

    pub async fn state(&self) -> State {
        let shards_holder = self.shards_holder.read().await;
        let transfers = shards_holder.shard_transfers.read().clone();
        State {
            config: self.collection_config.read().await.clone(),
            shards: shards_holder
                .get_shards()
                .map(|(shard_id, replicas)| {
                    let shard_info = ShardInfo {
                        replicas: replicas.peers(),
                    };
                    (*shard_id, shard_info)
                })
                .collect(),
            transfers,
            shards_key_mapping: shards_holder.get_shard_key_to_ids_mapping(),
        }
    }

    pub async fn remove_shards_at_peer(&self, peer_id: PeerId) -> CollectionResult<()> {
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
        for replica_set in shard_holder.all_shards() {
            replica_set.sync_local_state().await?;
        }

        // Check for un-reported finished transfers
        let outgoing_transfers = shard_holder
            .get_outgoing_transfers(&self.this_peer_id)
            .await;
        let tasks_lock = self.transfer_tasks.lock().await;
        for transfer in outgoing_transfers {
            match tasks_lock.get_task_result(&transfer.key()) {
                None => {
                    if !tasks_lock.check_if_still_running(&transfer.key()) {
                        log::debug!(
                            "Transfer {:?} does not exist, but not reported as cancelled. Reporting now.",
                            transfer.key()
                        );
                        on_transfer_failure(transfer, self.name(), "transfer task does not exist");
                    }
                }
                Some(true) => {
                    log::debug!(
                        "Transfer {:?} is finished successfully, but not reported. Reporting now.",
                        transfer.key()
                    );
                    on_transfer_success(transfer, self.name());
                }
                Some(false) => {
                    log::debug!(
                        "Transfer {:?} is failed, but not reported as failed. Reporting now.",
                        transfer.key()
                    );
                    on_transfer_failure(transfer, self.name(), "transfer failed");
                }
            }
        }

        // Check for proper replica states
        for replica_set in shard_holder.all_shards() {
            let this_peer_id = &replica_set.this_peer_id();
            let shard_id = replica_set.shard_id;

            let peers = replica_set.peers();
            let this_peer_state = peers.get(this_peer_id).copied();
            let is_last_active = peers.values().filter(|state| **state == Active).count() == 1;

            if this_peer_state == Some(Initializing) {
                // It is possible, that collection creation didn't report
                // Try to activate shard, as the collection clearly exists
                on_finish_init(*this_peer_id, shard_id);
                continue;
            }

            if self.shared_storage_config.node_type == NodeType::Listener {
                if this_peer_state == Some(Active) && !is_last_active {
                    // Convert active node from active to listener
                    on_convert_to_listener(*this_peer_id, shard_id);
                    continue;
                }
            } else if this_peer_state == Some(Listener) {
                // Convert listener node to active
                on_convert_from_listener(*this_peer_id, shard_id);
                continue;
            }

            if this_peer_state != Some(Dead) || replica_set.is_dummy().await {
                continue; // All good
            }

            // Try to find dead replicas with no active transfers
            let transfers = shard_holder.get_transfers(|_| true).await;

            // Try to find a replica to transfer from
            for replica_id in replica_set.active_remote_shards().await {
                let transfer = ShardTransfer {
                    from: replica_id,
                    to: *this_peer_id,
                    shard_id,
                    sync: true,
                    method: None,
                };
                if check_transfer_conflicts_strict(&transfer, transfers.iter()).is_some() {
                    continue; // this transfer won't work
                }
                log::debug!(
                    "Recovering shard {}:{} on peer {} by requesting it from {}",
                    self.name(),
                    shard_id,
                    this_peer_id,
                    replica_id
                );
                self.request_shard_transfer(transfer);
                break;
            }
        }

        Ok(())
    }

    pub async fn get_telemetry_data(&self) -> CollectionTelemetry {
        let (shards_telemetry, transfers) = {
            let mut shards_telemetry = Vec::new();
            let shards_holder = self.shards_holder.read().await;
            for shard in shards_holder.all_shards() {
                shards_telemetry.push(shard.get_telemetry_data().await)
            }
            (shards_telemetry, shards_holder.get_shard_transfer_info())
        };

        CollectionTelemetry {
            id: self.name(),
            init_time_ms: self.init_time.as_millis() as u64,
            config: self.collection_config.read().await.clone(),
            shards: shards_telemetry,
            transfers,
        }
    }

    pub async fn lock_updates(&self) -> RwLockWriteGuard<()> {
        self.updates_lock.write().await
    }

    pub fn wait_collection_initiated(&self, timeout: Duration) -> bool {
        self.is_initialized.await_ready_for_timeout(timeout)
    }

    pub fn request_shard_transfer(&self, shard_transfer: ShardTransfer) {
        self.request_shard_transfer_cb.deref()(shard_transfer)
    }
}

struct CollectionVersion;

impl StorageVersion for CollectionVersion {
    fn current() -> String {
        env!("CARGO_PKG_VERSION").to_string()
    }
}
