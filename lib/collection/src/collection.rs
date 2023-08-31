use std::cmp::max;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use futures::future::{join_all, try_join_all};
use itertools::Itertools;
use segment::common::version::StorageVersion;
use segment::spaces::tools::{peek_top_largest_iterable, peek_top_smallest_iterable};
use segment::types::{
    ExtendedPointId, Order, QuantizationConfig, ScoredPoint, WithPayload, WithPayloadInterface,
    WithVector,
};
use semver::Version;
use tar::Builder as TarBuilder;
use tokio::fs::{copy, create_dir_all, remove_dir_all, remove_file, rename};
use tokio::runtime::Handle;
use tokio::sync::{Mutex, RwLock, RwLockWriteGuard};
use validator::Validate;

use crate::collection_state::{ShardInfo, State};
use crate::common::is_ready::IsReady;
use crate::config::CollectionConfig;
use crate::hash_ring::HashRing;
use crate::operations::config_diff::{
    CollectionParamsDiff, DiffConfig, HnswConfigDiff, OptimizersConfigDiff, QuantizationConfigDiff,
};
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::point_ops::WriteOrdering;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::snapshot_ops::{
    get_snapshot_description, list_snapshots_in_directory, SnapshotDescription,
};
use crate::operations::types::{
    CollectionClusterInfo, CollectionError, CollectionInfo, CollectionResult, CountRequest,
    CountResult, LocalShardInfo, NodeType, PointRequest, Record, RemoteShardInfo, ScrollRequest,
    ScrollResult, SearchRequest, SearchRequestBatch, UpdateResult, VectorsConfigDiff,
};
use crate::operations::CollectionUpdateOperations;
use crate::optimizers_builder::OptimizersConfig;
use crate::shards::channel_service::ChannelService;
use crate::shards::collection_shard_distribution::CollectionShardDistribution;
use crate::shards::local_shard::LocalShard;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::replica_set::ReplicaState::{Active, Dead, Initializing, Listener};
use crate::shards::replica_set::{
    Change, ChangePeerState, ReplicaState, ShardReplicaSet as ReplicaSetShard,
}; // TODO rename ReplicaShard to ReplicaSetShard
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_config::{self, ShardConfig};
use crate::shards::shard_holder::{LockedShardHolder, ShardHolder};
use crate::shards::shard_versioning::versioned_shard_path;
use crate::shards::transfer::shard_transfer::{
    change_remote_shard_route, check_transfer_conflicts_strict, finalize_partial_shard,
    handle_transferred_shard_proxy, revert_proxy_shard_to_local, spawn_transfer_task,
    ShardTransfer, ShardTransferKey,
};
use crate::shards::transfer::transfer_tasks_pool::{TaskResult, TransferTasksPool};
use crate::shards::{replica_set, CollectionId, HASH_RING_SHARD_SCALE};
use crate::telemetry::CollectionTelemetry;

pub type VectorLookupFuture<'a> = Box<dyn Future<Output = CollectionResult<Vec<Record>>> + 'a>;
pub type OnTransferFailure = Arc<dyn Fn(ShardTransfer, CollectionId, &str) + Send + Sync>;
pub type OnTransferSuccess = Arc<dyn Fn(ShardTransfer, CollectionId) + Send + Sync>;
pub type RequestShardTransfer = Arc<dyn Fn(ShardTransfer) + Send + Sync>;

struct CollectionVersion;

impl StorageVersion for CollectionVersion {
    fn current() -> String {
        env!("CARGO_PKG_VERSION").to_string()
    }
}

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
}

impl Collection {
    pub fn name(&self) -> String {
        self.id.clone()
    }

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

        let mut shard_holder = ShardHolder::new(path, HashRing::fair(HASH_RING_SHARD_SCALE))?;

        let shared_collection_config = Arc::new(RwLock::new(collection_config.clone()));
        for (shard_id, mut peers) in shard_distribution.shards {
            let is_local = peers.remove(&this_peer_id);

            let replica_set = ReplicaSetShard::build(
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

            shard_holder.add_shard(shard_id, replica_set);
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
        })
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

        let ring = HashRing::fair(HASH_RING_SHARD_SCALE);
        let mut shard_holder = ShardHolder::new(path, ring).expect("Can not create shard holder");

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
        }
    }

    /// Return a list of local shards, present on this peer
    pub async fn get_local_shards(&self) -> Vec<ShardId> {
        let shards_holder = self.shards_holder.read().await;
        let mut res = vec![];
        for (shard_id, replica_set) in shards_holder.get_shards() {
            if replica_set.has_local_shard().await {
                res.push(*shard_id);
            }
        }
        res
    }

    pub async fn is_all_active(&self) -> bool {
        let shards_holder = self.shards_holder.read().await;
        for (_, replica_set) in shards_holder.get_shards() {
            if !replica_set
                .peers()
                .into_iter()
                .all(|(_, state)| state == ReplicaState::Active)
            {
                return false;
            }
        }
        true
    }

    pub async fn set_shard_replica_state(
        &self,
        shard_id: ShardId,
        peer_id: PeerId,
        state: ReplicaState,
        from_state: Option<ReplicaState>,
    ) -> CollectionResult<()> {
        let shard_holder = self.shards_holder.read().await;
        let replica_set =
            shard_holder
                .get_shard(&shard_id)
                .ok_or_else(|| CollectionError::NotFound {
                    what: format!("Shard {shard_id}"),
                })?;

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
                })
            } else {
                log::warn!("No alive replicas to recover shard {shard_id}");
            }
        }

        Ok(())
    }

    pub async fn contains_shard(&self, shard_id: ShardId) -> bool {
        let shard_holder_read = self.shards_holder.read().await;
        shard_holder_read.contains_shard(&shard_id)
    }

    /// Returns true if shard it explicitly local, false otherwise.
    pub async fn is_shard_local(&self, shard_id: &ShardId) -> Option<bool> {
        let shard_holder_read = self.shards_holder.read().await;
        if let Some(shard) = shard_holder_read.get_shard(shard_id) {
            Some(shard.is_local().await)
        } else {
            None
        }
    }

    pub async fn check_transfer_exists(&self, transfer_key: &ShardTransferKey) -> bool {
        let shard_holder_read = self.shards_holder.read().await;
        let matched = shard_holder_read
            .shard_transfers
            .read()
            .iter()
            .any(|transfer| transfer_key.check(transfer));
        matched
    }

    pub async fn get_transfer(&self, transfer_key: &ShardTransferKey) -> Option<ShardTransfer> {
        let shard_holder_read = self.shards_holder.read().await;
        let transfer = shard_holder_read
            .shard_transfers
            .read()
            .iter()
            .find(|transfer| transfer_key.check(transfer))
            .cloned();
        transfer
    }

    pub async fn get_outgoing_transfers(&self, current_peer_id: &PeerId) -> Vec<ShardTransfer> {
        self.get_transfers(|transfer| transfer.from == *current_peer_id)
            .await
    }

    pub async fn get_transfers<F>(&self, mut predicate: F) -> Vec<ShardTransfer>
    where
        F: FnMut(&ShardTransfer) -> bool,
    {
        let shard_holder = self.shards_holder.read().await;
        let transfers = shard_holder
            .shard_transfers
            .read()
            .iter()
            .filter(|&transfer| predicate(transfer))
            .cloned()
            .collect();
        transfers
    }

    async fn send_shard<OF, OE>(&self, transfer: ShardTransfer, on_finish: OF, on_error: OE)
    where
        OF: Future<Output = ()> + Send + 'static,
        OE: Future<Output = ()> + Send + 'static,
    {
        let mut active_transfer_tasks = self.transfer_tasks.lock().await;
        let task_result = active_transfer_tasks.stop_if_exists(&transfer.key()).await;

        debug_assert_eq!(task_result, TaskResult::NotFound);

        let shard_holder = self.shards_holder.clone();
        let collection_id = self.id.clone();
        let channel_service = self.channel_service.clone();

        let transfer_task = spawn_transfer_task(
            shard_holder,
            transfer.clone(),
            collection_id,
            channel_service,
            on_finish,
            on_error,
        );

        active_transfer_tasks.add_task(&transfer, transfer_task);
    }

    pub async fn start_shard_transfer<T, F>(
        &self,
        shard_transfer: ShardTransfer,
        on_finish: T,
        on_error: F,
    ) -> CollectionResult<bool>
    where
        T: Future<Output = ()> + Send + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        let shard_id = shard_transfer.shard_id;
        let do_transfer = {
            let shards_holder = self.shards_holder.read().await;
            let _was_not_transferred =
                shards_holder.register_start_shard_transfer(shard_transfer.clone())?;
            let replica_set_opt = shards_holder.get_shard(&shard_id);

            // Check if current node owns the shard which should be transferred
            // and therefore able to transfer
            let replica_set = if let Some(replica_set) = replica_set_opt {
                replica_set
            } else {
                // Service error, because it means the validation was incorrect
                return Err(CollectionError::service_error(format!(
                    "Shard {shard_id} doesn't exist"
                )));
            };
            // Set learning replica state on all peers
            // This should disable queries to learning replica even if it was active
            replica_set.set_replica_state(&shard_transfer.to, ReplicaState::Partial)?;
            replica_set.is_local().await && replica_set.this_peer_id() == shard_transfer.from
        };
        if do_transfer {
            self.send_shard(shard_transfer, on_finish, on_error).await;
        }
        Ok(do_transfer)
    }

    /// Handles finishing of the shard transfer.
    ///
    /// Returns true if state was changed, false otherwise.
    pub async fn finish_shard_transfer(&self, transfer: ShardTransfer) -> CollectionResult<()> {
        let transfer_finished = self
            .transfer_tasks
            .lock()
            .await
            .stop_if_exists(&transfer.key())
            .await
            .is_finished();
        log::debug!("transfer_finished: {}", transfer_finished);

        let shards_holder_guard = self.shards_holder.read().await;

        // Should happen on transfer side
        // Unwrap forward proxy into local shard, or replace it with remote shard
        // depending on the `sync` flag.
        if self.this_peer_id == transfer.from {
            let proxy_promoted = handle_transferred_shard_proxy(
                &shards_holder_guard,
                transfer.shard_id,
                transfer.to,
                transfer.sync,
            )
            .await?;
            log::debug!("proxy_promoted: {}", proxy_promoted);
        }

        // Should happen on receiving side
        // Promote partial shard to active shard
        if self.this_peer_id == transfer.to {
            let shard_promoted =
                finalize_partial_shard(&shards_holder_guard, transfer.shard_id).await?;
            log::debug!(
                "shard_promoted: {}, shard_id: {}, peer_id: {}",
                shard_promoted,
                transfer.shard_id,
                self.this_peer_id
            );
        }

        // Should happen on a third-party side
        // Change direction of the remote shards or add a new remote shard
        if self.this_peer_id != transfer.from {
            let remote_shard_rerouted = change_remote_shard_route(
                &shards_holder_guard,
                transfer.shard_id,
                transfer.from,
                transfer.to,
                transfer.sync,
            )
            .await?;
            log::debug!("remote_shard_rerouted: {}", remote_shard_rerouted);
        }
        let finish_was_registered =
            shards_holder_guard.register_finish_transfer(&transfer.key())?;
        log::debug!("finish_was_registered: {}", finish_was_registered);
        Ok(())
    }

    async fn _abort_shard_transfer(
        &self,
        transfer_key: ShardTransferKey,
        shard_holder_guard: &ShardHolder,
    ) -> CollectionResult<()> {
        let _transfer_finished = self
            .transfer_tasks
            .lock()
            .await
            .stop_if_exists(&transfer_key)
            .await
            .is_finished();

        let replica_set =
            if let Some(replica_set) = shard_holder_guard.get_shard(&transfer_key.shard_id) {
                replica_set
            } else {
                return Err(CollectionError::bad_request(format!(
                    "Shard {} doesn't exist",
                    transfer_key.shard_id
                )));
            };

        let transfer = self.get_transfer(&transfer_key).await;

        if transfer.map(|x| x.sync).unwrap_or(false) {
            replica_set.set_replica_state(&transfer_key.to, ReplicaState::Dead)?;
        } else {
            replica_set.remove_peer(transfer_key.to).await?;
        }

        if self.this_peer_id == transfer_key.from {
            revert_proxy_shard_to_local(shard_holder_guard, transfer_key.shard_id).await?;
        }

        let _finish_was_registered = shard_holder_guard.register_finish_transfer(&transfer_key)?;

        Ok(())
    }

    /// Handles abort of the transfer
    ///
    /// 1. Unregister the transfer
    /// 2. Stop transfer task
    /// 3. Unwrap the proxy
    /// 4. Remove temp shard, or mark it as dead
    pub async fn abort_shard_transfer(
        &self,
        transfer_key: ShardTransferKey,
    ) -> CollectionResult<()> {
        let shard_holder_guard = self.shards_holder.read().await;
        // Internal implementation, used to prevents double-read deadlock
        self._abort_shard_transfer(transfer_key, &shard_holder_guard)
            .await
    }

    /// Initiate local partial shard
    pub async fn initiate_local_partial_shard(&self, shard_id: ShardId) -> CollectionResult<()> {
        let shards_holder = self.shards_holder.read().await;
        let replica_set = match shards_holder.get_shard(&shard_id) {
            None => {
                return Err(CollectionError::service_error(format!(
                    "Shard {shard_id} doesn't exist, repartition is not supported yet"
                )))
            }
            Some(replica_set) => replica_set,
        };

        if !replica_set.has_local_shard().await {
            // create local shard
            let shard = LocalShard::build(
                shard_id,
                self.name(),
                &replica_set.shard_path,
                self.collection_config.clone(),
                self.shared_storage_config.clone(),
                self.update_runtime.clone(),
            )
            .await?;

            replica_set
                .set_local(shard, Some(ReplicaState::Partial))
                .await?;
        } else {
            if replica_set.is_dummy().await {
                return Err(CollectionError::service_error(format!(
                    "Shard {shard_id} is a \"dummy\" shard"
                )));
            } else if !replica_set.is_local().await {
                // We have proxy or something, we need to unwrap it
                log::warn!("Unwrapping proxy shard {}", shard_id);
                replica_set.un_proxify_local().await?
            }
            replica_set.set_replica_state(&replica_set.this_peer_id(), ReplicaState::Partial)?;
        }
        Ok(())
    }

    /// Handle collection updates from peers.
    ///
    /// Shard transfer aware.
    pub async fn update_from_peer(
        &self,
        operation: CollectionUpdateOperations,
        shard_selection: ShardId,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        let _update_lock = self.updates_lock.read().await;
        let shard_holder_guard = self.shards_holder.read().await;

        let res = match shard_holder_guard.get_shard(&shard_selection) {
            None => None,
            Some(target_shard) => target_shard.update_local(operation.clone(), wait).await?,
        };

        if let Some(res) = res {
            Ok(res)
        } else {
            Err(CollectionError::service_error(format!(
                "No target shard {shard_selection} found for update"
            )))
        }
    }

    pub async fn update_from_client(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
        ordering: WriteOrdering,
    ) -> CollectionResult<UpdateResult> {
        operation.validate()?;
        let _update_lock = self.updates_lock.read().await;

        let mut results = {
            let shards_holder = self.shards_holder.read().await;
            let shard_to_op = shards_holder.split_by_shard(operation);

            if shard_to_op.is_empty() {
                return Err(CollectionError::bad_request(
                    "Empty update request".to_string(),
                ));
            }

            let shard_requests = shard_to_op
                .into_iter()
                .map(move |(replica_set, operation)| {
                    replica_set.update_with_consistency(operation, wait, ordering)
                });
            join_all(shard_requests).await
        };

        let with_error = results
            .iter()
            .filter(|result| matches!(result, Err(_)))
            .count();

        // one request per shard
        let result_len = results.len();

        if with_error > 0 {
            let first_err = results
                .into_iter()
                .find(|result| matches!(result, Err(_)))
                .unwrap();
            // inconsistent if only a subset of the requests fail - one request per shard.
            if with_error < result_len {
                first_err.map_err(|err| {
                    // compute final status code based on the first error
                    // e.g. a partially successful batch update failing because of bad input is a client error
                    CollectionError::InconsistentShardFailure {
                        shards_total: result_len as u32, // report only the number of shards that took part in the update
                        shards_failed: with_error as u32,
                        first_err: Box::new(err),
                    }
                })
            } else {
                // all requests per shard failed - propagate first error (assume there are all the same)
                first_err
            }
        } else {
            // At least one result is always present.
            results.pop().unwrap()
        }
    }

    pub async fn search_batch(
        &self,
        request: SearchRequestBatch,
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        // shortcuts batch if all requests with limit=0
        if request.searches.iter().all(|s| s.limit == 0) {
            return Ok(vec![]);
        }
        // A factor which determines if we need to use the 2-step search or not
        // Should be adjusted based on usage statistics.
        const PAYLOAD_TRANSFERS_FACTOR_THRESHOLD: usize = 10;

        let is_payload_required = request.searches.iter().all(|s| {
            s.with_payload
                .clone()
                .map(|p| p.is_required())
                .unwrap_or_default()
        });
        let with_vectors = request.searches.iter().all(|s| {
            s.with_vector
                .as_ref()
                .map(|wv| wv.is_some())
                .unwrap_or(false)
        });

        let metadata_required = is_payload_required || with_vectors;

        let sum_limits: usize = request.searches.iter().map(|s| s.limit).sum();
        let sum_offsets: usize = request.searches.iter().map(|s| s.offset).sum();

        // Number of records we need to retrieve to fill the search result.
        let require_transfers = self.shards_holder.read().await.len() * (sum_limits + sum_offsets);
        // Actually used number of records.
        let used_transfers = sum_limits;

        let is_required_transfer_large_enough =
            require_transfers > used_transfers * PAYLOAD_TRANSFERS_FACTOR_THRESHOLD;

        if metadata_required && is_required_transfer_large_enough {
            // If there is a significant offset, we need to retrieve the whole result
            // set without payload first and then retrieve the payload.
            // It is required to do this because the payload might be too large to send over the
            // network.
            let mut without_payload_requests = Vec::with_capacity(request.searches.len());
            for search in &request.searches {
                let mut without_payload_request = search.clone();
                without_payload_request.with_payload = None;
                without_payload_request.with_vector = None;
                without_payload_requests.push(without_payload_request);
            }
            let without_payload_batch = SearchRequestBatch {
                searches: without_payload_requests,
            };
            let without_payload_results = self
                ._search_batch(without_payload_batch, read_consistency, shard_selection)
                .await?;
            let filled_results = without_payload_results
                .into_iter()
                .zip(request.clone().searches.into_iter())
                .map(|(without_payload_result, req)| {
                    self.fill_search_result_with_payload(
                        without_payload_result,
                        req.with_payload.clone(),
                        req.with_vector.unwrap_or_default(),
                        read_consistency,
                        shard_selection,
                    )
                });
            try_join_all(filled_results).await
        } else {
            let result = self
                ._search_batch(request, read_consistency, shard_selection)
                .await?;
            Ok(result)
        }
    }

    async fn _search_batch(
        &self,
        request: SearchRequestBatch,
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let batch_size = request.searches.len();
        let request = Arc::new(request);

        // query all shards concurrently
        let mut all_searches_res = {
            let shard_holder = self.shards_holder.read().await;
            let target_shards = shard_holder.target_shard(shard_selection)?;
            let all_searches = target_shards
                .iter()
                .map(|shard| shard.search(request.clone(), read_consistency));
            try_join_all(all_searches).await?
        };

        // merge results from shards in order
        let mut merged_results: Vec<Vec<ScoredPoint>> = vec![vec![]; batch_size];
        for shard_searches_results in all_searches_res.iter_mut() {
            for (index, shard_searches_result) in shard_searches_results.iter_mut().enumerate() {
                merged_results[index].append(shard_searches_result)
            }
        }
        let collection_params = self.collection_config.read().await.params.clone();
        let top_results: Vec<_> = merged_results
            .into_iter()
            .zip(request.searches.iter())
            .map(|(res, request)| {
                let distance = collection_params
                    .get_vector_params(request.vector.get_name())?
                    .distance;
                let mut top_res = match distance.distance_order() {
                    Order::LargeBetter => {
                        peek_top_largest_iterable(res, request.limit + request.offset)
                    }
                    Order::SmallBetter => {
                        peek_top_smallest_iterable(res, request.limit + request.offset)
                    }
                };
                // Remove `offset` from top result only for client requests
                // to avoid applying `offset` twice in distributed mode.
                if shard_selection.is_none() && request.offset > 0 {
                    if top_res.len() >= request.offset {
                        // Panics if the end point > length of the vector.
                        top_res.drain(..request.offset);
                    } else {
                        top_res.clear()
                    }
                }
                Ok(top_res)
            })
            .collect::<CollectionResult<Vec<_>>>()?;

        Ok(top_results)
    }

    pub(crate) async fn fill_search_result_with_payload(
        &self,
        search_result: Vec<ScoredPoint>,
        with_payload: Option<WithPayloadInterface>,
        with_vector: WithVector,
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        // short-circuit if not needed
        if let (&Some(WithPayloadInterface::Bool(false)), &WithVector::Bool(false)) =
            (&with_payload, &with_vector)
        {
            return Ok(search_result
                .into_iter()
                .map(|point| ScoredPoint {
                    payload: None,
                    vector: None,
                    ..point
                })
                .collect());
        };

        let retrieve_request = PointRequest {
            ids: search_result.iter().map(|x| x.id).collect(),
            with_payload,
            with_vector,
        };
        let retrieved_records = self
            .retrieve(retrieve_request, read_consistency, shard_selection)
            .await?;
        let mut records_map: HashMap<ExtendedPointId, Record> = retrieved_records
            .into_iter()
            .map(|rec| (rec.id, rec))
            .collect();
        let enriched_result = search_result
            .into_iter()
            .filter_map(|mut scored_point| {
                // Points might get deleted between search and retrieve.
                // But it's not a problem, because we don't want to return deleted points.
                // So we just filter out them.
                records_map.remove(&scored_point.id).map(|record| {
                    scored_point.payload = record.payload;
                    scored_point.vector = record.vector;
                    scored_point
                })
            })
            .collect();
        Ok(enriched_result)
    }

    pub async fn search(
        &self,
        request: SearchRequest,
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        if request.limit == 0 {
            return Ok(vec![]);
        }
        // search is a special case of search_batch with a single batch
        let request_batch = SearchRequestBatch {
            searches: vec![request],
        };
        let results = self
            ._search_batch(request_batch, read_consistency, shard_selection)
            .await?;
        Ok(results.into_iter().next().unwrap())
    }

    pub async fn scroll_by(
        &self,
        request: ScrollRequest,
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
    ) -> CollectionResult<ScrollResult> {
        let default_request = ScrollRequest::default();

        let offset = request.offset;
        let limit = request
            .limit
            .unwrap_or_else(|| default_request.limit.unwrap());
        let with_payload_interface = request
            .with_payload
            .clone()
            .unwrap_or_else(|| default_request.with_payload.clone().unwrap());
        let with_vector = request.with_vector;

        if limit == 0 {
            return Err(CollectionError::BadRequest {
                description: "Limit cannot be 0".to_string(),
            });
        }

        // Needed to return next page offset.
        let limit = limit + 1;
        let retrieved_points: Vec<_> = {
            let shards_holder = self.shards_holder.read().await;
            let target_shards = shards_holder.target_shard(shard_selection)?;
            let scroll_futures = target_shards.into_iter().map(|shard| {
                shard.scroll_by(
                    offset,
                    limit,
                    &with_payload_interface,
                    &with_vector,
                    request.filter.as_ref(),
                    read_consistency,
                )
            });

            try_join_all(scroll_futures).await?
        };
        let mut points: Vec<_> = retrieved_points
            .into_iter()
            .flatten()
            .sorted_by_key(|point| point.id)
            .take(limit)
            .collect();

        let next_page_offset = if points.len() < limit {
            // This was the last page
            None
        } else {
            // remove extra point, it would be a first point of the next page
            Some(points.pop().unwrap().id)
        };
        Ok(ScrollResult {
            points,
            next_page_offset,
        })
    }

    pub async fn count(
        &self,
        request: CountRequest,
        shard_selection: Option<ShardId>,
    ) -> CollectionResult<CountResult> {
        let request = Arc::new(request);

        let counts: Vec<_> = {
            let shards_holder = self.shards_holder.read().await;
            let target_shards = shards_holder.target_shard(shard_selection)?;
            let count_futures = target_shards
                .into_iter()
                .map(|shard| shard.count(request.clone()));
            try_join_all(count_futures).await?.into_iter().collect()
        };

        let total_count = counts.iter().map(|x| x.count).sum::<usize>();
        let aggregated_count = CountResult { count: total_count };
        Ok(aggregated_count)
    }

    pub async fn retrieve(
        &self,
        request: PointRequest,
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
    ) -> CollectionResult<Vec<Record>> {
        let with_payload_interface = request
            .with_payload
            .as_ref()
            .unwrap_or(&WithPayloadInterface::Bool(false));
        let with_payload = WithPayload::from(with_payload_interface);
        let request = Arc::new(request);
        let all_shard_collection_results = {
            let shard_holder = self.shards_holder.read().await;
            let target_shards = shard_holder.target_shard(shard_selection)?;
            let retrieve_futures = target_shards.into_iter().map(|shard| {
                shard.retrieve(
                    request.clone(),
                    &with_payload,
                    &request.with_vector,
                    read_consistency,
                )
            });
            try_join_all(retrieve_futures).await?
        };
        let points = all_shard_collection_results.into_iter().flatten().collect();
        Ok(points)
    }

    /// Updates collection params:
    /// Saves new params on disk
    ///
    /// After this, `recreate_optimizers_blocking` must be called to create new optimizers using
    /// the updated configuration.
    pub async fn update_params_from_diff(
        &self,
        params_diff: CollectionParamsDiff,
    ) -> CollectionResult<()> {
        {
            let mut config = self.collection_config.write().await;
            config.params = params_diff.update(&config.params)?;
        }
        self.collection_config.read().await.save(&self.path)?;
        Ok(())
    }

    /// Updates HNSW config:
    /// Saves new params on disk
    ///
    /// After this, `recreate_optimizers_blocking` must be called to create new optimizers using
    /// the updated configuration.
    pub async fn update_hnsw_config_from_diff(
        &self,
        hnsw_config_diff: HnswConfigDiff,
    ) -> CollectionResult<()> {
        {
            let mut config = self.collection_config.write().await;
            config.hnsw_config = hnsw_config_diff.update(&config.hnsw_config)?;
        }
        self.collection_config.read().await.save(&self.path)?;
        Ok(())
    }

    /// Updates quantization config:
    /// Saves new params on disk
    ///
    /// After this, `recreate_optimizers_blocking` must be called to create new optimizers using
    /// the updated configuration.
    pub async fn update_quantization_config_from_diff(
        &self,
        quantization_config_diff: QuantizationConfigDiff,
    ) -> CollectionResult<()> {
        {
            let mut config = self.collection_config.write().await;
            match quantization_config_diff {
                QuantizationConfigDiff::Scalar(scalar) => {
                    config
                        .quantization_config
                        .replace(QuantizationConfig::Scalar(scalar));
                }
                QuantizationConfigDiff::Product(product) => {
                    config
                        .quantization_config
                        .replace(QuantizationConfig::Product(product));
                }
                QuantizationConfigDiff::Disabled(_) => {
                    config.quantization_config = None;
                }
            }
        }
        self.collection_config.read().await.save(&self.path)?;
        Ok(())
    }

    /// Updates vectors config:
    /// Saves new params on disk
    ///
    /// After this, `recreate_optimizers_blocking` must be called to create new optimizers using
    /// the updated configuration.
    pub async fn update_vectors_from_diff(
        &self,
        update_vectors_diff: &VectorsConfigDiff,
    ) -> CollectionResult<()> {
        let mut config = self.collection_config.write().await;
        update_vectors_diff.check_vector_names(&config.params)?;
        config
            .params
            .update_vectors_from_diff(update_vectors_diff)?;
        config.save(&self.path)?;
        Ok(())
    }

    pub fn request_shard_transfer(&self, shard_transfer: ShardTransfer) {
        self.request_shard_transfer_cb.deref()(shard_transfer)
    }

    /// Handle replica changes
    ///
    /// add and remove replicas from replica set
    pub async fn handle_replica_changes(
        &self,
        replica_changes: Vec<Change>,
    ) -> CollectionResult<()> {
        if replica_changes.is_empty() {
            return Ok(());
        }
        let read_shard_holder = self.shards_holder.read().await;

        for change in replica_changes {
            match change {
                Change::Remove(shard_id, peer_id) => {
                    let replica_set_opt = read_shard_holder.get_shard(&shard_id);
                    let replica_set = if let Some(replica_set) = replica_set_opt {
                        replica_set
                    } else {
                        return Err(CollectionError::BadRequest {
                            description: format!("Shard {} of {} not found", shard_id, self.name()),
                        });
                    };

                    let peers = replica_set.peers();

                    if !peers.contains_key(&peer_id) {
                        return Err(CollectionError::BadRequest {
                            description: format!(
                                "Peer {peer_id} has no replica of shard {shard_id}"
                            ),
                        });
                    }

                    if peers.len() == 1 {
                        return Err(CollectionError::BadRequest {
                            description: format!("Shard {shard_id} must have at least one replica"),
                        });
                    }

                    replica_set.remove_peer(peer_id).await?;
                }
            }
        }
        Ok(())
    }

    /// Updates shard optimization params:
    /// Saves new params on disk
    ///
    /// After this, `recreate_optimizers_blocking` must be called to create new optimizers using
    /// the updated configuration.
    pub async fn update_optimizer_params_from_diff(
        &self,
        optimizer_config_diff: OptimizersConfigDiff,
    ) -> CollectionResult<()> {
        {
            let mut config = self.collection_config.write().await;
            config.optimizer_config =
                DiffConfig::update(optimizer_config_diff, &config.optimizer_config)?;
        }
        self.collection_config.read().await.save(&self.path)?;
        Ok(())
    }

    /// Updates shard optimization params: Saves new params on disk
    ///
    /// After this, `recreate_optimizers_blocking` must be called to create new optimizers using
    /// the updated configuration.
    pub async fn update_optimizer_params(
        &self,
        optimizer_config: OptimizersConfig,
    ) -> CollectionResult<()> {
        {
            let mut config = self.collection_config.write().await;
            config.optimizer_config = optimizer_config;
        }
        self.collection_config.read().await.save(&self.path)?;
        Ok(())
    }

    /// Recreate the optimizers on all shards for this collection
    ///
    /// This will stop existing optimizers, and start new ones with new configurations.
    ///
    /// # Blocking
    ///
    /// Partially blocking. Stopping existing optimizers is blocking. Starting new optimizers is
    /// not blocking.
    pub async fn recreate_optimizers_blocking(&self) -> CollectionResult<()> {
        let shard_holder = self.shards_holder.read().await;
        let updates = shard_holder
            .all_shards()
            .map(|replica_set| replica_set.on_optimizer_config_update());
        try_join_all(updates).await?;
        Ok(())
    }

    pub async fn info(&self, shard_selection: Option<ShardId>) -> CollectionResult<CollectionInfo> {
        let (all_shard_collection_results, mut info) = {
            let shards_holder = self.shards_holder.read().await;

            let target_shards = shards_holder.target_shard(shard_selection)?;

            let first_shard = *target_shards.first().ok_or_else(|| {
                CollectionError::service_error(
                    "There are no shards for selected collection".to_string(),
                )
            })?;

            let info = first_shard.info().await?;
            let info_futures = target_shards.into_iter().skip(1).map(|shard| shard.info());

            (try_join_all(info_futures).await?, info)
        };

        all_shard_collection_results
            .into_iter()
            .for_each(|shard_info| {
                info.status = max(info.status, shard_info.status);
                info.optimizer_status =
                    max(info.optimizer_status.clone(), shard_info.optimizer_status);
                info.vectors_count += shard_info.vectors_count;
                info.indexed_vectors_count += shard_info.indexed_vectors_count;
                info.points_count += shard_info.points_count;
                info.segments_count += shard_info.segments_count;
                for (key, schema) in shard_info.payload_schema {
                    match info.payload_schema.entry(key) {
                        Entry::Occupied(o) => {
                            o.into_mut().points += schema.points;
                        }
                        Entry::Vacant(v) => {
                            v.insert(schema);
                        }
                    };
                }
            });
        Ok(info)
    }

    pub async fn cluster_info(&self, peer_id: PeerId) -> CollectionResult<CollectionClusterInfo> {
        let shards_holder = self.shards_holder.read().await;
        let shard_count = shards_holder.len();
        let mut local_shards = Vec::new();
        let mut remote_shards = Vec::new();
        let count_request = Arc::new(CountRequest {
            filter: None,
            exact: false, // Don't need exact count of unique ids here, only size estimation
        });
        // extract shards info
        for (shard_id, replica_set) in shards_holder.get_shards() {
            let shard_id = *shard_id;
            let peers = replica_set.peers();

            if replica_set.has_local_shard().await {
                let state = peers
                    .get(&replica_set.this_peer_id())
                    .copied()
                    .unwrap_or(ReplicaState::Dead);
                let count_result = replica_set.count_local(count_request.clone()).await?;
                let points_count = count_result.map(|x| x.count).unwrap_or(0);
                local_shards.push(LocalShardInfo {
                    shard_id,
                    points_count,
                    state,
                })
            }
            for (peer_id, state) in replica_set.peers().into_iter() {
                if peer_id == replica_set.this_peer_id() {
                    continue;
                }
                remote_shards.push(RemoteShardInfo {
                    shard_id,
                    peer_id,
                    state,
                });
            }
        }
        let shard_transfers = shards_holder.get_shard_transfer_info();

        // sort by shard_id
        local_shards.sort_by_key(|k| k.shard_id);
        remote_shards.sort_by_key(|k| k.shard_id);

        let info = CollectionClusterInfo {
            peer_id,
            shard_count,
            local_shards,
            remote_shards,
            shard_transfers,
        };
        Ok(info)
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
        }
    }

    pub async fn apply_state(
        &self,
        state: State,
        this_peer_id: PeerId,
        abort_transfer: impl FnMut(ShardTransfer),
    ) -> CollectionResult<()> {
        state.apply(this_peer_id, self, abort_transfer).await
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

    pub async fn list_snapshots(&self) -> CollectionResult<Vec<SnapshotDescription>> {
        list_snapshots_in_directory(&self.snapshots_path).await
    }

    pub async fn get_snapshot_path(&self, snapshot_name: &str) -> CollectionResult<PathBuf> {
        let snapshot_path = self.snapshots_path.join(snapshot_name);

        let absolute_snapshot_path =
            snapshot_path
                .canonicalize()
                .map_err(|_| CollectionError::NotFound {
                    what: format!("Snapshot {snapshot_name}"),
                })?;

        let absolute_snapshot_dir =
            self.snapshots_path
                .canonicalize()
                .map_err(|_| CollectionError::NotFound {
                    what: format!("Snapshot directory: {}", self.snapshots_path.display()),
                })?;

        if !absolute_snapshot_path.starts_with(absolute_snapshot_dir) {
            return Err(CollectionError::NotFound {
                what: format!("Snapshot {snapshot_name}"),
            });
        }

        if !snapshot_path.exists() {
            return Err(CollectionError::NotFound {
                what: format!("Snapshot {snapshot_name}"),
            });
        }
        Ok(snapshot_path)
    }

    pub async fn create_snapshot(
        &self,
        temp_dir: &Path,
        this_peer_id: PeerId,
    ) -> CollectionResult<SnapshotDescription> {
        let snapshot_name = format!(
            "{}-{}-{}.snapshot",
            self.name(),
            this_peer_id,
            chrono::Utc::now().format("%Y-%m-%d-%H-%M-%S")
        );
        let snapshot_path = self.snapshots_path.join(&snapshot_name);
        log::info!(
            "Creating collection snapshot {} into {:?}",
            snapshot_name,
            snapshot_path
        );
        let snapshot_path_tmp = snapshot_path.with_extension("tmp");

        let snapshot_path_with_tmp_extension = temp_dir.join(&snapshot_name).with_extension("tmp");
        let snapshot_path_with_arc_extension = temp_dir.join(&snapshot_name).with_extension("arc");

        create_dir_all(&snapshot_path_with_tmp_extension).await?;

        {
            let shards_holder = self.shards_holder.read().await;
            // Create snapshot of each shard
            for (shard_id, replica_set) in shards_holder.get_shards() {
                let shard_snapshot_path =
                    versioned_shard_path(&snapshot_path_with_tmp_extension, *shard_id, 0);
                create_dir_all(&shard_snapshot_path).await?;
                // If node is listener, we can save whatever currently is in the storage
                let save_wal = self.shared_storage_config.node_type != NodeType::Listener;
                replica_set
                    .create_snapshot(temp_dir, &shard_snapshot_path, save_wal)
                    .await?;
            }
        }

        CollectionVersion::save(&snapshot_path_with_tmp_extension)?;
        self.collection_config
            .read()
            .await
            .save(&snapshot_path_with_tmp_extension)?;

        let snapshot_path_with_arc_extension_clone = snapshot_path_with_arc_extension.clone();
        let snapshot_path_with_tmp_extension_clone = snapshot_path_with_tmp_extension.clone();
        log::debug!("Archiving snapshot {:?}", snapshot_path_with_tmp_extension);
        let archiving = tokio::task::spawn_blocking(move || {
            // have to use std here, cause TarBuilder is not async
            let file = std::fs::File::create(&snapshot_path_with_arc_extension_clone)?;
            let mut builder = TarBuilder::new(file);
            // archive recursively collection directory `snapshot_path_with_arc_extension` into `snapshot_path`
            builder.append_dir_all(".", &snapshot_path_with_tmp_extension_clone)?;
            builder.finish()?;
            Ok::<_, CollectionError>(())
        });

        archiving.await??;

        // remove temporary snapshot directory
        remove_dir_all(&snapshot_path_with_tmp_extension).await?;

        // move snapshot to permanent location
        // We can't move right away, because snapshot folder can be on another mounting point.
        // We can't copy to the target location directly, cause copy is not atomic.
        copy(&snapshot_path_with_arc_extension, &snapshot_path_tmp).await?;
        rename(&snapshot_path_tmp, &snapshot_path).await?;
        remove_file(snapshot_path_with_arc_extension).await?;

        log::info!(
            "Collection snapshot {} completed into {:?}",
            snapshot_name,
            snapshot_path
        );
        get_snapshot_description(&snapshot_path).await
    }

    pub async fn recover_local_shard_from(
        &self,
        snapshot_shard_path: &Path,
        shard_id: ShardId,
    ) -> CollectionResult<bool> {
        let shard_holder = self.shards_holder.read().await;
        let replica_set =
            shard_holder
                .get_shard(&shard_id)
                .ok_or_else(|| CollectionError::NotFound {
                    what: format!("Shard {shard_id}"),
                })?;

        replica_set
            .restore_local_replica_from(snapshot_shard_path)
            .await
    }

    /// Restore collection from snapshot
    ///
    /// This method performs blocking IO.
    pub fn restore_snapshot(
        snapshot_path: &Path,
        target_dir: &Path,
        this_peer_id: PeerId,
        is_distributed: bool,
    ) -> CollectionResult<()> {
        // decompress archive
        let archive_file = std::fs::File::open(snapshot_path)?;
        let mut ar = tar::Archive::new(archive_file);
        ar.unpack(target_dir)?;

        let config = CollectionConfig::load(target_dir)?;
        config.validate_and_warn();
        let configured_shards = config.params.shard_number.get();

        for shard_id in 0..configured_shards {
            let shard_path = versioned_shard_path(target_dir, shard_id, 0);
            let shard_config_opt = ShardConfig::load(&shard_path)?;
            if let Some(shard_config) = shard_config_opt {
                match shard_config.r#type {
                    shard_config::ShardType::Local => LocalShard::restore_snapshot(&shard_path)?,
                    shard_config::ShardType::Remote { .. } => {
                        RemoteShard::restore_snapshot(&shard_path)
                    }
                    shard_config::ShardType::Temporary => {}
                    shard_config::ShardType::ReplicaSet { .. } => {
                        ReplicaSetShard::restore_snapshot(
                            &shard_path,
                            this_peer_id,
                            is_distributed,
                        )?
                    }
                }
            } else {
                return Err(CollectionError::service_error(format!(
                    "Can't read shard config at {}",
                    shard_path.display()
                )));
            }
        }

        Ok(())
    }

    pub async fn remove_shards_at_peer(&self, peer_id: PeerId) -> CollectionResult<()> {
        let shard_holder = self.shards_holder.read().await;

        for (_shard_id, replica_set) in shard_holder.get_shards() {
            replica_set.remove_peer(peer_id).await?;
        }
        Ok(())
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
        let outgoing_transfers = self.get_outgoing_transfers(&self.this_peer_id).await;
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
                    on_transfer_failure(transfer, self.name(), "transfer task does not exist");
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
            let transfers = self.get_transfers(|_| true).await;

            // Try to find a replica to transfer from
            for replica_id in replica_set.active_remote_shards().await {
                let transfer = ShardTransfer {
                    from: replica_id,
                    to: *this_peer_id,
                    shard_id,
                    sync: true,
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

    pub fn wait_collection_initiated(&self, timeout: Duration) -> bool {
        self.is_initialized.await_ready_for_timeout(timeout)
    }

    pub async fn lock_updates(&self) -> RwLockWriteGuard<()> {
        self.updates_lock.write().await
    }
}
