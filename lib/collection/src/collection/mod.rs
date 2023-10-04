mod collection_ops;
mod point_ops;
mod search;
mod shard_transfer;

use std::cmp;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use futures::future::{join_all, try_join_all};
use futures::TryStreamExt as _;
use itertools::Itertools;
use segment::common::version::StorageVersion;
use segment::spaces::tools::{peek_top_largest_iterable, peek_top_smallest_iterable};
use segment::types::{
    ExtendedPointId, Order, QuantizationConfig, ScoredPoint, WithPayload, WithPayloadInterface,
    WithVector,
};
use semver::Version;
use tar::Builder as TarBuilder;
use tokio::fs::{copy, create_dir_all, rename};
use tokio::runtime::Handle;
use tokio::sync::{Mutex, RwLock, RwLockWriteGuard};
use validator::Validate;

use crate::collection_state::{ShardInfo, State};
use crate::common::file_utils::FileCleaner;
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
    CollectionClusterInfo, CollectionError, CollectionInfo, CollectionResult,
    CoreSearchRequestBatch, CountRequest, CountResult, LocalShardInfo, NodeType, PointRequest,
    Record, RemoteShardInfo, ScrollRequest, ScrollResult, SearchRequest, SearchRequestBatch,
    UpdateResult, VectorsConfigDiff,
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
use crate::shards::shard_holder::{shard_not_found_error, LockedShardHolder, ShardHolder};
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
        self.shards_holder.read().await.get_local_shards().await
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
                })
            } else {
                log::warn!("No alive replicas to recover shard {shard_id}");
            }
        }

        Ok(())
    }

    pub async fn contains_shard(&self, shard_id: ShardId) -> bool {
        self.shards_holder.read().await.contains_shard(&shard_id)
    }

    pub fn request_shard_transfer(&self, shard_transfer: ShardTransfer) {
        self.request_shard_transfer_cb.deref()(shard_transfer)
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

    /// Creates a snapshot of the collection.
    ///
    /// The snapshot is created in three steps:
    /// 1. Create a temporary directory and create a snapshot of each shard in it.
    /// 2. Archive the temporary directory into a single file.
    /// 3. Move the archive to the final location.
    ///
    /// # Arguments
    ///
    /// * `global_temp_dir`: directory used to host snapshots while they are being created
    /// * `this_peer_id`: current peer id
    ///
    /// returns: Result<SnapshotDescription, CollectionError>
    pub async fn create_snapshot(
        &self,
        global_temp_dir: &Path,
        this_peer_id: PeerId,
    ) -> CollectionResult<SnapshotDescription> {
        let snapshot_name = format!(
            "{}-{}-{}.snapshot",
            self.name(),
            this_peer_id,
            chrono::Utc::now().format("%Y-%m-%d-%H-%M-%S")
        );

        // Final location of snapshot
        let snapshot_path = self.snapshots_path.join(&snapshot_name);
        log::info!(
            "Creating collection snapshot {} into {:?}",
            snapshot_name,
            snapshot_path
        );

        // Dedicated temporary directory for this snapshot (deleted on drop)
        let snapshot_temp_target_dir = tempfile::Builder::new()
            .prefix(&format!("{snapshot_name}-target-"))
            .tempdir_in(global_temp_dir)?;

        let snapshot_temp_target_dir_path = snapshot_temp_target_dir.path().to_path_buf();
        // Create snapshot of each shard
        {
            let snapshot_temp_temp_dir = tempfile::Builder::new()
                .prefix(&format!("{snapshot_name}-temp-"))
                .tempdir_in(global_temp_dir)?;
            let shards_holder = self.shards_holder.read().await;
            // Create snapshot of each shard
            for (shard_id, replica_set) in shards_holder.get_shards() {
                let shard_snapshot_path =
                    versioned_shard_path(&snapshot_temp_target_dir_path, *shard_id, 0);
                create_dir_all(&shard_snapshot_path).await?;
                // If node is listener, we can save whatever currently is in the storage
                let save_wal = self.shared_storage_config.node_type != NodeType::Listener;
                replica_set
                    .create_snapshot(
                        snapshot_temp_temp_dir.path(),
                        &shard_snapshot_path,
                        save_wal,
                    )
                    .await?;
            }
        }

        // Save collection config and version
        CollectionVersion::save(&snapshot_temp_target_dir_path)?;
        self.collection_config
            .read()
            .await
            .save(&snapshot_temp_target_dir_path)?;

        // Dedicated temporary file for archiving this snapshot (deleted on drop)
        let mut snapshot_temp_arc_file = tempfile::Builder::new()
            .prefix(&format!("{snapshot_name}-arc-"))
            .tempfile_in(global_temp_dir)?;

        // Archive snapshot folder into a single file
        log::debug!("Archiving snapshot {:?}", &snapshot_temp_target_dir_path);
        let archiving = tokio::task::spawn_blocking(move || {
            let mut builder = TarBuilder::new(snapshot_temp_arc_file.as_file_mut());
            // archive recursively collection directory `snapshot_path_with_arc_extension` into `snapshot_path`
            builder.append_dir_all(".", &snapshot_temp_target_dir_path)?;
            builder.finish()?;
            drop(builder);
            // return ownership of the file
            Ok::<_, CollectionError>(snapshot_temp_arc_file)
        });
        snapshot_temp_arc_file = archiving.await??;

        // Move snapshot to permanent location.
        // We can't move right away, because snapshot folder can be on another mounting point.
        // We can't copy to the target location directly, because copy is not atomic.
        // So we copy to the final location with a temporary name and then rename atomically.
        let snapshot_path_tmp_move = snapshot_path.with_extension("tmp");

        // Ensure that the temporary file is deleted on error
        let _file_cleaner = FileCleaner::new(&snapshot_path_tmp_move);
        copy(&snapshot_temp_arc_file.path(), &snapshot_path_tmp_move).await?;
        rename(&snapshot_path_tmp_move, &snapshot_path).await?;

        log::info!(
            "Collection snapshot {} completed into {:?}",
            snapshot_name,
            snapshot_path
        );
        get_snapshot_description(&snapshot_path).await
    }

    pub async fn list_shard_snapshots(
        &self,
        shard_id: ShardId,
    ) -> CollectionResult<Vec<SnapshotDescription>> {
        self.shards_holder
            .read()
            .await
            .list_shard_snapshots(&self.snapshots_path, shard_id)
            .await
    }

    pub async fn create_shard_snapshot(
        &self,
        shard_id: ShardId,
        temp_dir: &Path,
    ) -> CollectionResult<SnapshotDescription> {
        self.shards_holder
            .read()
            .await
            .create_shard_snapshot(&self.snapshots_path, &self.name(), shard_id, temp_dir)
            .await
    }

    pub async fn assert_shard_exists(&self, shard_id: ShardId) -> CollectionResult<()> {
        self.shards_holder
            .read()
            .await
            .assert_shard_exists(shard_id)
            .await
    }

    pub async fn get_shard_snapshot_path(
        &self,
        shard_id: ShardId,
        snapshot_file_name: impl AsRef<Path>,
    ) -> CollectionResult<PathBuf> {
        self.shards_holder
            .read()
            .await
            .get_shard_snapshot_path(&self.snapshots_path, shard_id, snapshot_file_name)
            .await
    }

    pub async fn restore_shard_snapshot(
        &self,
        shard_id: ShardId,
        snapshot_path: &Path,
        this_peer_id: PeerId,
        is_distributed: bool,
        temp_dir: &Path,
    ) -> CollectionResult<()> {
        self.shards_holder
            .read()
            .await
            .restore_shard_snapshot(
                snapshot_path,
                &self.name(),
                shard_id,
                this_peer_id,
                is_distributed,
                temp_dir,
            )
            .await
    }

    pub async fn recover_local_shard_from(
        &self,
        snapshot_shard_path: &Path,
        shard_id: ShardId,
    ) -> CollectionResult<bool> {
        // TODO:
        //   Check that shard snapshot is compatible with the collection
        //   (see `VectorsConfig::check_compatible_with_segment_config`)

        self.shards_holder
            .read()
            .await
            .recover_local_shard_from(snapshot_shard_path, shard_id)
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
