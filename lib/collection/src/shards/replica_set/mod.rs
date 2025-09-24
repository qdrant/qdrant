pub mod clock_set;
mod execute_read_operation;
mod locally_disabled_peers;
mod partial_snapshot_meta;
mod read_ops;
mod shard_transfer;
pub mod snapshots;
mod telemetry;
mod update;

use std::collections::{HashMap, HashSet};
use std::ops::Deref as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::rate_limiting::RateLimiter;
use common::save_on_disk::SaveOnDisk;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::types::{ExtendedPointId, Filter, ShardKey};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use tokio::sync::{Mutex, RwLock};

use self::partial_snapshot_meta::PartialSnapshotMeta;
use super::CollectionId;
use super::local_shard::LocalShard;
use super::local_shard::clock_map::RecoveryPoint;
use super::remote_shard::RemoteShard;
use super::transfer::ShardTransfer;
use crate::collection::payload_index_schema::PayloadIndexSchema;
use crate::common::collection_size_stats::CollectionSizeStats;
use crate::common::snapshots_manager::SnapshotStorageManager;
use crate::config::CollectionConfigInternal;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{CollectionError, CollectionResult, UpdateResult, UpdateStatus};
use crate::operations::{CollectionUpdateOperations, point_ops};
use crate::optimizers_builder::OptimizersConfig;
use crate::shards::channel_service::ChannelService;
use crate::shards::dummy_shard::DummyShard;
use crate::shards::replica_set::clock_set::ClockSet;
use crate::shards::shard::{PeerId, Shard, ShardId};
use crate::shards::shard_config::ShardConfig;

//    │    Collection Created
//    │
//    ▼
//  ┌──────────────┐
//  │              │
//  │ Initializing │
//  │              │
//  └──────┬───────┘
//         │  Report created    ┌───────────┐
//         └────────────────────►           │
//             Activate         │ Consensus │
//        ┌─────────────────────┤           │
//        │                     └───────────┘
//  ┌─────▼───────┐   User Promote           ┌──────────┐
//  │             ◄──────────────────────────►          │
//  │ Active      │                          │ Listener │
//  │             ◄───────────┐              │          │
//  └──┬──────────┘           │Transfer      └──┬───────┘
//     │                      │Finished         │
//     │               ┌──────┴────────┐        │Update
//     │Update         │               │        │Failure
//     │Failure        │ Partial       ├───┐    │
//     │               │               │   │    │
//     │               └───────▲───────┘   │    │
//     │                       │           │    │
//  ┌──▼──────────┐ Transfer   │           │    │
//  │             │ Started    │           │    │
//  │ Dead        ├────────────┘           │    │
//  │             │                        │    │
//  └─▲───────▲───┘        Transfer        │    │
//    │       │            Failed/Cancelled│    │
//    │       └────────────────────────────┘    │
//    │                                         │
//    └─────────────────────────────────────────┘
//

/// A set of shard replicas.
///
/// Handles operations so that the state is consistent across all the replicas of the shard.
/// Prefers local shard for read-only operations.
/// Perform updates on all replicas and report error if there is at least one failure.
///
pub struct ShardReplicaSet {
    local: RwLock<Option<Shard>>, // Abstract Shard to be able to use a Proxy during replication
    remotes: RwLock<Vec<RemoteShard>>,
    replica_state: Arc<SaveOnDisk<ReplicaSetState>>,
    /// List of peers that are marked as dead locally, but are not yet submitted to the consensus.
    /// List is checked on each consensus round and submitted to the consensus.
    /// If the state of the peer is changed in the consensus, it is removed from the list.
    /// Update and read operations are not performed on the peers marked as dead.
    locally_disabled_peers: parking_lot::RwLock<locally_disabled_peers::Registry>,
    pub(crate) shard_path: PathBuf,
    pub(crate) shard_id: ShardId,
    shard_key: Option<ShardKey>,
    notify_peer_failure_cb: ChangePeerFromState,
    abort_shard_transfer_cb: AbortShardTransfer,
    channel_service: ChannelService,
    collection_id: CollectionId,
    collection_config: Arc<RwLock<CollectionConfigInternal>>,
    optimizers_config: OptimizersConfig,
    pub(crate) shared_storage_config: Arc<SharedStorageConfig>,
    payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
    update_runtime: Handle,
    search_runtime: Handle,
    optimizer_resource_budget: ResourceBudget,
    /// Lock to serialized write operations on the replicaset when a write ordering is used.
    write_ordering_lock: Mutex<()>,
    /// Local clock set, used to tag new operations on this shard.
    clock_set: Mutex<ClockSet>,
    write_rate_limiter: Option<parking_lot::Mutex<RateLimiter>>,
    pub partial_snapshot_meta: PartialSnapshotMeta,
}

pub type AbortShardTransfer = Arc<dyn Fn(ShardTransfer, &str) + Send + Sync>;
pub type ChangePeerState = Arc<dyn Fn(PeerId, ShardId) + Send + Sync>;
pub type ChangePeerFromState = Arc<dyn Fn(PeerId, ShardId, Option<ReplicaState>) + Send + Sync>;

const REPLICA_STATE_FILE: &str = "replica_state.json";

impl ShardReplicaSet {
    /// Create a new fresh replica set, no previous state is expected.
    #[allow(clippy::too_many_arguments)]
    pub async fn build(
        shard_id: ShardId,
        shard_key: Option<ShardKey>,
        collection_id: CollectionId,
        this_peer_id: PeerId,
        local: bool,
        remotes: HashSet<PeerId>,
        on_peer_failure: ChangePeerFromState,
        abort_shard_transfer: AbortShardTransfer,
        collection_path: &Path,
        collection_config: Arc<RwLock<CollectionConfigInternal>>,
        effective_optimizers_config: OptimizersConfig,
        shared_storage_config: Arc<SharedStorageConfig>,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
        channel_service: ChannelService,
        update_runtime: Handle,
        search_runtime: Handle,
        optimizer_resource_budget: ResourceBudget,
        init_state: Option<ReplicaState>,
    ) -> CollectionResult<Self> {
        let shard_path = super::create_shard_dir(collection_path, shard_id).await?;
        let local = if local {
            let shard = LocalShard::build(
                shard_id,
                collection_id.clone(),
                &shard_path,
                collection_config.clone(),
                shared_storage_config.clone(),
                payload_index_schema.clone(),
                update_runtime.clone(),
                search_runtime.clone(),
                optimizer_resource_budget.clone(),
                effective_optimizers_config.clone(),
            )
            .await?;
            Some(Shard::Local(shard))
        } else {
            None
        };
        let replica_state: SaveOnDisk<ReplicaSetState> =
            SaveOnDisk::load_or_init_default(shard_path.join(REPLICA_STATE_FILE))?;

        let init_replica_state = init_state.unwrap_or(ReplicaState::Initializing);
        replica_state.write(|rs| {
            rs.this_peer_id = this_peer_id;
            if local.is_some() {
                rs.is_local = true;
                rs.set_peer_state(this_peer_id, init_replica_state);
            }
            for peer in remotes {
                rs.set_peer_state(peer, init_replica_state);
            }
        })?;

        let remote_shards = Self::init_remote_shards(
            shard_id,
            collection_id.clone(),
            &replica_state.read(),
            &channel_service,
        );

        // Save shard config as the last step, to ensure that the file state is consistent
        // Presence of shard config indicates that the shard is ready to be used
        let replica_set_shard_config = ShardConfig::new_replica_set();
        replica_set_shard_config.save(&shard_path)?;

        // Initialize the write rate limiter
        let config = collection_config.read().await;
        let write_rate_limiter = config.strict_mode_config.as_ref().and_then(|strict_mode| {
            strict_mode
                .write_rate_limit
                .map(RateLimiter::new_per_minute)
                .map(parking_lot::Mutex::new)
        });
        drop(config);

        Ok(Self {
            shard_id,
            shard_key,
            local: RwLock::new(local),
            remotes: RwLock::new(remote_shards),
            replica_state: replica_state.into(),
            locally_disabled_peers: Default::default(),
            shard_path,
            abort_shard_transfer_cb: abort_shard_transfer,
            notify_peer_failure_cb: on_peer_failure,
            channel_service,
            collection_id,
            collection_config,
            optimizers_config: effective_optimizers_config,
            shared_storage_config,
            payload_index_schema,
            update_runtime,
            search_runtime,
            optimizer_resource_budget,
            write_ordering_lock: Mutex::new(()),
            clock_set: Default::default(),
            write_rate_limiter,
            partial_snapshot_meta: PartialSnapshotMeta::default(),
        })
    }

    /// Recovers shard from disk.
    ///
    /// WARN: This method intended to be used only on the initial start of the node.
    /// It does not implement any logic to recover from a failure.
    /// Will panic or load partial state if there is a failure.
    #[allow(clippy::too_many_arguments)]
    pub async fn load(
        shard_id: ShardId,
        shard_key: Option<ShardKey>,
        collection_id: CollectionId,
        shard_path: &Path,
        is_dirty_shard: bool,
        collection_config: Arc<RwLock<CollectionConfigInternal>>,
        effective_optimizers_config: OptimizersConfig,
        shared_storage_config: Arc<SharedStorageConfig>,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
        channel_service: ChannelService,
        on_peer_failure: ChangePeerFromState,
        abort_shard_transfer: AbortShardTransfer,
        this_peer_id: PeerId,
        update_runtime: Handle,
        search_runtime: Handle,
        optimizer_resource_budget: ResourceBudget,
    ) -> Self {
        let replica_state: SaveOnDisk<ReplicaSetState> =
            SaveOnDisk::load_or_init_default(shard_path.join(REPLICA_STATE_FILE)).unwrap();

        if replica_state.read().this_peer_id != this_peer_id {
            replica_state
                .write(|rs| {
                    let this_peer_id = rs.this_peer_id;
                    let local_state = rs.remove_peer_state(this_peer_id);
                    if let Some(state) = local_state {
                        rs.set_peer_state(this_peer_id, state);
                    }
                    rs.this_peer_id = this_peer_id;
                })
                .map_err(|e| {
                    panic!("Failed to update replica state in {shard_path:?}: {e}");
                })
                .unwrap();
        }

        let remote_shards: Vec<_> = Self::init_remote_shards(
            shard_id,
            collection_id.clone(),
            &replica_state.read(),
            &channel_service,
        );

        let mut local_load_failure = false;
        let local = if replica_state.read().is_local {
            let shard = if let Some(recovery_reason) = &shared_storage_config.recovery_mode {
                Shard::Dummy(DummyShard::new(recovery_reason))
            } else if is_dirty_shard {
                log::error!(
                    "Shard {collection_id}:{shard_id} is not fully initialized - loading as dummy shard"
                );
                // This dummy shard will be replaced only when it rejects an update (marked as dead so recovery process kicks in)
                Shard::Dummy(DummyShard::new(
                    "Dirty shard - shard is not fully initialized",
                ))
            } else {
                let res = LocalShard::load(
                    shard_id,
                    collection_id.clone(),
                    shard_path,
                    collection_config.clone(),
                    effective_optimizers_config.clone(),
                    shared_storage_config.clone(),
                    payload_index_schema.clone(),
                    true,
                    update_runtime.clone(),
                    search_runtime.clone(),
                    optimizer_resource_budget.clone(),
                )
                .await;

                match res {
                    Ok(shard) => Shard::Local(shard),
                    Err(err) => {
                        if !shared_storage_config.handle_collection_load_errors {
                            panic!("Failed to load local shard {shard_path:?}: {err}")
                        }

                        local_load_failure = true;

                        log::error!(
                            "Failed to load local shard {shard_path:?}, \
                             initializing \"dummy\" shard instead: \
                             {err}"
                        );

                        Shard::Dummy(DummyShard::new(format!(
                            "Failed to load local shard {shard_path:?}: {err}"
                        )))
                    }
                }
            };

            Some(shard)
        } else {
            None
        };

        // Initialize the write rate limiter
        let config = collection_config.read().await;
        let write_rate_limiter = config.strict_mode_config.as_ref().and_then(|strict_mode| {
            strict_mode
                .write_rate_limit
                .map(RateLimiter::new_per_minute)
                .map(parking_lot::Mutex::new)
        });
        drop(config);

        let replica_set = Self {
            shard_id,
            shard_key,
            local: RwLock::new(local),
            remotes: RwLock::new(remote_shards),
            replica_state: replica_state.into(),
            // TODO: move to collection config
            locally_disabled_peers: Default::default(),
            shard_path: shard_path.to_path_buf(),
            notify_peer_failure_cb: on_peer_failure,
            abort_shard_transfer_cb: abort_shard_transfer,
            channel_service,
            collection_id,
            collection_config,
            optimizers_config: effective_optimizers_config,
            shared_storage_config,
            payload_index_schema,
            update_runtime,
            search_runtime,
            optimizer_resource_budget,
            write_ordering_lock: Mutex::new(()),
            clock_set: Default::default(),
            write_rate_limiter,
            partial_snapshot_meta: PartialSnapshotMeta::default(),
        };

        // `active_remote_shards` includes `Active` and `ReshardingScaleDown` replicas!
        if local_load_failure && replica_set.active_remote_shards().is_empty() {
            replica_set
                .locally_disabled_peers
                .write()
                .disable_peer(this_peer_id);
        }

        replica_set
    }

    pub fn shard_key(&self) -> Option<&ShardKey> {
        self.shard_key.as_ref()
    }

    pub fn this_peer_id(&self) -> PeerId {
        self.replica_state.read().this_peer_id
    }

    pub async fn has_remote_shard(&self) -> bool {
        !self.remotes.read().await.is_empty()
    }

    pub async fn has_local_shard(&self) -> bool {
        self.local.read().await.is_some()
    }

    pub async fn is_local(&self) -> bool {
        let local_read = self.local.read().await;
        matches!(*local_read, Some(Shard::Local(_) | Shard::Dummy(_)))
    }

    pub async fn is_queue_proxy(&self) -> bool {
        let local_read = self.local.read().await;
        matches!(*local_read, Some(Shard::QueueProxy(_)))
    }

    pub async fn is_dummy(&self) -> bool {
        let local_read = self.local.read().await;
        matches!(*local_read, Some(Shard::Dummy(_)))
    }

    pub fn peers(&self) -> HashMap<PeerId, ReplicaState> {
        self.replica_state.read().peers()
    }

    /// Checks if the current replica contains a unique source of truth and should never
    /// be deactivated or removed.
    /// If current replica is the only "alive" replica, it is considered the last source of truth.
    ///
    /// If our replica is `Initializing`, we consider it to be the last source of truth if there is
    /// no other active replicas. If we would deactivate it, it will be impossible to recover the
    /// replica later. This may happen if we got killed or crashed during collection creation.
    ///
    /// Same logic applies to `Listener` replicas, as they are not recoverable if there are no
    /// other active replicas.
    ///
    /// Examples:
    /// Active(this), Initializing(other), Initializing(other) -> true
    /// Active(this), Active(other) -> false
    /// Initializing(this) -> true
    /// Initializing(this), Initializing(other) -> true
    /// Initializing(this), Dead(other) -> true
    /// Initializing(this), Active(other) -> false
    /// Active(this), Initializing(other) -> true
    ///
    pub fn is_last_source_of_truth_replica(&self, peer_id: PeerId) -> bool {
        // This includes `Active` and `ReshardingScaleDown` replicas!
        let active_peers = self.replica_state.read().active_peers();
        if active_peers.is_empty()
            && let Some(peer_state) = self.peer_state(peer_id)
        {
            // If there are no other active peers, deactivating those replicas
            // is not recoverable, so it is considered the last source of truth,
            // even though it is not technically active.
            return matches!(
                peer_state,
                ReplicaState::Initializing | ReplicaState::Listener
            );
        }
        active_peers.len() == 1 && active_peers.contains(&peer_id)
    }

    pub fn peer_state(&self, peer_id: PeerId) -> Option<ReplicaState> {
        self.replica_state.read().get_peer_state(peer_id)
    }

    /// List the remote peer IDs on which this shard is active, excludes the local peer ID.
    pub fn active_remote_shards(&self) -> Vec<PeerId> {
        let replica_state = self.replica_state.read();
        let this_peer_id = replica_state.this_peer_id;
        replica_state
            .active_peers() // This includes `Active` and `ReshardingScaleDown` replicas!
            .into_iter()
            .filter(|&peer_id| !self.is_locally_disabled(peer_id) && peer_id != this_peer_id)
            .collect()
    }

    /// Wait for a local shard to be initialized.
    ///
    /// Uses a blocking thread internally.
    pub async fn wait_for_local(&self, timeout: Duration) -> CollectionResult<()> {
        self.wait_for(|replica_set_state| replica_set_state.is_local, timeout)
            .await
    }

    pub fn wait_for_state_condition_sync<F>(&self, check: F, timeout: Duration) -> bool
    where
        F: Fn(&ReplicaSetState) -> bool,
    {
        let replica_state = self.replica_state.clone();
        replica_state.wait_for(check, timeout)
    }

    /// Wait for a local shard to get into `state`
    ///
    /// Uses a blocking thread internally.
    pub async fn wait_for_local_state(
        &self,
        state: ReplicaState,
        timeout: Duration,
    ) -> CollectionResult<()> {
        self.wait_for(
            move |replica_set_state| {
                replica_set_state.get_peer_state(replica_set_state.this_peer_id) == Some(state)
            },
            timeout,
        )
        .await
    }

    /// Wait for a peer shard to get into `state`
    ///
    /// Uses a blocking thread internally.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub fn wait_for_state(
        &self,
        peer_id: PeerId,
        state: ReplicaState,
        timeout: Duration,
    ) -> impl Future<Output = CollectionResult<()>> + 'static {
        self.wait_for(
            move |replica_set_state| replica_set_state.get_peer_state(peer_id) == Some(state),
            timeout,
        )
    }

    /// Wait for a replica set state condition to be true.
    ///
    /// Uses a blocking thread internally.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub fn wait_for<F>(
        &self,
        check: F,
        timeout: Duration,
    ) -> impl Future<Output = CollectionResult<()>> + 'static
    where
        F: Fn(&ReplicaSetState) -> bool + Send + 'static,
    {
        // TODO: Propagate cancellation into `spawn_blocking` task!?

        let replica_state = self.replica_state.clone();
        let task = tokio::task::spawn_blocking(move || replica_state.wait_for(check, timeout));

        async move {
            let status = task.await.map_err(|err| {
                CollectionError::service_error(format!(
                    "Failed to wait for replica set state: {err}"
                ))
            })?;

            if status {
                Ok(())
            } else {
                Err(CollectionError::timeout(
                    timeout.as_secs() as usize,
                    "wait for replica set state",
                ))
            }
        }
    }

    /// Clears the local shard data and loads an empty local shard
    pub async fn init_empty_local_shard(&self) -> CollectionResult<()> {
        let mut local = self.local.write().await;

        let current_shard = local.take();

        LocalShard::clear(&self.shard_path).await?;
        let local_shard_res = LocalShard::build(
            self.shard_id,
            self.collection_id.clone(),
            &self.shard_path,
            self.collection_config.clone(),
            self.shared_storage_config.clone(),
            self.payload_index_schema.clone(),
            self.update_runtime.clone(),
            self.search_runtime.clone(),
            self.optimizer_resource_budget.clone(),
            self.optimizers_config.clone(),
        )
        .await;

        match local_shard_res {
            Ok(local_shard) => {
                *local = Some(Shard::Local(local_shard));
                Ok(())
            }
            Err(err) => {
                log::error!(
                    "Failed to initialize local shard {:?}: {err}",
                    self.shard_path
                );
                *local = current_shard;
                Err(err)
            }
        }
    }

    pub async fn set_local(
        &self,
        local: LocalShard,
        state: Option<ReplicaState>,
    ) -> CollectionResult<Option<Shard>> {
        let old_shard = self.local.write().await.replace(Shard::Local(local));

        if !self.replica_state.read().is_local || state.is_some() {
            self.replica_state.write(|rs| {
                rs.is_local = true;
                if let Some(state) = state {
                    rs.set_peer_state(self.this_peer_id(), state);
                }
            })?;
        }
        self.update_locally_disabled(self.this_peer_id());
        Ok(old_shard)
    }

    pub async fn remove_local(&self) -> CollectionResult<()> {
        // TODO: Ensure cancel safety!

        self.replica_state.write(|rs| {
            rs.is_local = false;
            let this_peer_id = rs.this_peer_id;
            rs.remove_peer_state(this_peer_id);
        })?;

        self.update_locally_disabled(self.this_peer_id());

        let removing_local = {
            let mut local = self.local.write().await;
            local.take()
        };

        if let Some(removing_local) = removing_local {
            // stop ongoing tasks and delete data
            drop(removing_local);
            LocalShard::clear(&self.shard_path).await?;
        }
        Ok(())
    }

    pub async fn add_remote(&self, peer_id: PeerId, state: ReplicaState) -> CollectionResult<()> {
        debug_assert!(peer_id != self.this_peer_id());

        self.replica_state.write(|rs| {
            rs.set_peer_state(peer_id, state);
        })?;

        self.update_locally_disabled(peer_id);

        let mut remotes = self.remotes.write().await;

        // check remote already exists
        if remotes.iter().any(|remote| remote.peer_id == peer_id) {
            return Ok(());
        }

        remotes.push(RemoteShard::new(
            self.shard_id,
            self.collection_id.clone(),
            peer_id,
            self.channel_service.clone(),
        ));

        Ok(())
    }

    pub async fn remove_remote(&self, peer_id: PeerId) -> CollectionResult<()> {
        self.replica_state.write(|rs| {
            rs.remove_peer_state(peer_id);
        })?;

        self.update_locally_disabled(peer_id);

        let mut remotes = self.remotes.write().await;
        remotes.retain(|remote| remote.peer_id != peer_id);
        Ok(())
    }

    /// Change state of the replica to the given.
    /// Ensure that remote shard is initialized.
    pub async fn ensure_replica_with_state(
        &self,
        peer_id: PeerId,
        state: ReplicaState,
    ) -> CollectionResult<()> {
        if peer_id == self.this_peer_id() {
            self.set_replica_state(peer_id, state)?;
        } else {
            // Create remote shard if necessary
            self.add_remote(peer_id, state).await?;
        }
        Ok(())
    }

    pub fn set_replica_state(&self, peer_id: PeerId, state: ReplicaState) -> CollectionResult<()> {
        log::debug!(
            "Changing local shard {}:{} state from {:?} to {state:?}",
            self.collection_id,
            self.shard_id,
            self.replica_state.read().get_peer_state(peer_id),
        );

        self.replica_state.write(|rs| {
            if rs.this_peer_id == peer_id {
                rs.is_local = true;
            }
            rs.set_peer_state(peer_id, state);
        })?;
        self.update_locally_disabled(peer_id);
        Ok(())
    }

    pub async fn remove_peer(&self, peer_id: PeerId) -> CollectionResult<()> {
        if self.this_peer_id() == peer_id {
            self.remove_local().await?;
        } else {
            self.remove_remote(peer_id).await?;
        }
        Ok(())
    }

    pub async fn apply_state(
        &mut self,
        replicas: HashMap<PeerId, ReplicaState>,
        shard_key: Option<ShardKey>,
    ) -> CollectionResult<()> {
        let old_peers = self.replica_state.read().peers();

        self.replica_state.write(|state| {
            state.set_peers(replicas.clone());
        })?;

        self.locally_disabled_peers.write().clear();

        let removed_peers = old_peers
            .keys()
            .filter(|peer_id| !replicas.contains_key(peer_id))
            .copied()
            .collect::<Vec<_>>();
        for peer_id in removed_peers {
            self.remove_peer(peer_id).await?;
        }

        for (peer_id, state) in replicas {
            let peer_already_exists = old_peers.contains_key(&peer_id);

            if peer_already_exists {
                // do nothing
                // We only need to change state and it is already saved
                continue;
            }

            if peer_id == self.this_peer_id() {
                // Consensus wants a local replica on this peer
                let local_shard = LocalShard::build(
                    self.shard_id,
                    self.collection_id.clone(),
                    &self.shard_path,
                    self.collection_config.clone(),
                    self.shared_storage_config.clone(),
                    self.payload_index_schema.clone(),
                    self.update_runtime.clone(),
                    self.search_runtime.clone(),
                    self.optimizer_resource_budget.clone(),
                    self.optimizers_config.clone(),
                )
                .await?;

                match state {
                    ReplicaState::Active
                    | ReplicaState::Listener
                    | ReplicaState::ReshardingScaleDown => {
                        // No way we can provide up-to-date replica right away at this point,
                        // so we report a failure to consensus
                        self.set_local(local_shard, Some(state)).await?;
                        self.notify_peer_failure(peer_id, Some(state));
                    }

                    ReplicaState::Dead
                    | ReplicaState::Partial
                    | ReplicaState::Initializing
                    | ReplicaState::PartialSnapshot
                    | ReplicaState::Recovery
                    | ReplicaState::Resharding => {
                        self.set_local(local_shard, Some(state)).await?;
                    }
                }

                continue;
            }

            // Otherwise it is a missing remote replica, we simply create it

            let new_remote = RemoteShard::new(
                self.shard_id,
                self.collection_id.clone(),
                peer_id,
                self.channel_service.clone(),
            );
            self.remotes.write().await.push(new_remote);
        }

        // Apply shard key
        self.shard_key = shard_key;

        Ok(())
    }

    pub(crate) async fn on_optimizer_config_update(&self) -> CollectionResult<()> {
        let read_local = self.local.read().await;
        if let Some(shard) = &*read_local {
            shard.on_optimizer_config_update().await
        } else {
            Ok(())
        }
    }

    /// Apply shard's strict mode configuration update
    /// - Update read and write rate limiters
    pub(crate) async fn on_strict_mode_config_update(&mut self) -> CollectionResult<()> {
        let mut read_local = self.local.write().await;
        if let Some(shard) = read_local.as_mut() {
            shard.on_strict_mode_config_update().await
        }
        drop(read_local);
        let config = self.collection_config.read().await;
        if let Some(strict_mode_config) = &config.strict_mode_config
            && strict_mode_config.enabled == Some(true)
        {
            // update write rate limiter
            if let Some(write_rate_limit_per_min) = strict_mode_config.write_rate_limit {
                let new_write_rate_limiter = RateLimiter::new_per_minute(write_rate_limit_per_min);
                self.write_rate_limiter
                    .replace(parking_lot::Mutex::new(new_write_rate_limiter));
                return Ok(());
            }
        }
        // remove write rate limiter for all other situations
        self.write_rate_limiter.take();
        Ok(())
    }

    /// Check if the write rate limiter allows the operation to proceed
    /// - hw_measurement_acc: the current hardware measurement accumulator
    /// - cost_fn: the cost of the operation called lazily
    ///
    /// Returns an error if the rate limit is exceeded.
    async fn check_write_rate_limiter<F>(
        &self,
        hw_measurement_acc: &HwMeasurementAcc,
        cost_fn: F,
    ) -> CollectionResult<()>
    where
        F: AsyncFnOnce() -> usize,
    {
        // Do not rate limit internal operation tagged with disposable measurement
        if hw_measurement_acc.is_disposable() {
            return Ok(());
        }
        if let Some(rate_limiter) = &self.write_rate_limiter {
            let cost = cost_fn().await;
            rate_limiter
                .lock()
                .try_consume(cost as f64)
                .map_err(|err| CollectionError::rate_limit_error(err, cost, true))?;
        }
        Ok(())
    }

    /// Check if there are any locally disabled peers
    /// And if so, report them to the consensus
    pub fn sync_local_state<F>(&self, get_shard_transfers: F) -> CollectionResult<()>
    where
        F: Fn(ShardId, PeerId) -> Vec<ShardTransfer>,
    {
        let peers_to_notify: Vec<_> = self
            .locally_disabled_peers
            .write()
            .notify_elapsed()
            .collect();

        for (failed_peer_id, from_state) in peers_to_notify {
            self.notify_peer_failure(failed_peer_id, from_state);

            for transfer in get_shard_transfers(self.shard_id, failed_peer_id) {
                self.abort_shard_transfer(
                    transfer,
                    &format!(
                        "{failed_peer_id}/{}:{} replica failed",
                        self.collection_id, self.shard_id,
                    ),
                );
            }
        }

        Ok(())
    }

    pub(crate) async fn health_check(&self, peer_id: PeerId) -> CollectionResult<()> {
        let remotes = self.remotes.read().await;

        let Some(remote) = remotes.iter().find(|remote| remote.peer_id == peer_id) else {
            return Err(CollectionError::NotFound {
                what: format!("{}/{}:{} shard", peer_id, self.collection_id, self.shard_id),
            });
        };

        remote.health_check().await?;

        Ok(())
    }

    pub async fn delete_local_points(
        &self,
        filter: Filter,
        hw_measurement_acc: HwMeasurementAcc,
        force: bool,
    ) -> CollectionResult<UpdateResult> {
        let local_shard_guard = self.local.read().await;

        let Some(local_shard) = local_shard_guard.deref() else {
            return Err(CollectionError::NotFound {
                what: format!("local shard {}:{}", self.collection_id, self.shard_id),
            });
        };

        let mut next_offset = Some(ExtendedPointId::NumId(0));
        let mut ids = Vec::new();

        while let Some(current_offset) = next_offset {
            const BATCH_SIZE: usize = 1000;

            let mut points = local_shard
                .get()
                .local_scroll_by_id(
                    Some(current_offset),
                    BATCH_SIZE + 1,
                    &false.into(),
                    &false.into(),
                    Some(&filter),
                    &self.search_runtime,
                    None,
                    hw_measurement_acc.clone(),
                )
                .await?;

            if points.len() > BATCH_SIZE {
                next_offset = points.pop().map(|points| points.id);
            } else {
                next_offset = None;
            }

            ids.extend(points.into_iter().map(|points| points.id));
        }

        if ids.is_empty() {
            return Ok(UpdateResult {
                operation_id: None,
                status: UpdateStatus::Completed,
                clock_tag: None,
            });
        }

        drop(local_shard_guard);

        let op =
            CollectionUpdateOperations::PointOperation(point_ops::PointOperations::DeletePoints {
                ids,
            });

        // TODO(resharding): Assign clock tag to the operation!? 🤔
        let result = self
            .update_local(op.into(), true, hw_measurement_acc, force)
            .await?
            .ok_or_else(|| {
                CollectionError::bad_request(format!(
                    "local shard {}:{} does not exist or is unavailable",
                    self.collection_id, self.shard_id,
                ))
            })?;

        Ok(result)
    }

    fn init_remote_shards(
        shard_id: ShardId,
        collection_id: CollectionId,
        state: &ReplicaSetState,
        channel_service: &ChannelService,
    ) -> Vec<RemoteShard> {
        state
            .peers()
            .iter()
            .filter(|(peer, _)| **peer != state.this_peer_id)
            .map(|(peer_id, _is_active)| {
                RemoteShard::new(
                    shard_id,
                    collection_id.clone(),
                    *peer_id,
                    channel_service.clone(),
                )
            })
            .collect()
    }

    /// Check whether a peer is registered as `active`.
    /// Unknown peers are not active.
    fn peer_is_active(&self, peer_id: PeerId) -> bool {
        // This is used *exclusively* during `execute_*_read_operation`, and so it *should* consider
        // `ReshardingScaleDown` replicas
        let is_active = matches!(
            self.peer_state(peer_id),
            Some(ReplicaState::Active | ReplicaState::ReshardingScaleDown)
        );

        is_active && !self.is_locally_disabled(peer_id)
    }

    fn peer_is_active_or_resharding(&self, peer_id: PeerId) -> bool {
        let is_active_or_resharding = matches!(
            self.peer_state(peer_id),
            Some(
                ReplicaState::Active | ReplicaState::Resharding | ReplicaState::ReshardingScaleDown
            )
        );

        is_active_or_resharding && !self.is_locally_disabled(peer_id)
    }

    fn peer_is_initializing(&self, peer_id: PeerId) -> bool {
        let is_initializing = matches!(self.peer_state(peer_id), Some(ReplicaState::Initializing));
        is_initializing && !self.is_locally_disabled(peer_id)
    }

    fn is_locally_disabled(&self, peer_id: PeerId) -> bool {
        self.locally_disabled_peers.read().is_disabled(peer_id)
    }

    /// Locally disable given peer
    ///
    /// Disables the peer and notifies consensus periodically.
    ///
    /// Prevents disabling the last peer (according to consensus).
    ///
    /// If `from_state` is given, the peer will only be disabled if the given state matches
    /// consensus.
    fn add_locally_disabled(
        &self,
        state: &ReplicaSetState,
        peer_id: PeerId,
        from_state: Option<ReplicaState>,
    ) {
        let other_peers = state
            .active_or_resharding_peers()
            .filter(|id| id != &peer_id);

        let mut locally_disabled_peers_guard = self.locally_disabled_peers.upgradable_read();

        // Prevent disabling last peer in consensus
        {
            if !locally_disabled_peers_guard.is_disabled(peer_id)
                && locally_disabled_peers_guard.is_all_disabled(other_peers)
            {
                log::warn!("Cannot locally disable last active peer {peer_id} for replica");
                return;
            }
        }

        locally_disabled_peers_guard.with_upgraded(|locally_disabled_peers| {
            if locally_disabled_peers.disable_peer_and_notify_if_elapsed(peer_id, from_state) {
                self.notify_peer_failure(peer_id, from_state);
            }
        });
    }

    /// Make sure that locally disabled peers do not contradict the consensus
    fn update_locally_disabled(&self, peer_id_to_remove: PeerId) {
        let mut locally_disabled_peers = self.locally_disabled_peers.write();

        // Check that we are not trying to disable the last active peer
        if locally_disabled_peers
            .is_all_disabled(self.replica_state.read().active_or_resharding_peers())
        {
            log::warn!("Resolving consensus/local state inconsistency");
            locally_disabled_peers.clear();
        } else {
            locally_disabled_peers.enable_peer(peer_id_to_remove);
        }
    }

    fn notify_peer_failure(&self, peer_id: PeerId, from_state: Option<ReplicaState>) {
        log::debug!("Notify peer failure: {peer_id}");
        self.notify_peer_failure_cb.deref()(peer_id, self.shard_id, from_state)
    }

    fn abort_shard_transfer(&self, transfer: ShardTransfer, reason: &str) {
        log::debug!(
            "Abort {}:{} / {} -> {} shard transfer",
            self.collection_id,
            transfer.shard_id,
            transfer.from,
            transfer.to,
        );

        self.abort_shard_transfer_cb.deref()(transfer, reason)
    }

    /// Get shard recovery point for WAL.
    pub(crate) async fn shard_recovery_point(&self) -> CollectionResult<RecoveryPoint> {
        let local_shard = self.local.read().await;
        let Some(local_shard) = local_shard.as_ref() else {
            return Err(CollectionError::NotFound {
                what: "Peer does not have local shard".into(),
            });
        };

        local_shard.shard_recovery_point().await
    }

    /// Update the cutoff point for the local shard.
    pub(crate) async fn update_shard_cutoff_point(
        &self,
        cutoff: &RecoveryPoint,
    ) -> CollectionResult<()> {
        let local_shard = self.local.read().await;
        let Some(local_shard) = local_shard.as_ref() else {
            return Err(CollectionError::NotFound {
                what: "Peer does not have local shard".into(),
            });
        };

        local_shard.update_cutoff(cutoff).await
    }

    pub(crate) fn get_snapshots_storage_manager(&self) -> CollectionResult<SnapshotStorageManager> {
        SnapshotStorageManager::new(&self.shared_storage_config.snapshots_config)
    }

    pub(crate) async fn trigger_optimizers(&self) -> bool {
        let shard = self.local.read().await;
        let Some(shard) = shard.as_ref() else {
            return false;
        };
        shard.trigger_optimizers();
        true
    }

    /// Returns the estimated size of all local segments.
    /// Since this locks all segments you should cache this value in performance critical scenarios!
    pub(crate) async fn calculate_local_shard_stats(&self) -> Option<CollectionSizeStats> {
        self.local
            .read()
            .await
            .as_ref()
            .map(|i| match i {
                Shard::Local(local) => {
                    let mut total_vector_size = 0;
                    let mut total_payload_size = 0;
                    let mut total_points = 0;

                    for segment in local.segments.read().iter() {
                        let size_info = segment.1.get().read().size_info();
                        total_vector_size += size_info.vectors_size_bytes;
                        total_payload_size += size_info.payloads_size_bytes;
                        total_points += size_info.num_points;
                    }

                    Some(CollectionSizeStats {
                        vector_storage_size: total_vector_size,
                        payload_storage_size: total_payload_size,
                        points_count: total_points,
                    })
                }
                Shard::Proxy(_)
                | Shard::ForwardProxy(_)
                | Shard::QueueProxy(_)
                | Shard::Dummy(_) => None,
            })
            .unwrap_or_default()
    }

    pub(crate) fn payload_index_schema(&self) -> Arc<SaveOnDisk<PayloadIndexSchema>> {
        self.payload_index_schema.clone()
    }
}

/// Represents a replica set state
#[derive(Debug, Deserialize, Serialize, Default, PartialEq, Eq, Clone)]
pub struct ReplicaSetState {
    pub is_local: bool,
    pub this_peer_id: PeerId,
    peers: HashMap<PeerId, ReplicaState>,
}

impl ReplicaSetState {
    pub fn get_peer_state(&self, peer_id: PeerId) -> Option<ReplicaState> {
        self.peers.get(&peer_id).copied()
    }

    pub fn set_peer_state(&mut self, peer_id: PeerId, state: ReplicaState) {
        self.peers.insert(peer_id, state);
    }

    pub fn remove_peer_state(&mut self, peer_id: PeerId) -> Option<ReplicaState> {
        self.peers.remove(&peer_id)
    }

    pub fn peers(&self) -> HashMap<PeerId, ReplicaState> {
        self.peers.clone()
    }

    pub fn active_peers(&self) -> Vec<PeerId> {
        self.peers
            .iter()
            .filter_map(|(peer_id, state)| {
                // We consider `ReshardingScaleDown` to be `Active`!
                matches!(
                    state,
                    ReplicaState::Active | ReplicaState::ReshardingScaleDown
                )
                .then_some(*peer_id)
            })
            .collect()
    }

    pub fn active_or_resharding_peers(&self) -> impl Iterator<Item = PeerId> + '_ {
        self.peers.iter().filter_map(|(peer_id, state)| {
            matches!(
                state,
                ReplicaState::Active | ReplicaState::Resharding | ReplicaState::ReshardingScaleDown
            )
            .then_some(*peer_id)
        })
    }

    pub fn set_peers(&mut self, peers: HashMap<PeerId, ReplicaState>) {
        self.peers = peers;
    }
}

/// State of the single shard within a replica set.
#[derive(
    Debug, Deserialize, Serialize, JsonSchema, Default, PartialEq, Eq, Hash, Clone, Copy, Anonymize,
)]
pub enum ReplicaState {
    // Active and sound
    #[default]
    Active,
    // Failed for some reason
    Dead,
    // The shard is partially loaded and is currently receiving data from other shards
    Partial,
    // Collection is being created
    Initializing,
    // A shard which receives data, but is not used for search
    // Useful for backup shards
    Listener,
    // Deprecated since Qdrant 1.9.0, used in Qdrant 1.7.0 and 1.8.0
    //
    // Snapshot shard transfer is in progress, updates aren't sent to the shard
    // Normally rejects updates. Since 1.8 it allows updates if force is true.
    PartialSnapshot,
    // Shard is undergoing recovery by an external node
    // Normally rejects updates, accepts updates if force is true
    Recovery,
    // Points are being migrated to this shard as part of resharding up
    Resharding,
    // Points are being migrated to this shard as part of resharding down
    ReshardingScaleDown,
}

impl ReplicaState {
    /// Check if replica state is active
    pub fn is_active(self) -> bool {
        match self {
            ReplicaState::Active => true,
            ReplicaState::ReshardingScaleDown => true,

            ReplicaState::Dead => false,
            ReplicaState::Partial => false,
            ReplicaState::Initializing => false,
            ReplicaState::Listener => false,
            ReplicaState::PartialSnapshot => false,
            ReplicaState::Recovery => false,
            ReplicaState::Resharding => false,
        }
    }

    /// Check whether the replica state is active or listener or resharding.
    pub fn is_active_or_listener_or_resharding(self) -> bool {
        match self {
            ReplicaState::Active
            | ReplicaState::Listener
            | ReplicaState::Resharding
            | ReplicaState::ReshardingScaleDown => true,

            ReplicaState::Dead
            | ReplicaState::Initializing
            | ReplicaState::Partial
            | ReplicaState::PartialSnapshot
            | ReplicaState::Recovery => false,
        }
    }

    /// Check whether the replica state is partial or partial-like.
    ///
    /// In other words: is the state related to shard transfers?
    //
    // TODO(resharding): What's the best way to handle `ReshardingScaleDown` properly!?
    pub fn is_partial_or_recovery(self) -> bool {
        match self {
            ReplicaState::Partial
            | ReplicaState::PartialSnapshot
            | ReplicaState::Recovery
            | ReplicaState::Resharding
            | ReplicaState::ReshardingScaleDown => true,

            ReplicaState::Active
            | ReplicaState::Dead
            | ReplicaState::Initializing
            | ReplicaState::Listener => false,
        }
    }

    /// Returns `true` if the replica state is resharding, either up or down.
    pub fn is_resharding(&self) -> bool {
        match self {
            ReplicaState::Resharding | ReplicaState::ReshardingScaleDown => true,

            ReplicaState::Partial
            | ReplicaState::PartialSnapshot
            | ReplicaState::Recovery
            | ReplicaState::Active
            | ReplicaState::Dead
            | ReplicaState::Initializing
            | ReplicaState::Listener => false,
        }
    }
}

/// Represents a change in replica set, due to scaling of `replication_factor`
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Hash, Clone)]
pub enum Change {
    Remove(ShardId, PeerId),
}
