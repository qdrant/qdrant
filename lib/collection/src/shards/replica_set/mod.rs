pub mod clock_set;
mod execute_read_operation;
mod locally_disabled_peers;
mod read_ops;
mod shard_transfer;
mod snapshots;
mod update;

use std::collections::{HashMap, HashSet};
use std::ops::Deref as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use common::cpu::CpuBudget;
use common::types::TelemetryDetail;
use schemars::JsonSchema;
use segment::types::{ExtendedPointId, Filter, ShardKey};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use tokio::sync::{Mutex, RwLock};

use super::local_shard::clock_map::RecoveryPoint;
use super::local_shard::LocalShard;
use super::remote_shard::RemoteShard;
use super::transfer::ShardTransfer;
use super::CollectionId;
use crate::collection::payload_index_schema::PayloadIndexSchema;
use crate::common::local_data_stats::LocalDataStats;
use crate::common::snapshots_manager::SnapshotStorageManager;
use crate::config::CollectionConfigInternal;
use crate::operations::point_ops::{self};
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{CollectionError, CollectionResult, UpdateResult, UpdateStatus};
use crate::operations::CollectionUpdateOperations;
use crate::optimizers_builder::OptimizersConfig;
use crate::save_on_disk::SaveOnDisk;
use crate::shards::channel_service::ChannelService;
use crate::shards::dummy_shard::DummyShard;
use crate::shards::replica_set::clock_set::ClockSet;
use crate::shards::shard::{PeerId, Shard, ShardId};
use crate::shards::shard_config::ShardConfig;
use crate::shards::telemetry::ReplicaSetTelemetry;

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
    optimizer_cpu_budget: CpuBudget,
    /// Lock to serialized write operations on the replicaset when a write ordering is used.
    write_ordering_lock: Mutex<()>,
    /// Local clock set, used to tag new operations on this shard.
    clock_set: Mutex<ClockSet>,
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
        optimizer_cpu_budget: CpuBudget,
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
                optimizer_cpu_budget.clone(),
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
            optimizer_cpu_budget,
            write_ordering_lock: Mutex::new(()),
            clock_set: Default::default(),
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
        optimizer_cpu_budget: CpuBudget,
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
            } else {
                let res = LocalShard::load(
                    shard_id,
                    collection_id.clone(),
                    shard_path,
                    collection_config.clone(),
                    effective_optimizers_config.clone(),
                    shared_storage_config.clone(),
                    payload_index_schema.clone(),
                    update_runtime.clone(),
                    search_runtime.clone(),
                    optimizer_cpu_budget.clone(),
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
            optimizer_cpu_budget,
            write_ordering_lock: Mutex::new(()),
            clock_set: Default::default(),
        };

        if local_load_failure && replica_set.active_remote_shards().is_empty() {
            replica_set
                .locally_disabled_peers
                .write()
                .disable_peer(this_peer_id);
        }

        replica_set
    }

    pub fn this_peer_id(&self) -> PeerId {
        self.replica_state.read().this_peer_id
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

    pub fn is_last_active_replica(&self, peer_id: PeerId) -> bool {
        let active_peers = self.replica_state.read().active_peers();
        active_peers.len() == 1 && active_peers.contains(&peer_id)
    }

    pub fn peer_state(&self, peer_id: PeerId) -> Option<ReplicaState> {
        self.replica_state.read().get_peer_state(peer_id)
    }

    /// List the peer IDs on which this shard is active, both the local and remote peers.
    pub fn active_shards(&self) -> Vec<PeerId> {
        let replica_state = self.replica_state.read();
        replica_state
            .active_peers()
            .into_iter()
            .filter(|&peer_id| !self.is_locally_disabled(peer_id))
            .collect()
    }

    /// List the remote peer IDs on which this shard is active, excludes the local peer ID.
    pub fn active_remote_shards(&self) -> Vec<PeerId> {
        let replica_state = self.replica_state.read();
        let this_peer_id = replica_state.this_peer_id;
        replica_state
            .active_peers()
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
    pub async fn wait_for_state(
        &self,
        peer_id: PeerId,
        state: ReplicaState,
        timeout: Duration,
    ) -> CollectionResult<()> {
        self.wait_for(
            move |replica_set_state| replica_set_state.get_peer_state(peer_id) == Some(state),
            timeout,
        )
        .await
    }

    /// Wait for a replica set state condition to be true.
    ///
    /// Uses a blocking thread internally.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    async fn wait_for<F>(&self, check: F, timeout: Duration) -> CollectionResult<()>
    where
        F: Fn(&ReplicaSetState) -> bool + Send + 'static,
    {
        // TODO: Propagate cancellation into `spawn_blocking` task!?

        let replica_state = self.replica_state.clone();
        let timed_out =
            !tokio::task::spawn_blocking(move || replica_state.wait_for(check, timeout))
                .await
                .map_err(|err| {
                    CollectionError::service_error(format!(
                        "Failed to wait for replica set state: {err}"
                    ))
                })?;

        if timed_out {
            return Err(CollectionError::service_error(
                "Failed to wait for replica set state, timed out",
            ));
        }

        Ok(())
    }

    pub async fn init_empty_local_shard(&self) -> CollectionResult<()> {
        let mut local = self.local.write().await;

        let current_shard = local.take();

        // ToDo: Remove shard files here?
        let local_shard_res = LocalShard::build(
            self.shard_id,
            self.collection_id.clone(),
            &self.shard_path,
            self.collection_config.clone(),
            self.shared_storage_config.clone(),
            self.payload_index_schema.clone(),
            self.update_runtime.clone(),
            self.search_runtime.clone(),
            self.optimizer_cpu_budget.clone(),
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
                if let Some(active) = state {
                    rs.set_peer_state(self.this_peer_id(), active);
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
        &self,
        replicas: HashMap<PeerId, ReplicaState>,
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
                    self.optimizer_cpu_budget.clone(),
                    self.optimizers_config.clone(),
                )
                .await?;
                match state {
                    ReplicaState::Active | ReplicaState::Listener => {
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

    pub(crate) async fn on_strict_mode_config_update(&self) -> CollectionResult<()> {
        let read_local = self.local.read().await;
        if let Some(shard) = &*read_local {
            shard.on_strict_mode_config_update().await
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

    pub(crate) async fn get_telemetry_data(&self, detail: TelemetryDetail) -> ReplicaSetTelemetry {
        let local_shard = self.local.read().await;
        let local = local_shard.as_ref();

        let local_telemetry = match local {
            Some(local_shard) => Some(local_shard.get_telemetry_data(detail).await),
            None => None,
        };

        ReplicaSetTelemetry {
            id: self.shard_id,
            key: self.shard_key.clone(),
            local: local_telemetry,
            remote: self
                .remotes
                .read()
                .await
                .iter()
                .map(|remote| remote.get_telemetry_data(detail))
                .collect(),
            replicate_states: self.replica_state.read().peers(),
        }
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

    pub async fn delete_local_points(&self, filter: Filter) -> CollectionResult<UpdateResult> {
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
                .scroll_by(
                    Some(current_offset),
                    BATCH_SIZE + 1,
                    &false.into(),
                    &false.into(),
                    Some(&filter),
                    &self.search_runtime,
                    None,
                    None,
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
        let result = self.update_local(op.into(), false).await?.ok_or_else(|| {
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
        self.peer_state(peer_id) == Some(ReplicaState::Active) && !self.is_locally_disabled(peer_id)
    }

    fn peer_is_active_or_resharding(&self, peer_id: PeerId) -> bool {
        let is_active_or_resharding = matches!(
            self.peer_state(peer_id),
            Some(ReplicaState::Active | ReplicaState::Resharding)
        );

        let is_locally_disabled = self.is_locally_disabled(peer_id);

        is_active_or_resharding && !is_locally_disabled
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
        log::debug!("Notify peer failure: {}", peer_id);
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

    /// Returns the estimated size of all locally stored vectors in bytes.
    /// Locks and iterates over all segments.
    /// Cache this value in performance critical scenarios!
    pub(crate) async fn calculate_local_shards_stats(&self) -> LocalDataStats {
        self.local
            .read()
            .await
            .as_ref()
            .map(|i| match i {
                Shard::Local(local) => {
                    let mut total_vector_size = 0;

                    for segment in local.segments.read().iter() {
                        let size_info = segment.1.get().read().size_info();
                        total_vector_size += size_info.vectors_size_bytes;
                    }

                    LocalDataStats {
                        vector_storage_size: total_vector_size,
                    }
                }
                Shard::Proxy(_)
                | Shard::ForwardProxy(_)
                | Shard::QueueProxy(_)
                | Shard::Dummy(_) => LocalDataStats::default(),
            })
            .unwrap_or_default()
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
                matches!(state, ReplicaState::Active).then_some(*peer_id)
            })
            .collect()
    }

    pub fn active_or_resharding_peers(&self) -> impl Iterator<Item = PeerId> + '_ {
        self.peers.iter().filter_map(|(peer_id, state)| {
            matches!(state, ReplicaState::Active | ReplicaState::Resharding).then_some(*peer_id)
        })
    }

    pub fn set_peers(&mut self, peers: HashMap<PeerId, ReplicaState>) {
        self.peers = peers;
    }
}

/// State of the single shard within a replica set.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Default, PartialEq, Eq, Hash, Clone, Copy)]
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
    #[schemars(skip)]
    PartialSnapshot,
    // Shard is undergoing recovery by an external node
    // Normally rejects updates, accepts updates if force is true
    Recovery,
    // Points are being migrated to this shard as part of resharding
    #[schemars(skip)]
    Resharding,
}

impl ReplicaState {
    /// Check whether the replica state is active or listener or resharding.
    pub fn is_active_or_listener_or_resharding(self) -> bool {
        match self {
            ReplicaState::Active | ReplicaState::Listener | ReplicaState::Resharding => true,

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
    pub fn is_partial_or_recovery(self) -> bool {
        match self {
            ReplicaState::Partial
            | ReplicaState::PartialSnapshot
            | ReplicaState::Recovery
            | ReplicaState::Resharding => true,

            ReplicaState::Active
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
