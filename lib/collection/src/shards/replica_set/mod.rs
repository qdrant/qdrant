mod execute_read_operation;
mod read_ops;
mod shard_transfer;
mod snapshots;
mod update;

use std::collections::{HashMap, HashSet};
use std::ops::Deref as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use tokio::sync::{Mutex, RwLock};

use super::local_shard::LocalShard;
use super::remote_shard::RemoteShard;
use super::CollectionId;
use crate::config::CollectionConfig;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::save_on_disk::SaveOnDisk;
use crate::shards::channel_service::ChannelService;
use crate::shards::dummy_shard::DummyShard;
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
    locally_disabled_peers: parking_lot::RwLock<HashSet<PeerId>>,
    pub(crate) shard_path: PathBuf,
    pub(crate) shard_id: ShardId,
    notify_peer_failure_cb: ChangePeerState,
    channel_service: ChannelService,
    collection_id: CollectionId,
    collection_config: Arc<RwLock<CollectionConfig>>,
    shared_storage_config: Arc<SharedStorageConfig>,
    update_runtime: Handle,
    search_runtime: Handle,
    /// Lock to serialized write operations on the replicaset when a write ordering is used.
    write_ordering_lock: Mutex<()>,
}

pub type ChangePeerState = Arc<dyn Fn(PeerId, ShardId) + Send + Sync>;

const REPLICA_STATE_FILE: &str = "replica_state.json";

impl ShardReplicaSet {
    /// Create a new fresh replica set, no previous state is expected.
    #[allow(clippy::too_many_arguments)]
    pub async fn build(
        shard_id: ShardId,
        collection_id: CollectionId,
        this_peer_id: PeerId,
        local: bool,
        remotes: HashSet<PeerId>,
        on_peer_failure: ChangePeerState,
        collection_path: &Path,
        collection_config: Arc<RwLock<CollectionConfig>>,
        shared_storage_config: Arc<SharedStorageConfig>,
        channel_service: ChannelService,
        update_runtime: Handle,
        search_runtime: Handle,
    ) -> CollectionResult<Self> {
        let shard_path = super::create_shard_dir(collection_path, shard_id).await?;
        let local = if local {
            let shard = LocalShard::build(
                shard_id,
                collection_id.clone(),
                &shard_path,
                collection_config.clone(),
                shared_storage_config.clone(),
                update_runtime.clone(),
            )
            .await?;
            Some(Shard::Local(shard))
        } else {
            None
        };
        let replica_state: SaveOnDisk<ReplicaSetState> =
            SaveOnDisk::load_or_init(shard_path.join(REPLICA_STATE_FILE))?;
        replica_state.write(|rs| {
            rs.this_peer_id = this_peer_id;
            if local.is_some() {
                rs.is_local = true;
                rs.set_peer_state(this_peer_id, ReplicaState::Initializing);
            }
            for peer in remotes {
                rs.set_peer_state(peer, ReplicaState::Initializing);
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
            local: RwLock::new(local),
            remotes: RwLock::new(remote_shards),
            replica_state: replica_state.into(),
            locally_disabled_peers: Default::default(),
            shard_path,
            notify_peer_failure_cb: on_peer_failure,
            channel_service,
            collection_id,
            collection_config,
            shared_storage_config,
            update_runtime,
            search_runtime,
            write_ordering_lock: Mutex::new(()),
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
        collection_id: CollectionId,
        shard_path: &Path,
        collection_config: Arc<RwLock<CollectionConfig>>,
        shared_storage_config: Arc<SharedStorageConfig>,
        channel_service: ChannelService,
        on_peer_failure: ChangePeerState,
        this_peer_id: PeerId,
        update_runtime: Handle,
        search_runtime: Handle,
    ) -> Self {
        let replica_state: SaveOnDisk<ReplicaSetState> =
            SaveOnDisk::load_or_init(shard_path.join(REPLICA_STATE_FILE)).unwrap();

        if replica_state.read().this_peer_id != this_peer_id {
            replica_state
                .write(|rs| {
                    let this_peer_id = rs.this_peer_id;
                    let local_state = rs.remove_peer_state(&this_peer_id);
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
                    shared_storage_config.clone(),
                    update_runtime.clone(),
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
            local: RwLock::new(local),
            remotes: RwLock::new(remote_shards),
            replica_state: replica_state.into(),
            // TODO: move to collection config
            locally_disabled_peers: Default::default(),
            shard_path: shard_path.to_path_buf(),
            notify_peer_failure_cb: on_peer_failure,
            channel_service,
            collection_id,
            collection_config,
            shared_storage_config,
            update_runtime,
            search_runtime,
            write_ordering_lock: Mutex::new(()),
        };

        if local_load_failure && replica_set.active_remote_shards().await.is_empty() {
            replica_set
                .locally_disabled_peers
                .write()
                .insert(this_peer_id);
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

    pub fn peer_state(&self, peer_id: &PeerId) -> Option<ReplicaState> {
        self.replica_state.read().get_peer_state(peer_id).copied()
    }

    pub async fn active_remote_shards(&self) -> Vec<PeerId> {
        let replica_state = self.replica_state.read();
        let this_peer_id = replica_state.this_peer_id;
        replica_state
            .active_peers()
            .into_iter()
            .filter(|peer_id| !self.is_locally_disabled(peer_id) && *peer_id != this_peer_id)
            .collect()
    }

    /// Wait for a local shard to be initialized.
    ///
    /// Uses a blocking thread internally.
    pub async fn wait_for_local(&self, timeout: Duration) -> CollectionResult<()> {
        self.wait_for(|replica_set_state| replica_set_state.is_local, timeout)
            .await
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
                replica_set_state.get_peer_state(&replica_set_state.this_peer_id) == Some(&state)
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
            move |replica_set_state| replica_set_state.get_peer_state(&peer_id) == Some(&state),
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
            self.update_runtime.clone(),
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
            rs.remove_peer_state(&this_peer_id);
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
            rs.remove_peer_state(&peer_id);
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
        peer_id: &PeerId,
        state: ReplicaState,
    ) -> CollectionResult<()> {
        if *peer_id == self.replica_state.read().this_peer_id {
            self.set_replica_state(peer_id, state)?;
        } else {
            // Create remote shard if necessary
            self.add_remote(*peer_id, state).await?;
        }
        Ok(())
    }

    pub fn set_replica_state(&self, peer_id: &PeerId, state: ReplicaState) -> CollectionResult<()> {
        log::debug!(
            "Changing local shard {}:{} state from {:?} to {state:?}",
            self.collection_id,
            self.shard_id,
            self.replica_state.read().get_peer_state(peer_id),
        );

        self.replica_state.write(|rs| {
            if rs.this_peer_id == *peer_id {
                rs.is_local = true;
            }
            rs.set_peer_state(*peer_id, state);
        })?;
        self.update_locally_disabled(*peer_id);
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
            let peer_already_exists = old_peers.get(&peer_id).is_some();

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
                    self.update_runtime.clone(),
                )
                .await?;
                match state {
                    ReplicaState::Active => {
                        // No way we can provide up-to-date replica right away at this point,
                        // so we report a failure to consensus
                        self.set_local(local_shard, Some(ReplicaState::Active))
                            .await?;
                        self.notify_peer_failure(peer_id);
                    }
                    ReplicaState::Dead => {
                        self.set_local(local_shard, Some(ReplicaState::Dead))
                            .await?;
                    }
                    ReplicaState::Partial => {
                        self.set_local(local_shard, Some(ReplicaState::Partial))
                            .await?;
                    }
                    ReplicaState::Initializing => {
                        self.set_local(local_shard, Some(ReplicaState::Initializing))
                            .await?;
                    }
                    ReplicaState::Listener => {
                        // Same as `Active`, we report a failure to consensus
                        self.set_local(local_shard, Some(ReplicaState::Listener))
                            .await?;
                        self.notify_peer_failure(peer_id);
                    }
                    ReplicaState::PartialSnapshot => {
                        self.set_local(local_shard, Some(ReplicaState::PartialSnapshot))
                            .await?;
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

    /// Check if the are any locally disabled peers
    /// And if so, report them to the consensus
    pub async fn sync_local_state(&self) -> CollectionResult<()> {
        for failed_peer in self.locally_disabled_peers.read().iter() {
            self.notify_peer_failure(*failed_peer);
        }
        Ok(())
    }

    pub(crate) async fn get_telemetry_data(&self) -> ReplicaSetTelemetry {
        let local_shard = self.local.read().await;
        let local = local_shard
            .as_ref()
            .map(|local_shard| local_shard.get_telemetry_data());
        ReplicaSetTelemetry {
            id: self.shard_id,
            local,
            remote: self
                .remotes
                .read()
                .await
                .iter()
                .map(|remote| remote.get_telemetry_data())
                .collect(),
            replicate_states: self.replica_state.read().peers(),
        }
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
    fn peer_is_active(&self, peer_id: &PeerId) -> bool {
        self.peer_state(peer_id) == Some(ReplicaState::Active) && !self.is_locally_disabled(peer_id)
    }

    fn is_locally_disabled(&self, peer_id: &PeerId) -> bool {
        self.locally_disabled_peers.read().contains(peer_id)
    }

    // Make sure that locally disabled peers do not contradict the consensus
    fn update_locally_disabled(&self, peer_id_to_remove: PeerId) {
        // Check that we are not trying to disable the last active peer
        let peers = self.peers();

        let active_peers: HashSet<_> = peers
            .iter()
            .filter(|(_, state)| **state == ReplicaState::Active)
            .map(|(peer, _)| *peer)
            .collect();

        let mut locally_disabled = self.locally_disabled_peers.write();

        locally_disabled.remove(&peer_id_to_remove);

        if active_peers.is_subset(&locally_disabled) {
            log::warn!("Resolving consensus/local state inconsistency");
            locally_disabled.clear();
        }
    }

    fn notify_peer_failure(&self, peer_id: PeerId) {
        log::debug!("Notify peer failure: {}", peer_id);
        self.notify_peer_failure_cb.deref()(peer_id, self.shard_id)
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
    pub fn get_peer_state(&self, peer_id: &PeerId) -> Option<&ReplicaState> {
        self.peers.get(peer_id)
    }

    pub fn set_peer_state(&mut self, peer_id: PeerId, state: ReplicaState) {
        self.peers.insert(peer_id, state);
    }

    pub fn remove_peer_state(&mut self, peer_id: &PeerId) -> Option<ReplicaState> {
        self.peers.remove(peer_id)
    }

    pub fn peers(&self) -> HashMap<PeerId, ReplicaState> {
        self.peers.clone()
    }

    pub fn active_peers(&self) -> Vec<PeerId> {
        self.peers
            .iter()
            .filter_map(|(peer_id, state)| {
                if *state == ReplicaState::Active {
                    Some(*peer_id)
                } else {
                    None
                }
            })
            .collect()
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
    // Snapshot shard transfer is in progress, updates aren't sent to the shard
    PartialSnapshot,
}

/// Represents a change in replica set, due to scaling of `replication_factor`
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Hash, Clone)]
pub enum Change {
    Remove(ShardId, PeerId),
}
