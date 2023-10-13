mod execute_read_operation;
mod read_ops;
mod shard_transfer;
mod snapshots;

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::ops::Deref as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use futures::future::{self, BoxFuture};
use futures::stream::FuturesUnordered;
use futures::{FutureExt as _, StreamExt as _};
use itertools::Itertools;
use rand::seq::SliceRandom as _;
use schemars::JsonSchema;
use segment::types::{
    ExtendedPointId, Filter, PointIdType, ScoredPoint, WithPayload, WithPayloadInterface,
    WithVector,
};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use tokio::sync::{Mutex, RwLock};

use super::local_shard::LocalShard;
use super::queue_proxy_shard::QueueProxyShard;
use super::remote_shard::RemoteShard;
use super::resolve::{Resolve, ResolveCondition};
use super::{create_shard_dir, CollectionId};
use crate::config::CollectionConfig;
use crate::operations::consistency_params::{ReadConsistency, ReadConsistencyType};
use crate::operations::point_ops::WriteOrdering;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CoreSearchRequestBatch, CountRequest,
    CountResult, PointRequest, Record, SearchRequestBatch, UpdateResult,
};
use crate::operations::CollectionUpdateOperations;
use crate::save_on_disk::SaveOnDisk;
use crate::shards::channel_service::ChannelService;
use crate::shards::dummy_shard::DummyShard;
use crate::shards::forward_proxy_shard::ForwardProxyShard;
use crate::shards::shard::Shard::{Dummy, ForwardProxy, Local, QueueProxy};
use crate::shards::shard::{PeerId, Shard, ShardId};
use crate::shards::shard_config::ShardConfig;
use crate::shards::shard_trait::ShardOperation;
use crate::shards::telemetry::ReplicaSetTelemetry;

pub type ChangePeerState = Arc<dyn Fn(PeerId, ShardId) + Send + Sync>;

const DEFAULT_SHARD_DEACTIVATION_TIMEOUT: Duration = Duration::from_secs(30);

const REPLICA_STATE_FILE: &str = "replica_state.json";

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
}

/// Represents a change in replica set, due to scaling of `replication_factor`
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Hash, Clone)]
pub enum Change {
    Remove(ShardId, PeerId),
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

impl ShardReplicaSet {
    pub async fn is_local(&self) -> bool {
        let local_read = self.local.read().await;
        matches!(*local_read, Some(Local(_) | Dummy(_)))
    }

    pub async fn is_dummy(&self) -> bool {
        let local_read = self.local.read().await;
        matches!(*local_read, Some(Dummy(_)))
    }

    pub async fn has_local_shard(&self) -> bool {
        self.local.read().await.is_some()
    }

    pub fn peers(&self) -> HashMap<PeerId, ReplicaState> {
        self.replica_state.read().peers()
    }

    pub fn this_peer_id(&self) -> PeerId {
        self.replica_state.read().this_peer_id
    }

    pub fn highest_replica_peer_id(&self) -> Option<PeerId> {
        self.replica_state.read().peers.keys().max().cloned()
    }

    pub fn highest_alive_replica_peer_id(&self) -> Option<PeerId> {
        let read_lock = self.replica_state.read();
        let peer_ids = read_lock.peers.keys().cloned().collect::<Vec<_>>();
        drop(read_lock);

        peer_ids
            .into_iter()
            .filter(|peer_id| self.peer_is_active(peer_id)) // re-acquire replica_state read lock
            .max()
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
                *local = Some(Local(local_shard));
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
        let shard_path = create_shard_dir(collection_path, shard_id).await?;
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
            Some(Local(shard))
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

    pub async fn remove_remote(&self, peer_id: PeerId) -> CollectionResult<()> {
        self.replica_state.write(|rs| {
            rs.remove_peer_state(&peer_id);
        })?;

        self.update_locally_disabled(peer_id);

        let mut remotes = self.remotes.write().await;
        remotes.retain(|remote| remote.peer_id != peer_id);
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

    pub async fn remove_local(&self) -> CollectionResult<()> {
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

    pub async fn set_local(
        &self,
        local: LocalShard,
        state: Option<ReplicaState>,
    ) -> CollectionResult<Option<Shard>> {
        let old_shard = self.local.write().await.replace(Local(local));

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

    pub async fn remove_peer(&self, peer_id: PeerId) -> CollectionResult<()> {
        if self.this_peer_id() == peer_id {
            self.remove_local().await?;
        } else {
            self.remove_remote(peer_id).await?;
        }
        Ok(())
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
                Dummy(DummyShard::new(recovery_reason))
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
                    Ok(shard) => Local(shard),
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

                        Dummy(DummyShard::new(format!(
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

    pub fn notify_peer_failure(&self, peer_id: PeerId) {
        log::debug!("Notify peer failure: {}", peer_id);
        self.notify_peer_failure_cb.deref()(peer_id, self.shard_id)
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

    pub fn is_locally_disabled(&self, peer_id: &PeerId) -> bool {
        self.locally_disabled_peers.read().contains(peer_id)
    }

    /// Check whether a peer is registered as `active`.
    /// Unknown peers are not active.
    pub fn peer_is_active(&self, peer_id: &PeerId) -> bool {
        self.peer_state(peer_id) == Some(ReplicaState::Active) && !self.is_locally_disabled(peer_id)
    }

    pub fn peer_is_active_or_pending(&self, peer_id: &PeerId) -> bool {
        let res = match self.peer_state(peer_id) {
            Some(ReplicaState::Active) => true,
            Some(ReplicaState::Partial) => true,
            Some(ReplicaState::Initializing) => true,
            Some(ReplicaState::Dead) => false,
            Some(ReplicaState::Listener) => true,
            None => false,
        };
        res && !self.is_locally_disabled(peer_id)
    }

    pub fn peer_state(&self, peer_id: &PeerId) -> Option<ReplicaState> {
        self.replica_state.read().get_peer_state(peer_id).copied()
    }

    pub(crate) async fn on_optimizer_config_update(&self) -> CollectionResult<()> {
        let read_local = self.local.read().await;
        if let Some(shard) = &*read_local {
            shard.on_optimizer_config_update().await
        } else {
            Ok(())
        }
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

    /// Update local shard if any without forwarding to remote shards
    pub async fn update_local(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<Option<UpdateResult>> {
        if let Some(local_shard) = &*self.local.read().await {
            match self.peer_state(&self.this_peer_id()) {
                Some(ReplicaState::Active | ReplicaState::Partial | ReplicaState::Initializing) => {
                    Ok(Some(local_shard.get().update(operation, wait).await?))
                }
                Some(ReplicaState::Listener) => {
                    Ok(Some(local_shard.get().update(operation, false).await?))
                }
                Some(ReplicaState::Dead) | None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    fn handle_failed_replicas(
        &self,
        failures: &Vec<(PeerId, CollectionError)>,
        state: &ReplicaSetState,
    ) -> bool {
        let mut wait_for_deactivation = false;

        for (peer_id, err) in failures {
            log::warn!(
                "Failed to update shard {}:{} on peer {}, error: {}",
                self.collection_id,
                self.shard_id,
                peer_id,
                err
            );

            let Some(&peer_state) = state.get_peer_state(peer_id) else {
                continue;
            };

            if peer_state != ReplicaState::Active && peer_state != ReplicaState::Initializing {
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
                "Deactivating peer {} because of failed update of shard {}:{}",
                peer_id,
                self.collection_id,
                self.shard_id
            );

            self.locally_disabled_peers.write().insert(*peer_id);
            self.notify_peer_failure(*peer_id);
        }

        wait_for_deactivation
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

    /// Check if the are any locally disabled peers
    /// And if so, report them to the consensus
    pub async fn sync_local_state(&self) -> CollectionResult<()> {
        for failed_peer in self.locally_disabled_peers.read().iter() {
            self.notify_peer_failure(*failed_peer);
        }
        Ok(())
    }

    pub async fn update_with_consistency(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
        ordering: WriteOrdering,
    ) -> CollectionResult<UpdateResult> {
        match self.leader_peer_for_update(ordering) {
            None => Err(CollectionError::service_error(format!(
                "Cannot update shard {}:{} with {ordering:?} ordering because no leader could be selected",
                self.collection_id, self.shard_id
            ))),
            Some(leader_peer) => {
                // If we are the leader, run the update from this replica set
                if leader_peer == self.this_peer_id() {
                    // lock updates if ordering is medium or strong
                    let _guard = match ordering {
                        WriteOrdering::Weak => None, // no locking required
                        WriteOrdering::Medium | WriteOrdering::Strong => Some(self.write_ordering_lock.lock().await), // one request at a time
                    };
                    self.update(operation, wait).await
                } else {
                    // forward the update to the designated leader
                    self.forward_update(leader_peer, operation, wait, ordering)
                        .await
                        .map_err(|err| {
                            if err.is_transient() {
                                // Deactivate the peer if forwarding failed with transient error
                                self.locally_disabled_peers.write().insert(leader_peer);
                                self.notify_peer_failure(leader_peer);
                                // return service error
                                CollectionError::service_error(format!(
                                    "Failed to apply update with {ordering:?} ordering via leader peer {leader_peer}: {err}"
                                ))
                            } else {
                                err
                            }
                        })
                }
            }
        }
    }

    /// Designated a leader replica for the update based on the WriteOrdering
    pub fn leader_peer_for_update(&self, ordering: WriteOrdering) -> Option<PeerId> {
        match ordering {
            WriteOrdering::Weak => Some(self.this_peer_id()), // no requirement for consistency
            WriteOrdering::Medium => self.highest_alive_replica_peer_id(), // consistency with highest alive replica
            WriteOrdering::Strong => self.highest_replica_peer_id(), // consistency with highest replica
        }
    }

    /// Forward update to the leader replica
    pub async fn forward_update(
        &self,
        leader_peer: PeerId,
        operation: CollectionUpdateOperations,
        wait: bool,
        ordering: WriteOrdering,
    ) -> CollectionResult<UpdateResult> {
        let remotes_guard = self.remotes.read().await;
        let remote_leader = remotes_guard.iter().find(|r| r.peer_id == leader_peer);

        match remote_leader {
            Some(remote_leader) => {
                remote_leader
                    .forward_update(operation, wait, ordering)
                    .await
            }
            None => Err(CollectionError::service_error(format!(
                "Cannot forward update to shard {} because was removed from the replica set",
                self.shard_id
            ))),
        }
    }

    pub async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        let all_res: Vec<Result<_, _>> = {
            let remotes = self.remotes.read().await;
            let local = self.local.read().await;
            let this_peer_id = self.this_peer_id();

            // target all remote peers that can receive updates
            let active_remote_shards: Vec<_> = remotes
                .iter()
                .filter(|rs| self.peer_is_active_or_pending(&rs.peer_id))
                .collect();

            // local is defined AND the peer itself can receive updates
            let local_is_updatable =
                local.is_some() && self.peer_is_active_or_pending(&this_peer_id);

            if active_remote_shards.is_empty() && !local_is_updatable {
                return Err(CollectionError::service_error(format!(
                    "The replica set for shard {} on peer {} has no active replica",
                    self.shard_id, this_peer_id
                )));
            }

            let mut update_futures = Vec::with_capacity(active_remote_shards.len() + 1);

            if let Some(local) = local.deref() {
                if self.peer_is_active_or_pending(&this_peer_id) {
                    let local_wait =
                        if self.peer_state(&this_peer_id) == Some(ReplicaState::Listener) {
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
                            .map_err(|err| {
                                let peer_id = err.remote_peer_id().unwrap_or(this_peer_id);

                                (peer_id, err)
                            })
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
                        .map_err(|err| (remote.peer_id, err))
                };

                update_futures.push(remote_update.right_future());
            }

            match self.shared_storage_config.update_concurrency {
                Some(concurrency) => {
                    futures::stream::iter(update_futures)
                        .buffer_unordered(concurrency.get())
                        .collect()
                        .await
                }

                None => FuturesUnordered::from_iter(update_futures).collect().await,
            }
        };

        let total_results = all_res.len();

        let (successes, failures): (Vec<_>, Vec<_>) = all_res.into_iter().partition_result();

        // Notify consensus about failures if:
        // 1. There is at least one success, otherwise it might be a problem of sending node
        // 2. ???

        if !successes.is_empty() {
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
                         timed out after {} seconds. Consistency of this update is not guaranteed. Please retry.",
                        self.shard_id, timeout.as_secs()
                    )));
                }
            }
        }

        if !failures.is_empty() {
            let write_consistency_factor = self
                .collection_config
                .read()
                .await
                .params
                .write_consistency_factor
                .get() as usize;
            let minimal_success_count = write_consistency_factor.min(total_results);
            if successes.len() < minimal_success_count {
                // completely failed - report error to user
                let (_peer_id, err) = failures.into_iter().next().expect("failures is not empty");
                return Err(err);
            }
        }
        // there are enough successes, return the first one
        let res = successes
            .into_iter()
            .next()
            .expect("successes is not empty");
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};

    use segment::types::Distance;
    use tempfile::{Builder, TempDir};

    use super::*;
    use crate::config::*;
    use crate::operations::types::{VectorParams, VectorsConfig};
    use crate::optimizers_builder::OptimizersConfig;

    const TEST_OPTIMIZERS_CONFIG: OptimizersConfig = OptimizersConfig {
        deleted_threshold: 0.9,
        vacuum_min_vector_number: 1000,
        default_segment_number: 2,
        max_segment_size: None,
        memmap_threshold: None,
        indexing_threshold: Some(50_000),
        flush_interval_sec: 30,
        max_optimization_threads: 2,
    };

    pub fn dummy_on_replica_failure() -> ChangePeerState {
        Arc::new(move |_peer_id, _shard_id| {})
    }

    async fn new_shard_replica_set(collection_dir: &TempDir) -> ShardReplicaSet {
        let update_runtime = Handle::current();
        let search_runtime = Handle::current();

        let wal_config = WalConfig {
            wal_capacity_mb: 1,
            wal_segments_ahead: 0,
        };

        let collection_params = CollectionParams {
            vectors: VectorsConfig::Single(VectorParams {
                size: NonZeroU64::new(4).unwrap(),
                distance: Distance::Dot,
                hnsw_config: None,
                quantization_config: None,
                on_disk: None,
            }),
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
            collection_dir.path(),
            shared_config,
            Default::default(),
            Default::default(),
            update_runtime,
            search_runtime,
        )
        .await
        .unwrap()
    }

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
}
