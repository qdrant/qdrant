use std::cmp;
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use futures::future::{join, join_all};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use rand::seq::SliceRandom;
use schemars::JsonSchema;
use segment::types::{
    ExtendedPointId, Filter, PointIdType, ScoredPoint, WithPayload, WithPayloadInterface,
    WithVector,
};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use tokio::sync::{Mutex, RwLock};

use super::local_shard::LocalShard;
use super::remote_shard::RemoteShard;
use super::resolve::{Resolve, ResolveCondition};
use super::{create_shard_dir, CollectionId};
use crate::config::CollectionConfig;
use crate::operations::consistency_params::{ReadConsistency, ReadConsistencyType};
use crate::operations::point_ops::WriteOrdering;
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CountRequest, CountResult, PointRequest,
    Record, SearchRequestBatch, UpdateResult,
};
use crate::operations::CollectionUpdateOperations;
use crate::save_on_disk::SaveOnDisk;
use crate::shards::channel_service::ChannelService;
use crate::shards::forward_proxy_shard::ForwardProxyShard;
use crate::shards::shard::Shard::{ForwardProxy, Local};
use crate::shards::shard::{PeerId, Shard, ShardId};
use crate::shards::shard_config::ShardConfig;
use crate::shards::shard_trait::ShardOperation;
use crate::shards::telemetry::ReplicaSetTelemetry;

pub type ActivatePeer = Arc<dyn Fn(PeerId, ShardId) + Send + Sync>;
pub type ChangePeerState = Arc<dyn Fn(PeerId, ShardId) + Send + Sync>;
pub type OnPeerCreated = Arc<dyn Fn(PeerId, ShardId) + Send + Sync>;

const DEFAULT_SHARD_DEACTIVATION_TIMEOUT: Duration = Duration::from_secs(30);

const READ_REMOTE_REPLICAS: u32 = 2;

const REPLICA_STATE_FILE: &str = "replica_state.json";

// Shard replication state machine:
//
//    │
//    │    Collection Created
//    │
//    ▼
//  ┌─────────────┐
//  │             │
//  │Initializing │
//  │             │
//  └─────┬─-─────┘
//        │  Report created     ┌───────────┐
//        └────────────────────►│           │
//             Activate         │ Consensus │
//        ┌─────────────────────│           │
//        │                     └───────────┘
//  ┌─────▼───────┐
//  │             │
//  │ Active      ◄───────────┐
//  │             │           │Transfer
//  └──┬──────────┘           │Finished
//     │                      │
//     │               ┌──────┴────────┐
//     │Update         │               │
//     │Failure        │ Partial       ├───┐
//     │               │               │   │
//     │               └───────▲───────┘   │
//     │                       │           │Transfer
//  ┌──▼──────────┐            │           │Failed/Cancelled
//  │             │ Transfer   │           │
//  │ Dead        ├────────────┘           │
//  │             │ Started                │
//  └──▲──────────┘                        │
//     │                                   │
//     └───────────────────────────────────┘

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
    replica_state: SaveOnDisk<ReplicaSetState>,
    /// List of peers that are marked as dead locally, but are not yet submitted to the consensus.
    /// List is checked on each consensus round and submitted to the consensus.
    /// If the state of the peer is changed in the consensus, it is removed from the list.
    /// Update and read operations are not performed on the peers marked as dead.
    locally_disabled_peers: parking_lot::RwLock<HashSet<PeerId>>,
    pub(crate) shard_path: PathBuf,
    pub(crate) shard_id: ShardId,
    /// Number of remote replicas to send read requests to.
    /// If actual number of peers is less than this, then read request will be sent to all of them.
    read_remote_replicas: u32,
    notify_peer_failure_cb: ChangePeerState,
    channel_service: ChannelService,
    collection_id: CollectionId,
    collection_config: Arc<RwLock<CollectionConfig>>,
    update_runtime: Handle,
    /// Lock to serialized write operations on the replicaset when a write ordering is used.
    write_ordering_lock: Mutex<()>,
}

impl ShardReplicaSet {
    pub async fn is_local(&self) -> bool {
        let local_read = self.local.read().await;
        matches!(*local_read, Some(Local(_)))
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
        self.replica_state
            .read()
            .peers
            .iter()
            .filter_map(|(peer_id, _state)| {
                if self.peer_is_active(peer_id) {
                    Some(peer_id)
                } else {
                    None
                }
            })
            .max()
            .cloned()
    }

    pub async fn remote_peers(&self) -> Vec<PeerId> {
        self.remotes
            .read()
            .await
            .iter()
            .map(|r| r.peer_id)
            .collect()
    }

    pub async fn active_remote_shards(&self) -> Vec<PeerId> {
        self.remote_peers()
            .await
            .into_iter()
            .filter(|peer_id| self.peer_is_active(peer_id))
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
        shared_config: Arc<RwLock<CollectionConfig>>,
        channel_service: ChannelService,
        update_runtime: Handle,
    ) -> CollectionResult<Self> {
        let shard_path = create_shard_dir(collection_path, shard_id).await?;
        let local = if local {
            let shard = LocalShard::build(
                shard_id,
                collection_id.clone(),
                &shard_path,
                shared_config.clone(),
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
            replica_state,
            locally_disabled_peers: Default::default(),
            shard_path,
            // TODO: move to collection config
            read_remote_replicas: READ_REMOTE_REPLICAS,
            notify_peer_failure_cb: on_peer_failure,
            channel_service,
            collection_id,
            collection_config: shared_config,
            update_runtime,
            write_ordering_lock: Mutex::new(()),
        })
    }

    pub async fn remove_remote(&self, peer_id: PeerId) -> CollectionResult<()> {
        self.replica_state.write(|rs| {
            rs.remove_peer_state(&peer_id);
        })?;

        self.locally_disabled_peers.write().remove(&peer_id);

        let mut remotes = self.remotes.write().await;
        remotes.retain(|remote| remote.peer_id != peer_id);
        Ok(())
    }

    pub async fn add_remote(&self, peer_id: PeerId, state: ReplicaState) -> CollectionResult<()> {
        self.replica_state.write(|rs| {
            rs.set_peer_state(peer_id, state);
        })?;

        self.locally_disabled_peers.write().remove(&peer_id);

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

        self.locally_disabled_peers
            .write()
            .remove(&self.this_peer_id());

        let removing_local = {
            let mut local = self.local.write().await;
            local.take()
        };

        if let Some(mut removing_local) = removing_local {
            removing_local.before_drop().await;
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
        self.locally_disabled_peers
            .write()
            .remove(&self.this_peer_id());
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

    pub async fn add_peer(&self, peer_id: PeerId, state: ReplicaState) -> CollectionResult<()> {
        if self.this_peer_id() == peer_id {
            let mut local = self.local.write().await;
            let new_local = if local.is_none() {
                Some(Local(
                    LocalShard::build(
                        self.shard_id,
                        self.collection_id.clone(),
                        &self.shard_path,
                        self.collection_config.clone(),
                        self.update_runtime.clone(),
                    )
                    .await?,
                ))
            } else {
                None
            };

            self.set_replica_state(&peer_id, state)?;
            if new_local.is_some() {
                *local = new_local;
            }
        } else {
            self.add_remote(peer_id, state).await?;
        }
        Ok(())
    }

    /// Recovers shard from disk.
    ///
    /// WARN: This method intended to be used only on the initial start of the node.
    /// It does not implement any logic to recover from a failure. Will panic if there is a failure.
    #[allow(clippy::too_many_arguments)]
    pub async fn load(
        shard_id: ShardId,
        collection_id: CollectionId,
        shard_path: &Path,
        shared_config: Arc<RwLock<CollectionConfig>>,
        channel_service: ChannelService,
        on_peer_failure: ChangePeerState,
        this_peer_id: PeerId,
        update_runtime: Handle,
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

        let local = if replica_state.read().is_local {
            let shard = LocalShard::load(
                shard_id,
                collection_id.clone(),
                shard_path,
                shared_config.clone(),
                update_runtime.clone(),
            )
            .await
            .map_err(|e| {
                panic!("Failed to load local shard {shard_path:?}: {e}");
            })
            .unwrap();
            Some(Local(shard))
        } else {
            None
        };

        Self {
            shard_id,
            local: RwLock::new(local),
            remotes: RwLock::new(remote_shards),
            replica_state,
            // TODO: move to collection config
            locally_disabled_peers: Default::default(),
            shard_path: shard_path.to_path_buf(),
            read_remote_replicas: READ_REMOTE_REPLICAS,
            notify_peer_failure_cb: on_peer_failure,
            channel_service,
            collection_id,
            collection_config: shared_config,
            update_runtime,
            write_ordering_lock: Mutex::new(()),
        }
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
        self.replica_state.write(|rs| {
            if rs.this_peer_id == *peer_id {
                rs.is_local = true;
            }
            rs.set_peer_state(*peer_id, state);
        })?;
        self.locally_disabled_peers.write().remove(peer_id);
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
            None => false,
        };
        res && !self.is_locally_disabled(peer_id)
    }

    pub fn peer_state(&self, peer_id: &PeerId) -> Option<ReplicaState> {
        self.replica_state.read().get_peer_state(peer_id).copied()
    }

    /// Execute read op. on replica set:
    /// 1 - Prefer local replica
    /// 2 - Otherwise uses `read_fan_out_ratio` to compute list of active remote shards.
    /// 3 - Fallbacks to all remaining shards if the optimisations fails.
    /// It does not report failing peer_ids to the consensus.
    pub async fn execute_read_operation<'a, F, Fut, Res>(
        &self,
        read_operation: F,
        local: &'a Option<Shard>,
        remotes: &'a [RemoteShard],
    ) -> CollectionResult<Res>
    where
        F: Fn(&'a (dyn ShardOperation + Send + Sync)) -> Fut,
        Fut: Future<Output = CollectionResult<Res>>,
    {
        let mut local_result = None;
        // 1 - prefer the local shard if it is active
        if let Some(local) = local {
            if self.peer_is_active(&self.this_peer_id()) {
                let read_operation_res = read_operation(local.get()).await;
                match read_operation_res {
                    Ok(_) => return read_operation_res,
                    res @ Err(CollectionError::ServiceError { .. }) => {
                        log::debug!("Local read op. failed: {:?}", res.as_ref().err());
                        local_result = Some(res);
                    }
                    res @ Err(CollectionError::Cancelled { .. }) => {
                        log::debug!("Local read op. cancelled: {:?}", res.as_ref().err());
                        local_result = Some(res);
                    }
                    res @ Err(_) => {
                        return res; // Validation errors are not recoverable, reply immediately
                    }
                }
            }
        }

        // 2 - try a subset of active remote shards in parallel for fast response
        let mut active_remote_shards: Vec<_> = remotes
            .iter()
            .filter(|rs| self.peer_is_active(&rs.peer_id))
            .collect();

        if active_remote_shards.is_empty() {
            if let Some(local_result) = local_result {
                return local_result;
            }

            return Err(CollectionError::service_error(format!(
                "The replica set for shard {} on peer {} has no active replica",
                self.shard_id,
                self.this_peer_id()
            )));
        }

        // Shuffle the list of active remote shards to avoid biasing the first ones
        active_remote_shards.shuffle(&mut rand::thread_rng());

        let fan_out_selection = cmp::min(
            active_remote_shards.len(),
            self.read_remote_replicas as usize,
        );

        let mut futures = FuturesUnordered::new();
        for remote in &active_remote_shards[0..fan_out_selection] {
            let fut = read_operation(*remote);
            futures.push(fut);
        }

        // shortcut at first successful result
        let mut captured_error = None;
        while let Some(result) = futures.next().await {
            match result {
                Ok(res) => return Ok(res), // We only need one successful result
                err @ Err(CollectionError::ServiceError { .. }) => {
                    log::debug!("Remote read op. failed: {:?}", err.as_ref().err());
                    captured_error = Some(err)
                } // capture error for possible error reporting
                err @ Err(CollectionError::Cancelled { .. }) => {
                    log::debug!("Remote read op. cancelled: {:?}", err.as_ref().err());
                    captured_error = Some(err)
                } // capture error for possible error reporting
                err @ Err(_) => return err, // Validation or user errors reported immediately
            }
        }
        debug_assert!(
            captured_error.is_some(),
            "there must be at least one failure"
        );

        // 3 - fallback to remaining remote shards as last chance
        let mut futures = FuturesUnordered::new();
        for remote in &active_remote_shards[fan_out_selection..] {
            let fut = read_operation(*remote);
            futures.push(fut);
        }

        // shortcut at first successful result
        while let Some(result) = futures.next().await {
            match result {
                Ok(res) => return Ok(res), // We only need one successful result
                err @ Err(CollectionError::ServiceError { .. }) => {
                    log::debug!("Remote fallback read op. failed: {:?}", err.as_ref().err());
                    captured_error = Some(err)
                } // capture error for possible error reporting
                err @ Err(CollectionError::Cancelled { .. }) => {
                    log::debug!(
                        "Remote fallback read op. cancelled: {:?}",
                        err.as_ref().err()
                    );
                    captured_error = Some(err)
                } // capture error for possible error reporting
                err @ Err(_) => return err, // Validation or user errors reported immediately
            }
        }
        captured_error.expect("at this point `captured_error` must be defined by construction")
    }

    pub async fn execute_and_resolve_read_operation<'a, F, Fut, Res>(
        &self,
        read_operation: F,
        local: &'a Option<Shard>,
        remotes: &'a [RemoteShard],
        read_consistency: ReadConsistency,
    ) -> CollectionResult<Res>
    where
        F: Fn(&'a (dyn ShardOperation + Send + Sync)) -> Fut,
        Fut: Future<Output = CollectionResult<Res>>,
        Res: Resolve,
    {
        let local_count = usize::from(local.is_some());
        let remotes_count = remotes.len();

        let active_local = local
            .as_ref()
            .filter(|_| self.peer_is_active(&self.this_peer_id()));

        let active_remotes_iter = remotes
            .iter()
            .filter(|remote| self.peer_is_active(&remote.peer_id));

        let active_local_count = usize::from(active_local.is_some());
        let active_remotes_count = active_remotes_iter.clone().count();

        let total_count = local_count + remotes_count;
        let active_count = active_local_count + active_remotes_count;

        let (factor, condition) = match read_consistency {
            ReadConsistency::Type(ReadConsistencyType::All) => (total_count, ResolveCondition::All),

            ReadConsistency::Type(ReadConsistencyType::Majority) => {
                (total_count, ResolveCondition::Majority)
            }

            ReadConsistency::Type(ReadConsistencyType::Quorum) => {
                (total_count / 2 + 1, ResolveCondition::All)
            }

            ReadConsistency::Factor(factor) => {
                (factor.clamp(1, total_count), ResolveCondition::All)
            }
        };

        if active_count < factor {
            return Err(CollectionError::service_error(format!(
                "The replica set for shard {} on peer {} does not have enough active replicas",
                self.shard_id,
                self.this_peer_id(),
            )));
        }

        let mut active_remotes: Vec<_> = active_remotes_iter.collect();
        active_remotes.shuffle(&mut rand::thread_rng());

        let local_operations = active_local
            .into_iter()
            .map(|local| read_operation(local.get()).left_future());

        let remote_operations = active_remotes
            .into_iter()
            .map(|remote| read_operation(remote).right_future());

        let mut operations = local_operations.chain(remote_operations);

        let required_reads = if active_local_count > 0 {
            // If there is a local shard, we can ignore fan-out `read_remote_replicas` param,
            // as we already know that the local peer is working.
            factor
        } else {
            max(factor, usize::try_from(self.read_remote_replicas).unwrap())
        };

        let mut pending_operations: FuturesUnordered<_> =
            operations.by_ref().take(required_reads).collect();

        let mut responses = Vec::new();

        while let Some(result) = pending_operations.next().await {
            match result {
                Ok(resp) => responses.push(resp),

                Err(err) => {
                    let is_transient = matches!(
                        &err,
                        CollectionError::ServiceError { .. } | CollectionError::Cancelled { .. },
                    );

                    if is_transient {
                        log::debug!("Read operation failed: {err}");
                    } else {
                        return Err(err);
                    }
                }
            }

            if responses.len() >= factor {
                break;
            }

            let maybe_responses = responses.len() + pending_operations.len();

            let schedule = factor.saturating_sub(maybe_responses);
            pending_operations.extend(operations.by_ref().take(schedule));

            let maybe_responses = responses.len() + pending_operations.len();

            if maybe_responses < factor {
                break;
            }
        }

        if responses.len() >= factor {
            if factor == 1 {
                Ok(responses.into_iter().next().unwrap())
            } else {
                Ok(Res::resolve(responses, condition))
            }
        } else {
            Err(CollectionError::service_error(
                "Failed to complete read operation: too many replicas returned an error".into(),
            ))
        }
    }

    pub(crate) async fn on_optimizer_config_update(&self) -> CollectionResult<()> {
        let read_local = self.local.read().await;
        if let Some(shard) = &*read_local {
            shard.on_optimizer_config_update().await
        } else {
            Ok(())
        }
    }

    pub(crate) async fn before_drop(&mut self) {
        let mut write_local = self.local.write().await;
        if let Some(shard) = &mut *write_local {
            shard.before_drop().await
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

    /// Returns if local shard was recovered from path
    pub async fn restore_local_replica_from(&self, replica_path: &Path) -> CollectionResult<bool> {
        if LocalShard::check_data(replica_path) {
            let mut local = self.local.write().await;
            let removed_local = local.take();

            if let Some(mut removing_local) = removed_local {
                removing_local.before_drop().await;
                LocalShard::clear(&self.shard_path).await?;
            }
            LocalShard::move_data(replica_path, &self.shard_path).await?;

            let new_local_shard = LocalShard::load(
                self.shard_id,
                self.collection_id.clone(),
                &self.shard_path,
                self.collection_config.clone(),
                self.update_runtime.clone(),
            )
            .await?;

            local.replace(Local(new_local_shard));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn restore_snapshot(snapshot_path: &Path) -> CollectionResult<()> {
        let replica_state: SaveOnDisk<ReplicaSetState> =
            SaveOnDisk::load_or_init(snapshot_path.join(REPLICA_STATE_FILE))?;
        if replica_state.read().is_local {
            LocalShard::restore_snapshot(snapshot_path)?;
        }
        Ok(())
    }

    pub async fn create_snapshot(&self, target_path: &Path) -> CollectionResult<()> {
        let local_read = self.local.read().await;

        if let Some(local) = &*local_read {
            local.create_snapshot(target_path).await?
        }

        self.replica_state
            .save_to(target_path.join(REPLICA_STATE_FILE))?;

        let shard_config = ShardConfig::new_replica_set();
        shard_config.save(target_path)?;
        Ok(())
    }

    pub async fn proxify_local(&self, remote_shard: RemoteShard) -> CollectionResult<()> {
        let mut local_write = self.local.write().await;

        match &*local_write {
            Some(Local(_)) => {
                // Do nothing, we proceed further
            }
            Some(ForwardProxy(proxy)) => {
                return if proxy.remote_shard.peer_id == remote_shard.peer_id {
                    Ok(())
                } else {
                    Err(CollectionError::service_error(format!(
                        "Cannot proxify local shard {} to peer {} because it is already proxified to peer {}",
                        self.shard_id, remote_shard.peer_id, proxy.remote_shard.peer_id
                    )))
                }
            }
            Some(shard) => {
                return Err(CollectionError::service_error(format!(
                    "Cannot proxify local shard {} - {} to peer {} because it is already proxified to another peer",
                    shard.variant_name(), self.shard_id, remote_shard.peer_id
                )))
            }
            None => {
                return Err(CollectionError::service_error(format!(
                    "Cannot proxify local shard {} on peer {} because it is not active",
                    self.shard_id,
                    self.this_peer_id()
                )));
            }
        };

        if let Some(Local(local)) = local_write.take() {
            let proxy_shard = ForwardProxyShard::new(local, remote_shard);
            let _ = local_write.insert(ForwardProxy(proxy_shard));
        }

        Ok(())
    }

    /// Un-proxify local shard.
    ///
    /// Returns true if the replica was un-proxified, false if it was already handled
    pub async fn un_proxify_local(&self) -> CollectionResult<()> {
        let mut local_write = self.local.write().await;

        match &*local_write {
            Some(ForwardProxy(_)) => {
                // Do nothing, we proceed further
            }
            Some(Local(_)) => return Ok(()),
            Some(shard) => {
                return Err(CollectionError::service_error(format!(
                    "Cannot un-proxify local shard {} because it has unexpected type - {}",
                    self.shard_id,
                    shard.variant_name(),
                )))
            }
            None => {
                return Err(CollectionError::service_error(format!(
                    "Cannot un-proxify local shard {} on peer {} because it is not active",
                    self.shard_id,
                    self.this_peer_id()
                )));
            }
        };

        if let Some(ForwardProxy(proxy)) = local_write.take() {
            let local_shard = proxy.wrapped_shard;
            let _ = local_write.insert(Local(local_shard));
        }

        Ok(())
    }

    /// Update local shard if any without forwarding to remote shards
    pub async fn update_local(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<Option<UpdateResult>> {
        if let Some(local_shard) = &*self.local.read().await {
            match self.peer_state(&self.this_peer_id()) {
                Some(ReplicaState::Active) => {
                    Ok(Some(local_shard.get().update(operation, wait).await?))
                }
                Some(ReplicaState::Partial) => {
                    Ok(Some(local_shard.get().update(operation, wait).await?))
                }
                Some(ReplicaState::Initializing) => {
                    Ok(Some(local_shard.get().update(operation, wait).await?))
                }
                Some(ReplicaState::Dead) | None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    /// Custom operation for transferring data from one shard to another during transfer
    pub async fn transfer_batch(
        &self,
        offset: Option<PointIdType>,
        batch_size: usize,
    ) -> CollectionResult<Option<PointIdType>> {
        let read_local = self.local.read().await;
        if let Some(ForwardProxy(proxy)) = &*read_local {
            proxy.transfer_batch(offset, batch_size).await
        } else {
            Err(CollectionError::service_error(format!(
                "Cannot transfer batch from shard {} because it is not proxified",
                self.shard_id
            )))
        }
    }

    /// Custom operation for transferring indexes from one shard to another during transfer
    pub async fn transfer_indexes(&self) -> CollectionResult<()> {
        let read_local = self.local.read().await;
        if let Some(ForwardProxy(proxy)) = &*read_local {
            proxy.transfer_indexes().await
        } else {
            Err(CollectionError::service_error(format!(
                "Cannot transfer indexes from shard {} because it is not proxified",
                self.shard_id
            )))
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
            if let Some(ReplicaState::Active) = state.get_peer_state(peer_id) {
                match err {
                    CollectionError::ServiceError { .. } | CollectionError::Cancelled { .. } => {
                        // If the error is service error, we should deactivate the peer
                        // before allowing other operations to continue.
                        // Otherwise, the failed node can become responsive again, before
                        // the other nodes deactivate it, so the storage might be inconsistent.
                        wait_for_deactivation = true;
                    }
                    _ => {}
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
        }
        wait_for_deactivation
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
                            match err {
                                CollectionError::ServiceError { .. } | CollectionError::Cancelled { .. } => {
                                    // Deactivate the peer if forwarding failed with service error
                                    self.locally_disabled_peers.write().insert(leader_peer);
                                    self.notify_peer_failure(leader_peer);
                                    // return service error
                                    CollectionError::service_error(format!(
                                        "Failed to apply update with {ordering:?} ordering via leader peer {leader_peer}: {err}"
                                    ))
                                }
                                _ => err // return the original error
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
            WriteOrdering::Medium => self.highest_replica_peer_id(), // consistency with highest replica
            WriteOrdering::Strong => self.highest_alive_replica_peer_id(), // consistency with highest alive replica
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
            let local = self.local.read().await;
            let remotes = self.remotes.read().await;

            // target all remote peers that can receive updates
            let active_remote_shards: Vec<_> = remotes
                .iter()
                .filter(|rs| self.peer_is_active_or_pending(&rs.peer_id))
                .collect();

            // local is defined AND the peer itself can receive updates
            let local_is_updatable =
                local.is_some() && self.peer_is_active_or_pending(&self.this_peer_id());

            if active_remote_shards.is_empty() && !local_is_updatable {
                return Err(CollectionError::service_error(format!(
                    "The replica set for shard {} on peer {} has no active replica",
                    self.shard_id,
                    self.this_peer_id()
                )));
            }

            let mut remote_futures = Vec::new();
            for remote in active_remote_shards {
                let op = operation.clone();
                remote_futures.push(async move {
                    remote
                        .update(op, wait)
                        .await
                        .map_err(|err| (remote.peer_id, err))
                });
            }

            match local.deref() {
                Some(local) if self.peer_is_active_or_pending(&self.this_peer_id()) => {
                    let local_update = async move {
                        local
                            .get()
                            .update(operation.clone(), wait)
                            .await
                            .map_err(|err| (self.this_peer_id(), err))
                    };
                    let remote_updates = join_all(remote_futures);

                    // run local and remote shards read concurrently
                    let (mut remote_res, local_res): (
                        Vec<Result<UpdateResult, (PeerId, CollectionError)>>,
                        _,
                    ) = join(remote_updates, local_update).await;
                    // return both remote and local results
                    remote_res.push(local_res);
                    remote_res
                }
                _ => join_all(remote_futures).await,
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
                let shards_disabled = self.replica_state.wait_for(
                    |state| {
                        failures.iter().all(|(peer_id, _)| {
                            state
                                .peers
                                .get(peer_id)
                                .map(|state| state != &ReplicaState::Active)
                                .unwrap_or(true) // not found means that peer is dead
                        })
                    },
                    DEFAULT_SHARD_DEACTIVATION_TIMEOUT,
                );
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

    #[allow(clippy::too_many_arguments)]
    pub async fn scroll_by(
        &self,
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        read_consistency: Option<ReadConsistency>,
    ) -> CollectionResult<Vec<Record>> {
        let local = self.local.read().await;
        let remotes = self.remotes.read().await;

        self.execute_and_resolve_read_operation(
            |shard| shard.scroll_by(offset, limit, with_payload_interface, with_vector, filter),
            &local,
            &remotes,
            read_consistency.unwrap_or_default(),
        )
        .await
    }

    pub async fn info(&self) -> CollectionResult<CollectionInfo> {
        let local = self.local.read().await;
        let remotes = self.remotes.read().await;

        self.execute_read_operation(|shard| shard.info(), &local, &remotes)
            .await
    }

    pub async fn search(
        &self,
        request: Arc<SearchRequestBatch>,
        read_consistency: Option<ReadConsistency>,
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let local = self.local.read().await;
        let remotes = self.remotes.read().await;

        self.execute_and_resolve_read_operation(
            |shard| shard.search(request.clone(), search_runtime_handle),
            &local,
            &remotes,
            read_consistency.unwrap_or_default(),
        )
        .await
    }

    pub async fn count_local(
        &self,
        request: Arc<CountRequest>,
    ) -> CollectionResult<Option<CountResult>> {
        let local = self.local.read().await;
        match &*local {
            None => Ok(None),
            Some(shard) => Ok(Some(shard.get().count(request).await?)),
        }
    }

    pub async fn count(&self, request: Arc<CountRequest>) -> CollectionResult<CountResult> {
        let local = self.local.read().await;
        let remotes = self.remotes.read().await;

        self.execute_read_operation(|shard| shard.count(request.clone()), &local, &remotes)
            .await
    }

    pub async fn retrieve(
        &self,
        request: Arc<PointRequest>,
        with_payload: &WithPayload,
        with_vector: &WithVector,
        read_consistency: Option<ReadConsistency>,
    ) -> CollectionResult<Vec<Record>> {
        let local = self.local.read().await;
        let remotes = self.remotes.read().await;

        self.execute_and_resolve_read_operation(
            |shard| shard.retrieve(request.clone(), with_payload, with_vector),
            &local,
            &remotes,
            read_consistency.unwrap_or_default(),
        )
        .await
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
        indexing_threshold: 50_000,
        flush_interval_sec: 30,
        max_optimization_threads: 2,
    };

    pub fn dummy_on_replica_failure() -> ChangePeerState {
        Arc::new(move |_peer_id, _shard_id| {})
    }

    async fn new_shard_replica_set(collection_dir: &TempDir) -> ShardReplicaSet {
        let update_runtime = Handle::current();
        let wal_config = WalConfig {
            wal_capacity_mb: 1,
            wal_segments_ahead: 0,
        };

        let collection_params = CollectionParams {
            vectors: VectorsConfig::Single(VectorParams {
                size: NonZeroU64::new(4).unwrap(),
                distance: Distance::Dot,
            }),
            shard_number: NonZeroU32::new(4).unwrap(),
            replication_factor: NonZeroU32::new(3).unwrap(),
            write_consistency_factor: NonZeroU32::new(2).unwrap(),
            on_disk_payload: false,
        };

        let config = CollectionConfig {
            params: collection_params,
            optimizer_config: TEST_OPTIMIZERS_CONFIG.clone(),
            wal_config,
            hnsw_config: Default::default(),
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
            update_runtime,
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
