use std::cmp;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::future::{join, join_all};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use itertools::Itertools;
use rand::seq::SliceRandom;
use schemars::JsonSchema;
use segment::types::{
    ExtendedPointId, Filter, PointIdType, ScoredPoint, WithPayload, WithPayloadInterface,
    WithVector,
};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use super::local_shard::LocalShard;
use super::remote_shard::RemoteShard;
use super::{create_shard_dir, CollectionId};
use crate::config::CollectionConfig;
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
use crate::shards::shard_trait::{ShardOperation, ShardOperationSS};
use crate::shards::telemetry::ReplicaSetTelemetry;

pub type OnPeerFailure = Arc<dyn Fn(PeerId, ShardId) + Send + Sync>;

const READ_REMOTE_REPLICAS: u32 = 2;

const REPLICA_STATE_FILE: &str = "replica_state.json";

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
    pub peers: HashMap<PeerId, ReplicaState>,
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
    pub(crate) shard_path: PathBuf,
    pub(crate) shard_id: ShardId,
    /// Number of remote replicas to send read requests to.
    /// If actual number of peers is less than this, then read request will be sent to all of them.
    read_remote_replicas: u32,
    notify_peer_failure_cb: OnPeerFailure,
    channel_service: ChannelService,
    collection_id: CollectionId,
    collection_config: Arc<RwLock<CollectionConfig>>,
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
        self.replica_state.read().peers.clone()
    }

    // return true if was activated
    pub fn activate_replica(&self, peer_id: PeerId) -> CollectionResult<bool> {
        Ok(self.replica_state.write(|state| {
            if let std::collections::hash_map::Entry::Occupied(mut e) = state.peers.entry(peer_id) {
                e.insert(ReplicaState::Active);
                true
            } else {
                false
            }
        })?)
    }

    pub fn as_shard_operations(&self) -> &ShardOperationSS {
        self
    }

    pub fn this_peer_id(&self) -> PeerId {
        self.replica_state.read().this_peer_id
    }

    fn init_remote_shards(
        shard_id: ShardId,
        collection_id: CollectionId,
        state: &ReplicaSetState,
        channel_service: &ChannelService,
    ) -> Vec<RemoteShard> {
        state
            .peers
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
        on_peer_failure: OnPeerFailure,
        collection_path: &Path,
        shared_config: Arc<RwLock<CollectionConfig>>,
        channel_service: ChannelService,
    ) -> CollectionResult<Self> {
        let shard_path = create_shard_dir(collection_path, shard_id).await?;
        let local = if local {
            let shard = LocalShard::build(
                shard_id,
                collection_id.clone(),
                &shard_path,
                shared_config.clone(),
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
                rs.peers.insert(this_peer_id, ReplicaState::Active);
            }
            for peer in remotes {
                rs.peers.insert(peer, ReplicaState::Active);
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
            shard_path,
            // TODO: move to collection config
            read_remote_replicas: READ_REMOTE_REPLICAS,
            notify_peer_failure_cb: on_peer_failure,
            channel_service,
            collection_id,
            collection_config: shared_config,
        })
    }

    pub async fn remove_remote(&self, peer_id: PeerId) -> CollectionResult<()> {
        self.replica_state.write(|rs| {
            rs.peers.remove(&peer_id);
        })?;

        let mut remotes = self.remotes.write().await;
        remotes.retain(|remote| remote.peer_id != peer_id);
        Ok(())
    }

    pub async fn add_remote(&self, peer_id: PeerId, state: ReplicaState) -> CollectionResult<()> {
        self.replica_state.write(|rs| {
            rs.peers.insert(peer_id, state);
        })?;

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
            rs.peers.remove(&rs.this_peer_id);
        })?;

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
                    rs.peers.insert(self.this_peer_id(), active);
                }
            })?;
        }
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
    pub async fn load(
        shard_id: ShardId,
        collection_id: CollectionId,
        shard_path: &Path,
        shared_config: Arc<RwLock<CollectionConfig>>,
        channel_service: ChannelService,
        on_peer_failure: OnPeerFailure,
        this_peer_id: PeerId,
    ) -> Self {
        let replica_state: SaveOnDisk<ReplicaSetState> =
            SaveOnDisk::load_or_init(shard_path.join(REPLICA_STATE_FILE)).unwrap();

        if replica_state.read().this_peer_id != this_peer_id {
            replica_state
                .write(|rs| {
                    rs.this_peer_id = this_peer_id;
                })
                .map_err(|e| {
                    panic!("Failed to update replica state in {:?}: {}", shard_path, e);
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
            )
            .await;
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
            shard_path: shard_path.to_path_buf(),
            read_remote_replicas: READ_REMOTE_REPLICAS,
            notify_peer_failure_cb: on_peer_failure,
            channel_service,
            collection_id,
            collection_config: shared_config,
        }
    }

    pub fn notify_peer_failure(&self, peer_id: PeerId) {
        self.notify_peer_failure_cb.deref()(peer_id, self.shard_id)
    }

    pub fn set_replica_state(&self, peer_id: &PeerId, state: ReplicaState) -> CollectionResult<()> {
        self.replica_state.write(|rs| {
            if rs.this_peer_id == *peer_id {
                rs.is_local = true;
            }
            rs.peers.insert(*peer_id, state);
        })?;
        Ok(())
    }

    pub async fn apply_state(
        &self,
        replicas: HashMap<PeerId, ReplicaState>,
    ) -> CollectionResult<()> {
        let old_peers = self.replica_state.read().peers.clone();

        self.replica_state.write(|state| {
            state.peers = replicas.clone();
        })?;

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

    /// Check whether a peer is registered as `active`.
    /// Unknown peers are not active.
    pub fn peer_is_active(&self, peer_id: &PeerId) -> bool {
        self.replica_state.read().peers.get(peer_id) == Some(&ReplicaState::Active)
    }

    pub fn peer_state(&self, peer_id: &PeerId) -> Option<ReplicaState> {
        self.replica_state.read().peers.get(peer_id).copied()
    }

    /// Execute read operation on replica set:
    /// 1 - Prefer local replica
    /// 2 - Otherwise uses `read_fan_out_ratio` to compute list of active remote shards.
    /// 3 - Fallbacks to all remaining shards if the optimisations fails.
    /// It does not report failing peer_ids to the consensus.
    pub async fn execute_read_operation<'a, F, Fut, Res>(
        &'_ self,
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
                        local_result = Some(res);
                    }
                    res @ Err(CollectionError::Cancelled { .. }) => {
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
                err @ Err(CollectionError::ServiceError { .. }) => captured_error = Some(err), // capture error for possible error reporting
                err @ Err(CollectionError::Cancelled { .. }) => captured_error = Some(err), // capture error for possible error reporting
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
                err @ Err(CollectionError::ServiceError { .. }) => captured_error = Some(err), // capture error for possible error reporting
                err @ Err(CollectionError::Cancelled { .. }) => captured_error = Some(err), // capture error for possible error reporting
                err @ Err(_) => return err, // Validation or user errors reported immediately
            }
        }
        captured_error.expect("at this point `captured_error` must be defined by construction")
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
        let local = if let Some(local_shard) = &*local_shard {
            Some(local_shard.get_telemetry_data().await)
        } else {
            None
        };
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
}

#[async_trait::async_trait]
impl ShardOperation for ShardReplicaSet {
    async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        let all_res: Vec<Result<_, _>> = {
            let local = self.local.read().await;
            let remotes = self.remotes.read().await;

            // target all remote peers that are active
            let active_remote_shards: Vec<_> = remotes
                .iter()
                .filter(|rs| self.peer_is_active(&rs.peer_id))
                .collect();

            // local is defined AND the peer itself is active
            let local_is_active = local.is_some() && self.peer_is_active(&self.this_peer_id());

            if active_remote_shards.is_empty() && !local_is_active {
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
                Some(local) if self.peer_is_active(&self.this_peer_id()) => {
                    let local_update = async move {
                        local
                            .get()
                            .update(operation.clone(), wait)
                            .await
                            .map_err(|err| (self.this_peer_id(), err))
                    };
                    let remote_updates = join_all(remote_futures);

                    // run local and remote shards read concurrently
                    let (mut remote_res, local_res) = join(remote_updates, local_update).await;
                    // return both remote and local results
                    remote_res.push(local_res);
                    remote_res
                }
                _ => join_all(remote_futures).await,
            }
        };

        let (successes, failures): (Vec<_>, Vec<_>) = all_res.into_iter().partition_result();

        // Notify consensus about failures if:
        // 1. There is at least one success, otherwise it might be a problem of sending node
        // 2. ???

        if !successes.is_empty() {
            // report all failing peers to consensus
            for (peer_id, _err) in &failures {
                self.notify_peer_failure(*peer_id);
            }
        }

        return if successes.is_empty() {
            // completely failed - report error to user
            let (_peer_id, err) = failures.into_iter().next().expect("failures is not empty");
            Err(err)
        } else {
            // at least one replica succeeded
            let res = successes
                .into_iter()
                .next()
                .expect("successes is not empty");
            Ok(res)
        };
    }

    #[allow(clippy::too_many_arguments)]
    async fn scroll_by(
        &self,
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
    ) -> CollectionResult<Vec<Record>> {
        let local = self.local.read().await;
        let remotes = self.remotes.read().await;

        self.execute_read_operation(
            |shard| shard.scroll_by(offset, limit, with_payload_interface, with_vector, filter),
            &local,
            &remotes,
        )
        .await
    }

    async fn info(&self) -> CollectionResult<CollectionInfo> {
        let local = self.local.read().await;
        let remotes = self.remotes.read().await;

        self.execute_read_operation(|shard| shard.info(), &local, &remotes)
            .await
    }

    async fn search(
        &self,
        request: Arc<SearchRequestBatch>,
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let local = self.local.read().await;
        let remotes = self.remotes.read().await;

        self.execute_read_operation(
            |shard| shard.search(request.clone(), search_runtime_handle),
            &local,
            &remotes,
        )
        .await
    }

    async fn count(&self, request: Arc<CountRequest>) -> CollectionResult<CountResult> {
        let local = self.local.read().await;
        let remotes = self.remotes.read().await;

        self.execute_read_operation(|shard| shard.count(request.clone()), &local, &remotes)
            .await
    }

    async fn retrieve(
        &self,
        request: Arc<PointRequest>,
        with_payload: &WithPayload,
        with_vector: &WithVector,
    ) -> CollectionResult<Vec<Record>> {
        let local = self.local.read().await;
        let remotes = self.remotes.read().await;

        self.execute_read_operation(
            |shard| shard.retrieve(request.clone(), with_payload, with_vector),
            &local,
            &remotes,
        )
        .await
    }
}
