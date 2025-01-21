mod resharding;
pub(crate) mod shard_mapping;

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::ops::Deref as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use common::cpu::CpuBudget;
use common::tar_ext::BuilderExt;
use futures::{stream, Future, StreamExt, TryStreamExt as _};
use itertools::Itertools;
use segment::common::validate_snapshot_archive::open_snapshot_archive_with_validation;
use segment::types::{ShardKey, SnapshotFormat};
use shard_mapping::{SaveOnDiskShardKeyMappingWrapper, ShardKeyMapping};
use tokio::runtime::Handle;
use tokio::sync::{broadcast, OwnedRwLockReadGuard, RwLock};
use tokio_util::codec::{BytesCodec, FramedRead};
use tokio_util::io::SyncIoBridge;

use super::replica_set::{AbortShardTransfer, ChangePeerFromState};
use super::resharding::{ReshardStage, ReshardState};
use super::transfer::transfer_tasks_pool::TransferTasksPool;
use crate::collection::payload_index_schema::PayloadIndexSchema;
use crate::common::collection_size_stats::CollectionSizeStats;
use crate::common::snapshot_stream::SnapshotStream;
use crate::config::{CollectionConfigInternal, ShardingMethod};
use crate::hash_ring::HashRingRouter;
use crate::operations::cluster_ops::ReshardingDirection;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::snapshot_ops::SnapshotDescription;
use crate::operations::types::{
    CollectionError, CollectionResult, ReshardingInfo, ShardTransferInfo,
};
use crate::operations::{OperationToShard, SplitByShard};
use crate::optimizers_builder::OptimizersConfig;
use crate::save_on_disk::SaveOnDisk;
use crate::shards::channel_service::ChannelService;
use crate::shards::local_shard::LocalShard;
use crate::shards::replica_set::{ReplicaState, ShardReplicaSet};
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_config::{ShardConfig, ShardType};
use crate::shards::shard_versioning::latest_shard_paths;
use crate::shards::transfer::{ShardTransfer, ShardTransferKey};
use crate::shards::CollectionId;

const SHARD_TRANSFERS_FILE: &str = "shard_transfers";
const RESHARDING_STATE_FILE: &str = "resharding_state.json";
pub const SHARD_KEY_MAPPING_FILE: &str = "shard_key_mapping.json";

pub struct ShardHolder {
    shards: HashMap<ShardId, ShardReplicaSet>,
    pub(crate) shard_transfers: SaveOnDisk<HashSet<ShardTransfer>>,
    pub(crate) shard_transfer_changes: broadcast::Sender<ShardTransferChange>,
    pub(crate) resharding_state: SaveOnDisk<Option<ReshardState>>,
    pub(crate) rings: HashMap<Option<ShardKey>, HashRingRouter>,
    key_mapping: SaveOnDiskShardKeyMappingWrapper,
    // Duplicates the information from `key_mapping` for faster access
    // Do not require locking
    shard_id_to_key_mapping: HashMap<ShardId, ShardKey>,
}

pub type LockedShardHolder = RwLock<ShardHolder>;

impl ShardHolder {
    pub async fn trigger_optimizers(&self) {
        for shard in self.shards.values() {
            shard.trigger_optimizers().await;
        }
    }

    pub fn new(collection_path: &Path) -> CollectionResult<Self> {
        let shard_transfers =
            SaveOnDisk::load_or_init_default(collection_path.join(SHARD_TRANSFERS_FILE))?;
        let resharding_state: SaveOnDisk<Option<ReshardState>> =
            SaveOnDisk::load_or_init_default(collection_path.join(RESHARDING_STATE_FILE))?;

        let key_mapping = SaveOnDiskShardKeyMappingWrapper::load_or_init_default(
            collection_path.join(SHARD_KEY_MAPPING_FILE),
        )?;

        let mut shard_id_to_key_mapping = HashMap::new();

        for (shard_key, shard_ids) in key_mapping.read().iter() {
            for shard_id in shard_ids {
                shard_id_to_key_mapping.insert(*shard_id, shard_key.clone());
            }
        }

        let rings = HashMap::from([(None, HashRingRouter::single())]);

        let (shard_transfer_changes, _) = broadcast::channel(64);

        Ok(Self {
            shards: HashMap::new(),
            shard_transfers,
            shard_transfer_changes,
            resharding_state,
            rings,
            key_mapping,
            shard_id_to_key_mapping,
        })
    }

    pub async fn save_key_mapping_to_tar(
        &self,
        tar: &common::tar_ext::BuilderExt,
    ) -> CollectionResult<()> {
        self.key_mapping
            .save_to_tar(tar, Path::new(SHARD_KEY_MAPPING_FILE))
            .await?;
        Ok(())
    }

    pub fn get_shard_id_to_key_mapping(&self) -> &HashMap<ShardId, ShardKey> {
        &self.shard_id_to_key_mapping
    }

    pub fn get_shard_key_to_ids_mapping(&self) -> ShardKeyMapping {
        self.key_mapping.read().clone()
    }

    pub async fn drop_and_remove_shard(
        &mut self,
        shard_id: ShardId,
    ) -> Result<(), CollectionError> {
        if let Some(replica_set) = self.shards.remove(&shard_id) {
            let shard_path = replica_set.shard_path.clone();
            drop(replica_set);

            // Explicitly drop shard config file first
            // If removing all shard files at once, it may be possible for the shard configuration
            // file to be left behind if the process is killed in the middle. We must avoid this so
            // we don't attempt to load this shard anymore on restart.
            let shard_config_path = ShardConfig::get_config_path(&shard_path);
            if let Err(err) = tokio::fs::remove_file(shard_config_path).await {
                log::error!("Failed to remove shard config file before removing the rest of the files: {err}");
            }

            tokio::fs::remove_dir_all(shard_path).await?;
        }
        Ok(())
    }

    pub fn remove_shard_from_key_mapping(
        &mut self,
        shard_id: ShardId,
        shard_key: &ShardKey,
    ) -> Result<(), CollectionError> {
        self.key_mapping.write_optional(|key_mapping| {
            if !key_mapping.contains_key(shard_key) {
                return None;
            }

            let mut key_mapping = key_mapping.clone();
            key_mapping.get_mut(shard_key).unwrap().remove(&shard_id);
            Some(key_mapping)
        })?;
        self.shard_id_to_key_mapping.remove(&shard_id);

        Ok(())
    }

    pub fn add_shard(
        &mut self,
        shard_id: ShardId,
        shard: ShardReplicaSet,
        shard_key: Option<ShardKey>,
    ) -> Result<(), CollectionError> {
        self.shards.insert(shard_id, shard);
        self.rings
            .entry(shard_key.clone())
            .or_insert_with(HashRingRouter::single)
            .add(shard_id);

        if let Some(shard_key) = shard_key {
            self.key_mapping.write_optional(|key_mapping| {
                let has_id = key_mapping
                    .get(&shard_key)
                    .map(|shard_ids| shard_ids.contains(&shard_id))
                    .unwrap_or(false);

                if has_id {
                    return None;
                }
                let mut copy_of_mapping = key_mapping.clone();
                let shard_ids = copy_of_mapping.entry(shard_key.clone()).or_default();
                shard_ids.insert(shard_id);
                Some(copy_of_mapping)
            })?;
            self.shard_id_to_key_mapping.insert(shard_id, shard_key);
        }
        Ok(())
    }

    pub async fn remove_shard_key(&mut self, shard_key: &ShardKey) -> Result<(), CollectionError> {
        let mut remove_shard_ids = Vec::new();

        self.key_mapping.write_optional(|key_mapping| {
            if key_mapping.contains_key(shard_key) {
                let mut new_key_mapping = key_mapping.clone();
                if let Some(shard_ids) = new_key_mapping.remove(shard_key) {
                    for shard_id in shard_ids {
                        remove_shard_ids.push(shard_id);
                    }
                }
                Some(new_key_mapping)
            } else {
                None
            }
        })?;

        self.rings.remove(&shard_key.clone().into());
        for shard_id in remove_shard_ids {
            self.drop_and_remove_shard(shard_id).await?;
            self.shard_id_to_key_mapping.remove(&shard_id);
        }
        Ok(())
    }

    fn rebuild_rings(&mut self) {
        let mut rings = HashMap::from([(None, HashRingRouter::single())]);
        let ids_to_key = self.get_shard_id_to_key_mapping();
        for shard_id in self.shards.keys() {
            let shard_key = ids_to_key.get(shard_id).cloned();
            rings
                .entry(shard_key)
                .or_insert_with(HashRingRouter::single)
                .add(*shard_id);
        }

        // Restore resharding hash ring if resharding is active and haven't reached
        // `WriteHashRingCommitted` stage yet
        if let Some(state) = self.resharding_state.read().deref() {
            let ring = rings
                .get_mut(&state.shard_key)
                .expect("must have hash ring for current resharding shard key");

            ring.start_resharding(state.shard_id, state.direction);

            if state.stage >= ReshardStage::WriteHashRingCommitted {
                ring.commit_resharding();
            }
        }

        self.rings = rings;
    }

    pub async fn apply_shards_state(
        &mut self,
        shard_ids: HashSet<ShardId>,
        shard_key_mapping: ShardKeyMapping,
        extra_shards: HashMap<ShardId, ShardReplicaSet>,
    ) -> Result<(), CollectionError> {
        self.shards.extend(extra_shards.into_iter());

        let all_shard_ids = self.shards.keys().cloned().collect::<HashSet<_>>();

        self.key_mapping
            .write_optional(|_key_mapping| Some(shard_key_mapping))?;

        for shard_id in all_shard_ids {
            if !shard_ids.contains(&shard_id) {
                self.drop_and_remove_shard(shard_id).await?;
            }
        }

        self.rebuild_rings();

        Ok(())
    }

    pub fn contains_shard(&self, shard_id: ShardId) -> bool {
        self.shards.contains_key(&shard_id)
    }

    pub fn get_shard(&self, shard_id: ShardId) -> Option<&ShardReplicaSet> {
        self.shards.get(&shard_id)
    }

    pub fn get_shards(&self) -> impl Iterator<Item = (ShardId, &ShardReplicaSet)> {
        self.shards.iter().map(|(id, shard)| (*id, shard))
    }

    pub fn all_shards(&self) -> impl Iterator<Item = &ShardReplicaSet> {
        self.shards.values()
    }

    pub fn all_shards_mut(&mut self) -> impl Iterator<Item = &mut ShardReplicaSet> {
        self.shards.values_mut()
    }

    pub fn split_by_shard<O: SplitByShard + Clone>(
        &self,
        operation: O,
        shard_keys_selection: &Option<ShardKey>,
    ) -> CollectionResult<Vec<(&ShardReplicaSet, O)>> {
        let Some(hashring) = self.rings.get(&shard_keys_selection.clone()) else {
            return if let Some(shard_key) = shard_keys_selection {
                Err(CollectionError::bad_input(format!(
                    "Shard key {shard_key} not found"
                )))
            } else {
                Err(CollectionError::bad_input(
                    "Shard key not specified".to_string(),
                ))
            };
        };

        if hashring.is_empty() {
            return Err(CollectionError::bad_input(
                "No shards found for shard key".to_string(),
            ));
        }

        let operation_to_shard = operation.split_by_shard(hashring);
        let shard_ops: Vec<_> = match operation_to_shard {
            OperationToShard::ByShard(by_shard) => by_shard
                .into_iter()
                .map(|(shard_id, operation)| (self.shards.get(&shard_id).unwrap(), operation))
                .collect(),
            OperationToShard::ToAll(operation) => {
                if let Some(shard_key) = shard_keys_selection {
                    let shard_ids = self
                        .key_mapping
                        .read()
                        .get(shard_key)
                        .cloned()
                        .unwrap_or_default();
                    shard_ids
                        .into_iter()
                        .map(|shard_id| (self.shards.get(&shard_id).unwrap(), operation.clone()))
                        .collect()
                } else {
                    self.all_shards()
                        .map(|shard| (shard, operation.clone()))
                        .collect()
                }
            }
        };
        Ok(shard_ops)
    }

    pub fn register_start_shard_transfer(&self, transfer: ShardTransfer) -> CollectionResult<bool> {
        let changed = self
            .shard_transfers
            .write(|transfers| transfers.insert(transfer.clone()))?;
        let _ = self
            .shard_transfer_changes
            .send(ShardTransferChange::Start(transfer));
        Ok(changed)
    }

    pub fn register_finish_transfer(&self, key: &ShardTransferKey) -> CollectionResult<bool> {
        let any_removed = self.shard_transfers.write(|transfers| {
            let before_remove = transfers.len();
            transfers.retain(|transfer| !key.check(transfer));
            before_remove != transfers.len()
        })?;
        let _ = self
            .shard_transfer_changes
            .send(ShardTransferChange::Finish(*key));
        Ok(any_removed)
    }

    pub fn register_abort_transfer(&self, key: &ShardTransferKey) -> CollectionResult<bool> {
        let any_removed = self.shard_transfers.write(|transfers| {
            let before_remove = transfers.len();
            transfers.retain(|transfer| !key.check(transfer));
            before_remove != transfers.len()
        })?;
        let _ = self
            .shard_transfer_changes
            .send(ShardTransferChange::Abort(*key));
        Ok(any_removed)
    }

    /// Await for a given shard transfer to complete.
    ///
    /// The returned inner result defines whether it successfully finished or whether it was
    /// aborted/cancelled.
    pub fn await_shard_transfer_end(
        &self,
        transfer: ShardTransferKey,
        timeout: Duration,
    ) -> impl Future<Output = CollectionResult<Result<(), ()>>> {
        let mut subscriber = self.shard_transfer_changes.subscribe();
        let receiver = async move {
            loop {
                match subscriber.recv().await {
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return Err(CollectionError::service_error(
                        "Failed to await shard transfer end: failed to listen for shard transfer changes, channel closed"
                    )),
                    Err(err @ tokio::sync::broadcast::error::RecvError::Lagged(_)) => return Err(CollectionError::service_error(format!(
                        "Failed to await shard transfer end: failed to listen for shard transfer changes, channel lagged behind: {err}"
                    ))),
                    Ok(ShardTransferChange::Finish(key)) if key == transfer => return Ok(Ok(())),
                    Ok(ShardTransferChange::Abort(key)) if key == transfer => return Ok(Err(())),
                    Ok(_) => {},
                }
            }
        };

        async move {
            match tokio::time::timeout(timeout, receiver).await {
                Ok(operation) => Ok(operation?),
                // Timeout
                Err(err) => Err(CollectionError::service_error(format!(
                    "Awaiting for shard transfer end timed out: {err}"
                ))),
            }
        }
    }

    /// The count of incoming and outgoing shard transfers on the given peer
    ///
    /// This only includes shard transfers that are in consensus for the current collection. A
    /// shard transfer that has just been proposed may not be included yet.
    pub fn count_shard_transfer_io(&self, peer_id: PeerId) -> (usize, usize) {
        let (mut incoming, mut outgoing) = (0, 0);

        for transfer in self.shard_transfers.read().iter() {
            incoming += usize::from(transfer.to == peer_id);
            outgoing += usize::from(transfer.from == peer_id);
        }

        (incoming, outgoing)
    }

    pub fn get_shard_transfer_info(
        &self,
        tasks_pool: &TransferTasksPool,
    ) -> Vec<ShardTransferInfo> {
        let mut shard_transfers = vec![];
        for shard_transfer in self.shard_transfers.read().iter() {
            let shard_id = shard_transfer.shard_id;
            let to_shard_id = shard_transfer.to_shard_id;
            let to = shard_transfer.to;
            let from = shard_transfer.from;
            let sync = shard_transfer.sync;
            let method = shard_transfer.method;
            let status = tasks_pool.get_task_status(&shard_transfer.key());
            shard_transfers.push(ShardTransferInfo {
                shard_id,
                to_shard_id,
                from,
                to,
                sync,
                method,
                comment: status.map(|p| p.comment),
            })
        }
        shard_transfers.sort_by_key(|k| k.shard_id);
        shard_transfers
    }

    pub fn get_resharding_operations_info(&self) -> Option<Vec<ReshardingInfo>> {
        let mut resharding_operations = vec![];

        // We eventually expect to extend this to multiple concurrent operations, which is why
        // we're using a list here
        let Some(resharding_state) = &*self.resharding_state.read() else {
            return None;
        };

        resharding_operations.push(ReshardingInfo {
            uuid: resharding_state.uuid,
            shard_id: resharding_state.shard_id,
            peer_id: resharding_state.peer_id,
            direction: resharding_state.direction,
            shard_key: resharding_state.shard_key.clone(),
        });

        resharding_operations.sort_by_key(|k| k.shard_id);
        Some(resharding_operations)
    }

    pub fn get_related_transfers(&self, shard_id: ShardId, peer_id: PeerId) -> Vec<ShardTransfer> {
        self.get_transfers(|transfer| {
            transfer.shard_id == shard_id && (transfer.from == peer_id || transfer.to == peer_id)
        })
    }

    pub fn get_shard_ids_by_key(&self, shard_key: &ShardKey) -> CollectionResult<HashSet<ShardId>> {
        match self.key_mapping.read().get(shard_key).cloned() {
            None => Err(CollectionError::bad_request(format!(
                "Shard key {shard_key} not found"
            ))),
            Some(ids) => Ok(ids),
        }
    }

    pub fn select_shards<'a>(
        &'a self,
        shard_selector: &'a ShardSelectorInternal,
    ) -> CollectionResult<Vec<(&'a ShardReplicaSet, Option<&'a ShardKey>)>> {
        let mut res = Vec::new();

        match shard_selector {
            ShardSelectorInternal::Empty => {
                debug_assert!(false, "Do not expect empty shard selector")
            }
            ShardSelectorInternal::All => {
                for (&shard_id, shard) in self.shards.iter() {
                    // Ignore a new resharding shard until it completed point migration
                    // The shard will be marked as active at the end of the migration stage
                    let resharding_migrating_up =
                        self.resharding_state.read().clone().is_some_and(|state| {
                            state.direction == ReshardingDirection::Up
                                && state.shard_id == shard_id
                                && state.stage < ReshardStage::ReadHashRingCommitted
                        });
                    if resharding_migrating_up {
                        continue;
                    }

                    let shard_key = self.shard_id_to_key_mapping.get(&shard_id);
                    res.push((shard, shard_key));
                }
            }
            ShardSelectorInternal::ShardKey(shard_key) => {
                for shard_id in self.get_shard_ids_by_key(shard_key)? {
                    if let Some(replica_set) = self.shards.get(&shard_id) {
                        res.push((replica_set, Some(shard_key)));
                    } else {
                        debug_assert!(false, "Shard id {shard_id} not found")
                    }
                }
            }
            ShardSelectorInternal::ShardKeys(shard_keys) => {
                for shard_key in shard_keys {
                    for shard_id in self.get_shard_ids_by_key(shard_key)? {
                        if let Some(replica_set) = self.shards.get(&shard_id) {
                            res.push((replica_set, Some(shard_key)));
                        } else {
                            debug_assert!(false, "Shard id {shard_id} not found")
                        }
                    }
                }
            }
            ShardSelectorInternal::ShardId(shard_id) => {
                if let Some(replica_set) = self.shards.get(shard_id) {
                    res.push((replica_set, self.shard_id_to_key_mapping.get(shard_id)));
                } else {
                    return Err(shard_not_found_error(*shard_id));
                }
            }
        }
        Ok(res)
    }

    pub fn len(&self) -> usize {
        self.shards.len()
    }

    pub fn is_empty(&self) -> bool {
        self.shards.is_empty()
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn load_shards(
        &mut self,
        collection_path: &Path,
        collection_id: &CollectionId,
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
    ) {
        let shard_number = collection_config.read().await.params.shard_number.get();

        let (shard_ids_list, shard_id_to_key_mapping) = match collection_config
            .read()
            .await
            .params
            .sharding_method
            .unwrap_or_default()
        {
            ShardingMethod::Auto => {
                let ids_list = (0..shard_number).collect::<Vec<_>>();
                let shard_id_to_key_mapping = HashMap::new();
                (ids_list, shard_id_to_key_mapping)
            }
            ShardingMethod::Custom => {
                let shard_id_to_key_mapping = self.get_shard_id_to_key_mapping();
                let ids_list = shard_id_to_key_mapping
                    .keys()
                    .cloned()
                    .sorted()
                    .collect::<Vec<_>>();
                (ids_list, shard_id_to_key_mapping.clone())
            }
        };

        // ToDo: remove after version 0.11.0
        for shard_id in shard_ids_list {
            let shard_key = self.get_shard_id_to_key_mapping().get(&shard_id).cloned();

            for (path, _shard_version, shard_type) in
                latest_shard_paths(collection_path, shard_id).await.unwrap()
            {
                let replica_set = ShardReplicaSet::load(
                    shard_id,
                    shard_key.clone(),
                    collection_id.clone(),
                    &path,
                    collection_config.clone(),
                    effective_optimizers_config.clone(),
                    shared_storage_config.clone(),
                    payload_index_schema.clone(),
                    channel_service.clone(),
                    on_peer_failure.clone(),
                    abort_shard_transfer.clone(),
                    this_peer_id,
                    update_runtime.clone(),
                    search_runtime.clone(),
                    optimizer_cpu_budget.clone(),
                )
                .await;

                let mut require_migration = true;
                match shard_type {
                    ShardType::Local => {
                        // deprecated
                        let local_shard = LocalShard::load(
                            shard_id,
                            collection_id.clone(),
                            &path,
                            collection_config.clone(),
                            effective_optimizers_config.clone(),
                            shared_storage_config.clone(),
                            payload_index_schema.clone(),
                            update_runtime.clone(),
                            search_runtime.clone(),
                            optimizer_cpu_budget.clone(),
                        )
                        .await
                        .unwrap();
                        replica_set
                            .set_local(local_shard, Some(ReplicaState::Active))
                            .await
                            .unwrap();
                    }
                    ShardType::Remote { peer_id } => {
                        // deprecated
                        replica_set
                            .add_remote(peer_id, ReplicaState::Active)
                            .await
                            .unwrap();
                    }
                    ShardType::Temporary => {
                        // deprecated
                        let temp_shard = LocalShard::load(
                            shard_id,
                            collection_id.clone(),
                            &path,
                            collection_config.clone(),
                            effective_optimizers_config.clone(),
                            shared_storage_config.clone(),
                            payload_index_schema.clone(),
                            update_runtime.clone(),
                            search_runtime.clone(),
                            optimizer_cpu_budget.clone(),
                        )
                        .await
                        .unwrap();

                        replica_set
                            .set_local(temp_shard, Some(ReplicaState::Partial))
                            .await
                            .unwrap();
                    }
                    ShardType::ReplicaSet => {
                        require_migration = false;
                        // nothing to do, replicate set should be loaded already
                    }
                }
                // Migrate shard config to replica set
                // Override existing shard configuration
                if require_migration {
                    ShardConfig::new_replica_set()
                        .save(&path)
                        .map_err(|e| panic!("Failed to save shard config {path:?}: {e}"))
                        .unwrap();
                }

                // Change local shards stuck in Initializing state to Active
                let local_peer_id = replica_set.this_peer_id();
                let not_distributed = !shared_storage_config.is_distributed;
                let is_local =
                    replica_set.this_peer_id() == local_peer_id && replica_set.is_local().await;
                let is_initializing =
                    replica_set.peer_state(local_peer_id) == Some(ReplicaState::Initializing);
                if not_distributed && is_local && is_initializing {
                    log::warn!("Local shard {collection_id}:{} stuck in Initializing state, changing to Active", replica_set.shard_id);
                    replica_set
                        .set_replica_state(local_peer_id, ReplicaState::Active)
                        .expect("Failed to set local shard state");
                }
                let shard_key = shard_id_to_key_mapping.get(&shard_id).cloned();
                self.add_shard(shard_id, replica_set, shard_key).unwrap();
            }
        }

        // If resharding, rebuild the hash rings because they'll be messed up
        if self.resharding_state.read().is_some() {
            self.rebuild_rings();
        }
    }

    pub fn assert_shard_exists(&self, shard_id: ShardId) -> CollectionResult<()> {
        match self.get_shard(shard_id) {
            Some(_) => Ok(()),
            None => Err(shard_not_found_error(shard_id)),
        }
    }

    async fn assert_shard_is_local(&self, shard_id: ShardId) -> CollectionResult<()> {
        let is_local_shard = self
            .is_shard_local(shard_id)
            .await
            .ok_or_else(|| shard_not_found_error(shard_id))?;

        if is_local_shard {
            Ok(())
        } else {
            Err(CollectionError::bad_input(format!(
                "Shard {shard_id} is not a local shard"
            )))
        }
    }

    async fn assert_shard_is_local_or_queue_proxy(
        &self,
        shard_id: ShardId,
    ) -> CollectionResult<()> {
        let is_local_shard = self
            .is_shard_local_or_queue_proxy(shard_id)
            .await
            .ok_or_else(|| shard_not_found_error(shard_id))?;

        if is_local_shard {
            Ok(())
        } else {
            Err(CollectionError::bad_input(format!(
                "Shard {shard_id} is not a local or queue proxy shard"
            )))
        }
    }

    /// Returns true if shard is explicitly local, false otherwise.
    pub async fn is_shard_local(&self, shard_id: ShardId) -> Option<bool> {
        match self.get_shard(shard_id) {
            Some(shard) => Some(shard.is_local().await),
            None => None,
        }
    }

    /// Returns true if shard is explicitly local or is queue proxy shard, false otherwise.
    pub async fn is_shard_local_or_queue_proxy(&self, shard_id: ShardId) -> Option<bool> {
        match self.get_shard(shard_id) {
            Some(shard) => Some(shard.is_local().await || shard.is_queue_proxy().await),
            None => None,
        }
    }

    /// Return a list of local shards, present on this peer
    pub async fn get_local_shards(&self) -> Vec<ShardId> {
        let mut res = Vec::with_capacity(1);
        for (shard_id, replica_set) in self.get_shards() {
            if replica_set.has_local_shard().await {
                res.push(shard_id);
            }
        }
        res
    }

    /// Count how many shard replicas are on the given peer.
    pub fn count_peer_shards(&self, peer_id: PeerId) -> usize {
        self.get_shards()
            .filter(|(_, replica_set)| replica_set.peer_state(peer_id).is_some())
            .count()
    }

    pub fn check_transfer_exists(&self, transfer_key: &ShardTransferKey) -> bool {
        self.shard_transfers
            .read()
            .iter()
            .any(|transfer| transfer_key.check(transfer))
    }

    pub fn get_transfer(&self, transfer_key: &ShardTransferKey) -> Option<ShardTransfer> {
        self.shard_transfers
            .read()
            .iter()
            .find(|transfer| transfer_key.check(transfer))
            .cloned()
    }

    pub fn get_transfers<F>(&self, mut predicate: F) -> Vec<ShardTransfer>
    where
        F: FnMut(&ShardTransfer) -> bool,
    {
        self.shard_transfers
            .read()
            .iter()
            .filter(|&transfer| predicate(transfer))
            .cloned()
            .collect()
    }

    pub fn get_outgoing_transfers(&self, current_peer_id: PeerId) -> Vec<ShardTransfer> {
        self.get_transfers(|transfer| transfer.from == current_peer_id)
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn list_shard_snapshots(
        &self,
        snapshots_path: &Path,
        shard_id: ShardId,
    ) -> CollectionResult<Vec<SnapshotDescription>> {
        self.assert_shard_is_local(shard_id).await?;

        let snapshots_path = Self::snapshots_path_for_shard_unchecked(snapshots_path, shard_id);

        let shard = self
            .get_shard(shard_id)
            .ok_or_else(|| shard_not_found_error(shard_id))?;
        let snapshot_manager = shard.get_snapshots_storage_manager()?;
        snapshot_manager.list_snapshots(&snapshots_path).await
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn create_shard_snapshot(
        &self,
        snapshots_path: &Path,
        collection_name: &str,
        shard_id: ShardId,
        temp_dir: &Path,
    ) -> CollectionResult<SnapshotDescription> {
        // - `snapshot_temp_dir` and `temp_file` are handled by `tempfile`
        //   and would be deleted, if future is cancelled

        let shard = self
            .get_shard(shard_id)
            .ok_or_else(|| shard_not_found_error(shard_id))?;

        if !shard.is_local().await && !shard.is_queue_proxy().await {
            return Err(CollectionError::bad_input(format!(
                "Shard {shard_id} is not a local or queue proxy shard"
            )));
        }

        let snapshot_file_name = format!(
            "{collection_name}-shard-{shard_id}-{}.snapshot",
            chrono::Utc::now().format("%Y-%m-%d-%H-%M-%S"),
        );

        let snapshot_temp_dir = tempfile::Builder::new()
            .prefix(&format!("{snapshot_file_name}-temp-"))
            .tempdir_in(temp_dir)?;

        let temp_file = tempfile::Builder::new()
            .prefix(&format!("{snapshot_file_name}-"))
            .suffix(".tar")
            .tempfile_in(temp_dir)?;

        let tar = BuilderExt::new_seekable_owned(File::create(temp_file.path())?);

        shard
            .create_snapshot(
                snapshot_temp_dir.path(),
                &tar,
                SnapshotFormat::Regular,
                false,
            )
            .await?;

        let snapshot_temp_dir_path = snapshot_temp_dir.path().to_path_buf();
        if let Err(err) = snapshot_temp_dir.close() {
            log::error!(
                "Failed to remove temporary directory {}: {err}",
                snapshot_temp_dir_path.display(),
            );
        }

        tar.finish().await?;

        let snapshot_path =
            Self::shard_snapshot_path_unchecked(snapshots_path, shard_id, snapshot_file_name)?;

        let snapshot_manager = shard.get_snapshots_storage_manager()?;
        let snapshot_description = snapshot_manager
            .store_file(temp_file.path(), &snapshot_path)
            .await;
        if snapshot_description.is_ok() {
            let _ = temp_file.keep();
        }
        snapshot_description
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn stream_shard_snapshot(
        shard: OwnedRwLockReadGuard<ShardHolder, ShardReplicaSet>,
        collection_name: &str,
        shard_id: ShardId,
        temp_dir: &Path,
    ) -> CollectionResult<SnapshotStream> {
        // - `snapshot_temp_dir` and `temp_file` are handled by `tempfile`
        //   and would be deleted, if future is cancelled

        if !shard.is_local().await && !shard.is_queue_proxy().await {
            return Err(CollectionError::bad_input(format!(
                "Shard {shard_id} is not a local or queue proxy shard"
            )));
        }

        let snapshot_file_name = format!(
            "{collection_name}-shard-{shard_id}-{}.snapshot",
            chrono::Utc::now().format("%Y-%m-%d-%H-%M-%S"),
        );

        let snapshot_temp_dir = tempfile::Builder::new()
            .prefix(&format!("{snapshot_file_name}-temp-"))
            .tempdir_in(temp_dir)?;

        let (read_half, write_half) = tokio::io::duplex(4096);

        tokio::spawn(async move {
            let tar = BuilderExt::new_streaming_owned(SyncIoBridge::new(write_half));

            shard
                .create_snapshot(
                    snapshot_temp_dir.path(),
                    &tar,
                    SnapshotFormat::Streamable,
                    false,
                )
                .await?;

            let snapshot_temp_dir_path = snapshot_temp_dir.path().to_path_buf();
            if let Err(err) = snapshot_temp_dir.close() {
                log::error!(
                    "Failed to remove temporary directory {}: {err}",
                    snapshot_temp_dir_path.display(),
                );
            }

            tar.finish().await?;

            CollectionResult::Ok(())
        });

        Ok(SnapshotStream::new_stream(
            FramedRead::new(read_half, BytesCodec::new()).map_ok(|bytes| bytes.freeze()),
            Some(snapshot_file_name),
        ))
    }

    /// # Cancel safety
    ///
    /// This method is *not* cancel safe.
    #[allow(clippy::too_many_arguments)]
    pub async fn restore_shard_snapshot(
        &self,
        snapshot_path: &Path,
        collection_name: &str,
        shard_id: ShardId,
        this_peer_id: PeerId,
        is_distributed: bool,
        temp_dir: &Path,
        cancel: cancel::CancellationToken,
    ) -> CollectionResult<()> {
        if !self.contains_shard(shard_id) {
            return Err(shard_not_found_error(shard_id));
        }

        if !temp_dir.exists() {
            std::fs::create_dir_all(temp_dir)?;
        }

        let snapshot_file_name = snapshot_path.file_name().unwrap().to_string_lossy();

        let snapshot_path = snapshot_path.to_path_buf();
        let snapshot_temp_dir = tempfile::Builder::new()
            .prefix(&format!(
                "{collection_name}-shard-{shard_id}-{snapshot_file_name}"
            ))
            .tempdir_in(temp_dir)?;

        let task = {
            let snapshot_temp_dir = snapshot_temp_dir.path().to_path_buf();

            cancel::blocking::spawn_cancel_on_token(
                cancel.child_token(),
                move |cancel| -> CollectionResult<_> {
                    let mut tar = open_snapshot_archive_with_validation(&snapshot_path)?;

                    if cancel.is_cancelled() {
                        return Err(cancel::Error::Cancelled.into());
                    }

                    tar.unpack(&snapshot_temp_dir)?;
                    drop(tar);

                    if cancel.is_cancelled() {
                        return Err(cancel::Error::Cancelled.into());
                    }

                    ShardReplicaSet::restore_snapshot(
                        &snapshot_temp_dir,
                        this_peer_id,
                        is_distributed,
                    )?;

                    Ok(())
                },
            )
        };

        task.await??;

        // `ShardHolder::recover_local_shard_from` is *not* cancel safe
        // (see `ShardReplicaSet::restore_local_replica_from`)
        let recovered = self
            .recover_local_shard_from(snapshot_temp_dir.path(), shard_id, cancel)
            .await?;

        if !recovered {
            return Err(CollectionError::bad_request(format!(
                "Invalid snapshot {snapshot_file_name}"
            )));
        }

        Ok(())
    }

    /// # Cancel safety
    ///
    /// This method is *not* cancel safe.
    pub async fn recover_local_shard_from(
        &self,
        snapshot_shard_path: &Path,
        shard_id: ShardId,
        cancel: cancel::CancellationToken,
    ) -> CollectionResult<bool> {
        // TODO:
        //   Check that shard snapshot is compatible with the collection
        //   (see `VectorsConfig::check_compatible_with_segment_config`)

        let replica_set = self
            .get_shard(shard_id)
            .ok_or_else(|| shard_not_found_error(shard_id))?;

        // `ShardReplicaSet::restore_local_replica_from` is *not* cancel safe
        replica_set
            .restore_local_replica_from(snapshot_shard_path, cancel)
            .await
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn get_shard_snapshot_path(
        &self,
        snapshots_path: &Path,
        shard_id: ShardId,
        snapshot_file_name: impl AsRef<Path>,
    ) -> CollectionResult<PathBuf> {
        self.assert_shard_is_local_or_queue_proxy(shard_id).await?;
        Self::shard_snapshot_path_unchecked(snapshots_path, shard_id, snapshot_file_name)
    }

    fn snapshots_path_for_shard_unchecked(snapshots_path: &Path, shard_id: ShardId) -> PathBuf {
        snapshots_path.join(format!("shards/{shard_id}"))
    }

    fn shard_snapshot_path_unchecked(
        snapshots_path: &Path,
        shard_id: ShardId,
        snapshot_file_name: impl AsRef<Path>,
    ) -> CollectionResult<PathBuf> {
        let snapshots_path = Self::snapshots_path_for_shard_unchecked(snapshots_path, shard_id);

        let snapshot_file_name = snapshot_file_name.as_ref();

        if snapshot_file_name.file_name() != Some(snapshot_file_name.as_os_str()) {
            return Err(CollectionError::not_found(format!(
                "Snapshot {}",
                snapshot_file_name.display(),
            )));
        }

        let snapshot_path = snapshots_path.join(snapshot_file_name);

        Ok(snapshot_path)
    }

    pub async fn remove_shards_at_peer(&self, peer_id: PeerId) -> CollectionResult<()> {
        for (_shard_id, replica_set) in self.get_shards() {
            replica_set.remove_peer(peer_id).await?;
        }
        Ok(())
    }

    /// Estimates the collections size based on local shard data. Returns `None` if no shard for the collection was found locally.
    pub async fn estimate_collection_size_stats(&self) -> Option<CollectionSizeStats> {
        if self.is_distributed().await {
            // In distributed, we estimate the whole collection size by using a single local shard and multiply by amount of shards in the collection.
            for shard in self.shards.iter() {
                if let Some(shard_stats) = shard.1.calculate_local_shard_stats().await {
                    // TODO(resharding) take into account the ongoing resharding and exclude shards that are being filled from multiplication.
                    // Project the single shards size to the full collection.
                    let collection_estimate = shard_stats.multiplied_with(self.shards.len());
                    return Some(collection_estimate);
                }
            }

            return None;
        }

        // Local mode: return collection size estimations using all shards.
        let mut stats = CollectionSizeStats::default();
        for shard in self.shards.iter() {
            if let Some(shard_stats) = shard.1.calculate_local_shard_stats().await {
                stats.accumulate_metrics_from(&shard_stats);
            }
        }

        Some(stats)
    }

    /// Returns `true` if the collection is distributed across multiple nodes.
    async fn is_distributed(&self) -> bool {
        stream::iter(self.shards.iter())
            .any(|i| async { i.1.has_remote_shard().await })
            .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ShardTransferChange {
    Start(ShardTransfer),
    Finish(ShardTransferKey),
    Abort(ShardTransferKey),
}

pub(crate) fn shard_not_found_error(shard_id: ShardId) -> CollectionError {
    CollectionError::NotFound {
        what: format!("shard {shard_id}"),
    }
}
