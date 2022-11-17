use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::config::CollectionConfig;
use crate::hash_ring::HashRing;
use crate::operations::types::{CollectionError, CollectionResult, ShardTransferInfo};
use crate::operations::{OperationToShard, SplitByShard};
use crate::save_on_disk::SaveOnDisk;
use crate::shards::channel_service::ChannelService;
use crate::shards::local_shard::LocalShard;
use crate::shards::replica_set::{OnPeerFailure, ReplicaState, ShardReplicaSet};
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_config::{ShardConfig, ShardType};
use crate::shards::shard_versioning::latest_shard_paths;
use crate::shards::transfer::shard_transfer::{ShardTransfer, ShardTransferKey};
use crate::shards::CollectionId;

const SHARD_TRANSFERS_FILE: &str = "shard_transfers";

pub struct ShardHolder {
    shards: HashMap<ShardId, ShardReplicaSet>,
    pub(crate) shard_transfers: SaveOnDisk<HashSet<ShardTransfer>>,
    ring: HashRing<ShardId>,
}

pub struct LockedShardHolder(pub RwLock<ShardHolder>);

impl ShardHolder {
    pub fn new(collection_path: &Path, hashring: HashRing<ShardId>) -> CollectionResult<Self> {
        let shard_transfers = SaveOnDisk::load_or_init(collection_path.join(SHARD_TRANSFERS_FILE))?;
        Ok(Self {
            shards: HashMap::new(),
            shard_transfers,
            ring: hashring,
        })
    }

    pub fn add_shard(&mut self, shard_id: ShardId, shard: ShardReplicaSet) {
        self.shards.insert(shard_id, shard);
        self.ring.add(shard_id);
    }

    pub fn remove_shard(&mut self, shard_id: ShardId) -> Option<ShardReplicaSet> {
        let shard = self.shards.remove(&shard_id);
        self.ring.remove(&shard_id);
        shard
    }

    /// Take shard
    ///
    /// remove shard and return ownership
    pub fn take_shard(&mut self, shard_id: ShardId) -> Option<ShardReplicaSet> {
        self.shards.remove(&shard_id)
    }

    /// Replace shard
    ///
    /// return old shard
    pub fn replace_shard(
        &mut self,
        shard_id: ShardId,
        shard: ShardReplicaSet,
    ) -> Option<ShardReplicaSet> {
        self.shards.insert(shard_id, shard)
    }

    pub fn contains_shard(&self, shard_id: &ShardId) -> bool {
        self.shards.contains_key(shard_id)
    }

    pub fn get_shard(&self, shard_id: &ShardId) -> Option<&ShardReplicaSet> {
        self.shards.get(shard_id)
    }

    pub fn get_mut_shard(&mut self, shard_id: &ShardId) -> Option<&mut ShardReplicaSet> {
        self.shards.get_mut(shard_id)
    }

    pub fn get_shards(&self) -> impl Iterator<Item = (&ShardId, &ShardReplicaSet)> {
        self.shards.iter()
    }

    pub fn all_shards(&self) -> impl Iterator<Item = &ShardReplicaSet> {
        self.shards.values()
    }

    pub fn split_by_shard<O: SplitByShard + Clone>(
        &self,
        operation: O,
    ) -> Vec<(&ShardReplicaSet, O)> {
        let operation_to_shard = operation.split_by_shard(&self.ring);
        let shard_ops: Vec<_> = match operation_to_shard {
            OperationToShard::ByShard(by_shard) => by_shard
                .into_iter()
                .map(|(shard_id, operation)| (self.shards.get(&shard_id).unwrap(), operation))
                .collect(),
            OperationToShard::ToAll(operation) => self
                .all_shards()
                .map(|shard| (shard, operation.clone()))
                .collect(),
        };
        shard_ops
    }

    pub fn register_start_shard_transfer(&self, transfer: ShardTransfer) -> CollectionResult<bool> {
        Ok(self
            .shard_transfers
            .write(|transfers| transfers.insert(transfer))?)
    }

    pub fn register_finish_transfer(&self, key: &ShardTransferKey) -> CollectionResult<bool> {
        Ok(self.shard_transfers.write(|transfers| {
            let before_remove = transfers.len();
            transfers.retain(|transfer| !key.check(transfer));
            before_remove != transfers.len() // `true` if something was removed
        })?)
    }

    pub fn get_shard_transfer_info(&self) -> Vec<ShardTransferInfo> {
        let mut shard_transfers = vec![];
        for shard_transfer in self.shard_transfers.read().iter() {
            let shard_id = shard_transfer.shard_id;
            let to = shard_transfer.to;
            let from = shard_transfer.from;
            let sync = shard_transfer.sync;
            shard_transfers.push(ShardTransferInfo {
                shard_id,
                from,
                to,
                sync,
            })
        }
        shard_transfers.sort_by_key(|k| k.shard_id);
        shard_transfers
    }

    pub fn set_shard_replica_state(
        &self,
        shard_id: ShardId,
        peer_id: PeerId,
        active: ReplicaState,
    ) -> CollectionResult<()> {
        let replica_set = self
            .get_shard(&shard_id)
            .ok_or_else(|| CollectionError::NotFound {
                what: format!("Shard {shard_id}"),
            })?;
        replica_set.set_replica_state(&peer_id, active)
    }

    pub fn target_shard(
        &self,
        shard_selection: Option<ShardId>,
    ) -> CollectionResult<Vec<&ShardReplicaSet>> {
        match shard_selection {
            None => Ok(self.all_shards().collect()),
            Some(shard_selection) => {
                let shard_opt = self.get_shard(&shard_selection);
                let shards = match shard_opt {
                    None => vec![],
                    Some(shard) => vec![shard],
                };
                Ok(shards)
            }
        }
    }

    pub async fn before_drop(&mut self) {
        let futures: FuturesUnordered<_> = self
            .shards
            .iter_mut()
            .map(|(_, shard)| shard.before_drop())
            .collect();
        futures.collect::<Vec<()>>().await;
    }

    pub fn len(&self) -> usize {
        self.shards.len()
    }

    pub fn is_empty(&self) -> bool {
        self.shards.is_empty()
    }

    pub async fn load_shards(
        &mut self,
        collection_path: &Path,
        collection_id: &CollectionId,
        shared_collection_config: Arc<RwLock<CollectionConfig>>,
        channel_service: ChannelService,
        on_peer_failure: OnPeerFailure,
        this_peer_id: PeerId,
    ) {
        let shard_number = shared_collection_config
            .read()
            .await
            .params
            .shard_number
            .get();
        // ToDo: remove after version 0.11.0
        for shard_id in 0..shard_number {
            for (path, _shard_version, shard_type) in
                latest_shard_paths(collection_path, shard_id).await.unwrap()
            {
                let replica_set = ShardReplicaSet::load(
                    shard_id,
                    collection_id.clone(),
                    &path,
                    shared_collection_config.clone(),
                    channel_service.clone(),
                    on_peer_failure.clone(),
                    this_peer_id,
                )
                .await;

                let mut require_migration = true;
                match shard_type {
                    ShardType::Local => {
                        let local_shard = LocalShard::load(
                            shard_id,
                            collection_id.clone(),
                            &path,
                            shared_collection_config.clone(),
                        )
                        .await
                        .unwrap();
                        replica_set
                            .set_local(local_shard, Some(ReplicaState::Active))
                            .await
                            .unwrap();
                    }
                    ShardType::Remote { peer_id } => {
                        replica_set
                            .add_remote(peer_id, ReplicaState::Active)
                            .await
                            .unwrap();
                    }
                    ShardType::Temporary => {
                        let temp_shard = LocalShard::load(
                            shard_id,
                            collection_id.clone(),
                            &path,
                            shared_collection_config.clone(),
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
                        .map_err(|e| panic!("Failed to save shard config {:?}: {}", path, e))
                        .unwrap();
                }
                self.add_shard(shard_id, replica_set);
            }
        }
    }
}

impl LockedShardHolder {
    pub fn new(shard_holder: ShardHolder) -> Self {
        Self(RwLock::new(shard_holder))
    }

    pub async fn read(&self) -> RwLockReadGuard<'_, ShardHolder> {
        self.0.read().await
    }

    pub async fn write(&self) -> RwLockWriteGuard<'_, ShardHolder> {
        self.0.write().await
    }
}
