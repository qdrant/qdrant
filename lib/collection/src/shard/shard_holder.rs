use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::config::CollectionConfig;
use crate::hash_ring::HashRing;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::operations::{OperationToShard, SplitByShard};
use crate::shard::local_shard::LocalShard;
use crate::shard::remote_shard::RemoteShard;
use crate::shard::shard_config::ShardType;
use crate::shard::shard_versioning::latest_shard_paths;
use crate::shard::Shard::Local;
use crate::shard::{ChannelService, CollectionId, PeerId, Shard, ShardId, ShardTransfer};

pub struct ShardHolder {
    shards: HashMap<ShardId, Shard>,
    shard_transfers: HashMap<ShardId, ShardTransfer>,
    temporary_shards: HashMap<ShardId, Shard>,
    ring: HashRing<ShardId>,
}

pub struct LockedShardHolder(pub RwLock<ShardHolder>);

impl ShardHolder {
    pub fn new(hashring: HashRing<ShardId>) -> Self {
        Self {
            shards: HashMap::new(),
            shard_transfers: HashMap::new(),
            temporary_shards: HashMap::new(),
            ring: hashring,
        }
    }

    pub fn add_shard(&mut self, shard_id: ShardId, shard: Shard) {
        self.shards.insert(shard_id, shard);
        self.ring.add(shard_id);
    }

    pub fn remove_shard(&mut self, shard_id: ShardId) -> Option<Shard> {
        let shard = self.shards.remove(&shard_id);
        self.ring.remove(&shard_id);
        shard
    }

    /// Take shard
    ///
    /// remove shard and return ownership
    pub fn take_shard(&mut self, shard_id: ShardId) -> Option<Shard> {
        self.shards.remove(&shard_id)
    }

    /// Replace shard
    ///
    /// return old shard
    pub fn replace_shard(&mut self, shard_id: ShardId, shard: Shard) -> Option<Shard> {
        let old_shard = self.shards.remove(&shard_id);
        self.shards.insert(shard_id, shard);
        old_shard
    }

    pub fn get_shard(&self, shard_id: &ShardId) -> Option<&Shard> {
        self.shards.get(shard_id)
    }

    pub fn get_shards(&self) -> impl Iterator<Item = (&ShardId, &Shard)> {
        self.shards.iter()
    }

    pub fn all_shards(&self) -> impl Iterator<Item = &Shard> {
        self.shards.values()
    }

    pub fn get_temporary_shard(&self, shard_id: &ShardId) -> Option<&Shard> {
        self.temporary_shards.get(shard_id)
    }

    pub fn all_temporary_shards(&self) -> impl Iterator<Item = &Shard> {
        self.temporary_shards.values()
    }

    pub fn split_by_shard<O: SplitByShard + Clone>(&self, operation: O) -> Vec<(&Shard, O)> {
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

    /// Add temporary shard
    pub fn add_temporary_shard(
        &mut self,
        shard_id: ShardId,
        temporary_shard: LocalShard,
    ) -> Option<Shard> {
        self.temporary_shards
            .insert(shard_id, Local(temporary_shard))
    }

    /// Remove temporary shard
    pub fn remove_temporary_shard(&mut self, shard_id: ShardId) -> Option<Shard> {
        self.temporary_shards.remove(&shard_id)
    }

    pub fn start_shard_transfer(
        &mut self,
        shard_id: ShardId,
        to_peer: PeerId,
        this_peer: PeerId,
    ) -> CollectionResult<ShardTransfer> {
        let shard = self.shards.get(&shard_id).ok_or_else(|| {
            CollectionError::service_error("Shard {shard_id} is absent".to_owned())
        })?;
        let from_peer = shard.peer_id(this_peer);
        let shard_transfer = ShardTransfer {
            from: from_peer,
            to: to_peer,
        };
        self.shard_transfers
            .insert(shard_id, shard_transfer.clone());
        Ok(shard_transfer)
    }

    pub async fn finish_transfer(
        &mut self,
        collection_id: &CollectionId,
        shard_id: ShardId,
    ) -> CollectionResult<()> {
        // remove on-going transfer for `shard_id`
        let transfer = self.shard_transfers.remove(&shard_id).ok_or_else(|| {
            CollectionError::service_error(format!(
                "Shard transfer data for {collection_id}:{shard_id} is absent at the end of the transfer."
            ))
        })?;

        // update peer's id if `shard_id` is a remote shard
        if let Shard::Remote(shard) = self.shards.get_mut(&shard_id).ok_or_else(|| {
            CollectionError::service_error("Shard {collection_id}:{shard_id} is absent".to_owned())
        })? {
            shard.peer_id = transfer.to;
        };
        Ok(())
    }

    pub fn target_shards(&self, shard_selection: Option<ShardId>) -> CollectionResult<Vec<&Shard>> {
        match shard_selection {
            None => Ok(self.all_shards().collect()),
            Some(shard_selection) => {
                let shard_opt = self.get_shard(&shard_selection);
                let target_shard = match shard_opt {
                    None => {
                        // check if a temporary shard exist for the shard_selection
                        let temporary_shard_opt = self.get_temporary_shard(&shard_selection);
                        match temporary_shard_opt {
                            Some(temp) => temp,
                            None => {
                                return Err(CollectionError::bad_shard_selection(format!(
                                    "Shard {} does not exist",
                                    shard_selection
                                )))
                            }
                        }
                    }
                    Some(shard) => match *shard {
                        Shard::Local(_) => shard,
                        Shard::Proxy(_) => shard,
                        Shard::Remote(_) => {
                            // check temporary shards if the target is a remote shard
                            let temporary_shard_opt = self.get_temporary_shard(&shard_selection);
                            match temporary_shard_opt {
                                None => shard, // forward to the remote shard
                                Some(temp) => temp,
                            }
                        }
                    },
                };
                Ok(vec![target_shard])
            }
        }
    }

    pub async fn before_drop(&mut self) {
        let futures: FuturesUnordered<_> = self
            .shards
            .iter_mut()
            .chain(self.temporary_shards.iter_mut())
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
    ) {
        let shard_number = shared_collection_config
            .read()
            .await
            .params
            .shard_number
            .get();

        for shard_id in 0..shard_number {
            for (path, _shard_version, shard_type) in
                latest_shard_paths(collection_path, shard_id).await.unwrap()
            {
                match shard_type {
                    ShardType::Local => {
                        self.add_shard(
                            shard_id,
                            Shard::Local(
                                LocalShard::load(
                                    shard_id,
                                    collection_id.clone(),
                                    &path,
                                    shared_collection_config.clone(),
                                )
                                .await,
                            ),
                        );
                    }
                    ShardType::Remote { peer_id } => {
                        let shard = RemoteShard::new(
                            shard_id,
                            collection_id.clone(),
                            peer_id,
                            channel_service.clone(),
                        );
                        self.add_shard(shard_id, Shard::Remote(shard));
                    }
                    ShardType::Temporary => {
                        let replaces_shard = self.add_temporary_shard(
                            shard_id,
                            LocalShard::load(
                                shard_id,
                                collection_id.clone(),
                                &path,
                                shared_collection_config.clone(),
                            )
                            .await,
                        );
                        debug_assert!(replaces_shard.is_none())
                    }
                }
            }
        }
    }
}

impl LockedShardHolder {
    pub fn new(shard_holder: ShardHolder) -> Self {
        Self(RwLock::new(shard_holder))
    }

    async fn get_shard(&self, shard_id: ShardId) -> Option<RwLockReadGuard<'_, Shard>> {
        let holder = self.0.read().await;
        RwLockReadGuard::try_map(holder, |h| h.shards.get(&shard_id)).ok()
    }

    /// Fails if the shard is not found or not local.
    pub async fn local_shard_by_id(
        &self,
        id: ShardId,
    ) -> CollectionResult<RwLockReadGuard<'_, Shard>> {
        let shard_opt = self.get_shard(id).await;
        match shard_opt {
            None => Err(CollectionError::bad_shard_selection(format!(
                "Shard {} does not exist",
                id
            ))),
            Some(shard) => match &*shard {
                Shard::Local(_) => Ok(shard),
                Shard::Proxy(_) => Ok(shard),
                Shard::Remote(_) => Err(CollectionError::bad_shard_selection(format!(
                    "Shard {} is not local on peer",
                    id
                ))),
            },
        }
    }

    pub async fn read(&self) -> RwLockReadGuard<'_, ShardHolder> {
        self.0.read().await
    }

    pub async fn write(&self) -> RwLockWriteGuard<'_, ShardHolder> {
        self.0.write().await
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;
    use crate::shard::remote_shard::RemoteShard;
    use crate::shard::ChannelService;

    #[tokio::test]
    async fn test_shard_holder() {
        let shard_dir = TempDir::new("shard").unwrap();

        let shard = RemoteShard::init(
            2,
            "test_collection".to_string(),
            123,
            shard_dir.path().to_owned(),
            ChannelService::default(),
        )
        .unwrap();

        let mut shard_holder = ShardHolder::new(HashRing::fair(100));
        shard_holder.add_shard(2, Shard::Remote(shard));
        let locked_shard_holder = LockedShardHolder::new(shard_holder);

        let retrieved_shard = locked_shard_holder.get_shard(2).await;

        match retrieved_shard {
            Some(shard) => match &*shard {
                Shard::Remote(shard) => {
                    assert_eq!(shard.id, 2);
                }
                _ => panic!("Wrong shard type"),
            },
            None => {
                panic!("Shard not found");
            }
        }
    }
}
