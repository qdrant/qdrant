use std::collections::HashMap;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::hash_ring::HashRing;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::operations::{OperationToShard, SplitByShard};
use crate::shard::{PeerId, Shard, ShardId, ShardTransfer};

pub struct ShardHolder {
    shards: HashMap<ShardId, Shard>,
    shard_transfers: HashMap<ShardId, ShardTransfer>,
    ring: HashRing<ShardId>,
}

pub struct LockedShardHolder(pub RwLock<ShardHolder>);

impl ShardHolder {
    pub fn new(hashring: HashRing<ShardId>) -> Self {
        Self {
            shards: HashMap::new(),
            shard_transfers: HashMap::new(),
            ring: hashring,
        }
    }

    pub async fn add_shard(&mut self, shard_id: ShardId, shard: Shard) {
        self.shards.insert(shard_id, shard);
        self.ring.add(shard_id);
    }

    pub async fn remove_shard(&mut self, shard_id: ShardId) -> Option<Shard> {
        let shard = self.shards.remove(&shard_id);
        self.ring.remove(&shard_id);
        shard
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

    pub async fn split_by_shard<O: SplitByShard + Clone>(&self, operation: O) -> Vec<(&Shard, O)> {
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

    pub fn finish_transfer(&mut self, shard_id: ShardId) -> CollectionResult<()> {
        let transfer = self.shard_transfers.remove(&shard_id).ok_or_else(|| {
            CollectionError::service_error(format!(
                "Shard transfer data for {shard_id} is absent at the end of the transfer."
            ))
        })?;
        if let Shard::Remote(shard) = self.shards.get_mut(&shard_id).ok_or_else(|| {
            CollectionError::service_error("Shard {shard_id} is absent".to_owned())
        })? {
            shard.peer_id = transfer.to;
        };
        Ok(())
    }

    pub fn local_shard_by_id(&self, id: ShardId) -> CollectionResult<&Shard> {
        match self.shards.get(&id) {
            None => Err(CollectionError::bad_shard_selection(format!(
                "Shard {} does not exist",
                id
            ))),
            Some(Shard::Remote(_)) => Err(CollectionError::bad_shard_selection(format!(
                "Shard {} is not local on peer",
                id
            ))),
            Some(shard @ Shard::Local(_)) => Ok(shard),
            Some(shard @ Shard::Proxy(_)) => Ok(shard),
        }
    }

    pub fn target_shards(&self, shard_selection: Option<ShardId>) -> CollectionResult<Vec<&Shard>> {
        match shard_selection {
            None => Ok(self.all_shards().collect()),
            Some(shard_selection) => {
                let local_shard = self.local_shard_by_id(shard_selection)?;
                Ok(vec![local_shard])
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
}

impl LockedShardHolder {
    pub fn new(shard_holder: ShardHolder) -> Self {
        Self(RwLock::new(shard_holder))
    }

    async fn get_shard(&self, shard_id: ShardId) -> Option<RwLockReadGuard<'_, Shard>> {
        let holder = self.0.read().await;
        RwLockReadGuard::try_map(holder, |h| h.shards.get(&shard_id)).ok()
    }

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
        shard_holder.add_shard(2, Shard::Remote(shard)).await;
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
