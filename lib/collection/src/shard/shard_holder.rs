use std::collections::HashMap;
use std::ops::Deref;

use tokio::sync::{RwLock, RwLockReadGuard};

use crate::hash_ring::HashRing;
use crate::operations::{OperationToShard, SplitByShard};
use crate::shard::{Shard, ShardId, ShardTransfer};

pub struct ShardHolder {
    shards: HashMap<ShardId, Shard>,
    shard_transfers: HashMap<ShardId, ShardTransfer>,
    ring: HashRing<ShardId>,
}

pub struct LockedShardHolder(RwLock<ShardHolder>);

impl LockedShardHolder {
    pub fn new(hashring: HashRing<ShardId>) -> Self {
        LockedShardHolder(RwLock::new(ShardHolder {
            shards: HashMap::new(),
            shard_transfers: HashMap::new(),
            ring: hashring,
        }))
    }



    pub async fn add_shard(&self, shard_id: ShardId, shard: Shard) {
        let mut shards = self.shards.write().await;
        shards.insert(shard_id, shard);
        self.ring.write().await.add(shard_id);
    }

    pub async fn remove_shard(&self, shard_id: ShardId) -> Option<Shard> {
        let mut shards = self.shards.write().await;
        let shard = shards.remove(&shard_id);
        self.ring.write().await.remove(&shard_id);
        shard
    }

    pub async fn get_shard<'a>(&'a self, shard_id: ShardId) -> Option<RwLockReadGuard<'a, Shard>> {
        let shards = self.shards.read().await;
        if shards.contains_key(&shard_id) {
            Some(RwLockReadGuard::map(self.shards.read().await, |shards| {
                shards.get(&shard_id).unwrap()
            }))
        } else {
            None
        }
    }

    pub async fn split_by_shard<O: SplitByShard>(&self, operation: O) -> OperationToShard<O> {
        let ring = self.ring.read().await;
        operation.split_by_shard(ring.deref())
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

        let shard_holder = ShardHolder::new(HashRing::fair(100));
        shard_holder.add_shard(2, Shard::Remote(shard)).await;

        let retrieved_shard = shard_holder.get_shard(2).await;

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
