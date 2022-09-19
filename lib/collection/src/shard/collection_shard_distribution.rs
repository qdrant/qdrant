use std::collections::{HashMap, HashSet};

use crate::collection_state::ShardInfo;
use crate::shard::{PeerId, ShardId};

#[derive(Debug)]
pub enum ShardType {
    Local,
    Remote(PeerId),
    ReplicaSet {
        local: bool,
        remote: HashSet<PeerId>,
    },
}

#[derive(Debug)]
pub struct CollectionShardDistribution {
    pub shards: HashMap<ShardId, ShardType>,
}

impl CollectionShardDistribution {
    pub fn all_local(shard_number: Option<u32>) -> Self {
        Self {
            // This method is called only when distributed deployment is disabled
            // so if not specified it will suggest 1 shard per collection for better performance.
            shards: (0..shard_number.unwrap_or(1))
                .map(|shard_id| (shard_id, ShardType::Local))
                .collect(),
        }
    }

    pub fn from_shards_info(this_peer: PeerId, shards_info: HashMap<ShardId, ShardInfo>) -> Self {
        Self {
            shards: shards_info
                .into_iter()
                .map(|(shard_id, info)| match info {
                    ShardInfo::ReplicaSet { replicas } => (
                        shard_id,
                        ShardType::ReplicaSet {
                            local: replicas.contains_key(&this_peer),
                            remote: replicas.into_keys().filter(|id| id != &this_peer).collect(),
                        },
                    ),
                    ShardInfo::Single(peer_id) => {
                        if peer_id == this_peer {
                            (shard_id, ShardType::Local)
                        } else {
                            (shard_id, ShardType::Remote(peer_id))
                        }
                    }
                })
                .collect(),
        }
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn shard_replica_count(&self) -> usize {
        self.shards
            .values()
            .map(|shard| match shard {
                ShardType::ReplicaSet { local, remote } => *local as usize + remote.len(),
                _ => 1,
            })
            .sum()
    }
}
