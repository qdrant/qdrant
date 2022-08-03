use std::collections::HashMap;

use crate::shard::{PeerId, ShardId};

#[derive(Debug)]
pub struct CollectionShardDistribution {
    pub local: Vec<ShardId>,
    pub remote: Vec<(ShardId, PeerId)>,
}

impl CollectionShardDistribution {
    pub fn new(local: Vec<ShardId>, remote: Vec<(ShardId, PeerId)>) -> Self {
        Self { local, remote }
    }

    pub fn all_local(shard_number: Option<u32>) -> Self {
        Self {
            // This method is called only when distributed deployment is disabled
            // so if not specified it will suggest 1 shard per collection for better performance.
            local: (0..shard_number.unwrap_or(1)).collect(),
            remote: vec![],
        }
    }

    pub fn from_shard_to_peer(this_peer: PeerId, shard_to_peer: &HashMap<ShardId, PeerId>) -> Self {
        let local = shard_to_peer
            .iter()
            .filter_map(|(shard, peer)| {
                if peer == &this_peer {
                    Some(*shard)
                } else {
                    None
                }
            })
            .collect();

        let remote = shard_to_peer
            .iter()
            .filter_map(|(&shard, &peer)| {
                if peer != this_peer {
                    Some((shard, peer))
                } else {
                    None
                }
            })
            .clone()
            .collect();

        Self { local, remote }
    }

    pub fn shard_count(&self) -> usize {
        self.local.len() + self.remote.len()
    }
}
