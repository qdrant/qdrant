use crate::{CollectionConfig, CollectionResult, PeerId, ShardConfig, ShardId, ShardType};
use std::collections::HashMap;
use std::path::Path;

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

    /// Read remote & local shard info from file system
    pub fn from_local_state(collection_path: &Path) -> CollectionResult<Self> {
        let config = CollectionConfig::load(collection_path).unwrap_or_else(|err| {
            panic!(
                "Can't read collection config due to {}\nat {}",
                err,
                collection_path.to_str().unwrap()
            )
        });
        let shard_number = config.params.shard_number.get();
        let mut local_shards = Vec::new();
        let mut remote_shards = Vec::new();

        for shard_id in 0..shard_number {
            let shard_path = crate::shard_path(collection_path, shard_id);
            let shard_config = ShardConfig::load(&shard_path)?;
            match shard_config.r#type {
                ShardType::Local => local_shards.push(shard_id),
                ShardType::Remote { peer_id } => remote_shards.push((shard_id, peer_id)),
            }
        }

        Ok(Self::new(local_shards, remote_shards))
    }

    pub fn shard_count(&self) -> usize {
        self.local.len() + self.remote.len()
    }
}
