use schemars::JsonSchema;
use segment::types::ShardKey;
use serde::{Deserialize, Serialize};

use super::shard::{PeerId, ShardId};

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ReshardingState {
    pub peer_id: PeerId,
    pub shard_id: ShardId,
    pub shard_key: Option<ShardKey>,
}

impl ReshardingState {
    #[allow(dead_code)]
    pub fn new(peer_id: PeerId, shard_id: ShardId, shard_key: Option<ShardKey>) -> Self {
        Self {
            peer_id,
            shard_id,
            shard_key,
        }
    }

    pub fn matches(&self, key: &ReshardingKey) -> bool {
        self.peer_id == key.peer_id
            && self.shard_id == key.shard_id
            && self.shard_key == key.shard_key
    }

    pub fn key(&self) -> ReshardingKey {
        ReshardingKey {
            peer_id: self.peer_id,
            shard_id: self.shard_id,
            shard_key: self.shard_key.clone(),
        }
    }
}

/// Unique identifier of a resharding operation
#[derive(Clone, Debug, Eq, PartialEq, Hash, Deserialize, Serialize, JsonSchema)]
pub struct ReshardingKey {
    pub peer_id: PeerId,
    pub shard_id: ShardId,
    pub shard_key: Option<ShardKey>,
}
