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

    pub fn key(&self) -> ReshardingKey {
        ReshardingKey {
            peer_id: self.peer_id,
            shard_id: self.shard_id,
            shard_key: self.shard_key.clone(),
        }
    }
}

/// Unique identifier of a resharding operation
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReshardingKey {
    pub peer_id: PeerId,
    pub shard_id: ShardId,
    pub shard_key: Option<ShardKey>,
}
