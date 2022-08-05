use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::shard::{PeerId, ShardId};

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum ClusterOperations {
    /// Move shard to a different peer
    MoveShard(MoveShardOperation),
    /// Abort currently running shard moving operation
    AbortTransfer(AbortTransferOperation),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct MoveShardOperation {
    pub move_shard: MoveShard,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct AbortTransferOperation {
    pub abort_transfer: MoveShard,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct MoveShard {
    pub shard_id: ShardId,
    pub to_peer_id: PeerId,
    pub from_peer_id: PeerId,
}
