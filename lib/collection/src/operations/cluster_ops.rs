use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::shard::{PeerId, ShardId};

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ClusterOperations {
    /// Move shard to a different peer
    #[serde(rename = "move_shard")]
    MoveShard(MoveShard),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct MoveShard {
    pub shard_id: ShardId,
    pub peer_id: PeerId,
}
