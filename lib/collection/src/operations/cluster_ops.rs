use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::shards::shard::{PeerId, ShardId};

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum ClusterOperations {
    /// Move shard to a different peer
    MoveShard(MoveShardOperation),
    /// Replicate shard to a different peer
    ReplicateShard(ReplicateShardOperation),
    /// Abort currently running shard moving operation
    AbortTransfer(AbortTransferOperation),
    /// Drop replica of a shard from a peer
    DropReplica(DropReplicaOperation),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct MoveShardOperation {
    pub move_shard: MoveShard,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct ReplicateShardOperation {
    pub replicate_shard: MoveShard,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct DropReplicaOperation {
    pub drop_replica: Replica,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct AbortTransferOperation {
    pub abort_transfer: MoveShard,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct MoveShard {
    pub shard_id: ShardId,
    pub to_peer_id: PeerId,
    pub from_peer_id: PeerId,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Replica {
    pub shard_id: ShardId,
    pub peer_id: PeerId,
}

impl Validate for ClusterOperations {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            ClusterOperations::MoveShard(op) => op.validate(),
            ClusterOperations::ReplicateShard(op) => op.validate(),
            ClusterOperations::AbortTransfer(op) => op.validate(),
            ClusterOperations::DropReplica(op) => op.validate(),
        }
    }
}
