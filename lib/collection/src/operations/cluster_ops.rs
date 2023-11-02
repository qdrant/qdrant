use std::num::NonZeroU32;

use common::validation::validate_move_shard_different_peers;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use validator::{Validate, ValidationErrors};

use crate::shards::shard::{PeerId, ShardId, ShardKey};
use crate::shards::transfer::shard_transfer::ShardTransferMethod;

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(untagged, rename_all = "snake_case")]
pub enum ClusterOperations {
    /// Move shard to a different peer
    MoveShard(MoveShardOperation),
    /// Replicate shard to a different peer
    ReplicateShard(ReplicateShardOperation),
    /// Abort currently running shard moving operation
    AbortTransfer(AbortTransferOperation),
    /// Drop replica of a shard from a peer
    DropReplica(DropReplicaOperation),
    /// Create a custom shard partition for a given key
    CreateShardingKey(CreateShardingKeyOperation),
    /// Drop a custom shard partition for a given key
    DropShardingKey(DropShardingKeyOperation),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct CreateShardingKeyOperation {
    pub create_sharding_key: CreateShardingKey,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct DropShardingKeyOperation {
    pub drop_sharding_key: DropShardingKey,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct CreateShardingKey {
    pub shard_key: ShardKey,
    /// How many shards to create for this key
    /// If not specified, will use the default value from config
    pub shards_number: Option<NonZeroU32>,
    /// How many replicas to create for each shard
    /// If not specified, will use the default value from config
    pub replication_factor: Option<NonZeroU32>,
    /// Placement of shards for this key
    /// List of peer ids, that can be used to place shards for this key
    /// If not specified, will be randomly placed among all peers
    #[serde(default)]
    pub placement: Option<Vec<PeerId>>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct DropShardingKey {
    pub shard_key: ShardKey,
}

impl Validate for ClusterOperations {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            ClusterOperations::MoveShard(op) => op.validate(),
            ClusterOperations::ReplicateShard(op) => op.validate(),
            ClusterOperations::AbortTransfer(op) => op.validate(),
            ClusterOperations::DropReplica(op) => op.validate(),
            ClusterOperations::CreateShardingKey(op) => op.validate(),
            ClusterOperations::DropShardingKey(op) => op.validate(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct MoveShardOperation {
    #[validate]
    pub move_shard: MoveShard,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct ReplicateShardOperation {
    #[validate]
    pub replicate_shard: MoveShard,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct DropReplicaOperation {
    #[validate]
    pub drop_replica: Replica,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct AbortTransferOperation {
    #[validate]
    pub abort_transfer: MoveShard,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct MoveShard {
    pub shard_id: ShardId,
    pub to_peer_id: PeerId,
    pub from_peer_id: PeerId,
    /// Method for transferring the shard from one node to another
    pub method: Option<ShardTransferMethod>,
}

impl Validate for MoveShard {
    fn validate(&self) -> Result<(), ValidationErrors> {
        validate_move_shard_different_peers(self.from_peer_id, self.to_peer_id)
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Replica {
    pub shard_id: ShardId,
    pub peer_id: PeerId,
}
