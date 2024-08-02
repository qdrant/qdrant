use std::num::NonZeroU32;

use common::validation::validate_shard_different_peers;
use schemars::JsonSchema;
use segment::types::ShardKey;
use serde::{Deserialize, Serialize};
use validator::{Validate, ValidationErrors};

use crate::shards::shard::{PeerId, ShardId};
use crate::shards::transfer::ShardTransferMethod;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ClusterOperations {
    /// Move shard to a different peer
    MoveShard(MoveShard),
    /// Replicate shard to a different peer
    ReplicateShard(ReplicateShard),
    /// Abort currently running shard moving operation
    AbortTransfer(AbortShardTransfer),
    /// Restart transfer
    RestartTransfer(RestartTransfer),

    /// Drop replica of a shard from a peer
    DropReplica(Replica),

    /// Create a custom shard partition for a given key
    CreateShardingKey(CreateShardingKey),
    /// Drop a custom shard partition for a given key
    DropShardingKey(DropShardingKey),

    /// Start resharding
    #[schemars(skip)]
    StartResharding(StartResharding),
    /// Abort resharding
    #[schemars(skip)]
    AbortResharding(AbortResharding),

    #[schemars(skip)]
    CommitReadHashRing(CommitReadHashRing),
    #[schemars(skip)]
    CommitWriteHashRing(CommitWriteHashRing),
}

impl Validate for ClusterOperations {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            ClusterOperations::MoveShard(op) => op.validate(),
            ClusterOperations::ReplicateShard(op) => op.validate(),
            ClusterOperations::AbortTransfer(op) => op.validate(),
            ClusterOperations::RestartTransfer(op) => op.validate(),
            ClusterOperations::DropReplica(op) => op.validate(),
            ClusterOperations::CreateShardingKey(op) => op.validate(),
            ClusterOperations::DropShardingKey(op) => op.validate(),
            ClusterOperations::StartResharding(op) => op.validate(),
            ClusterOperations::AbortResharding(op) => op.validate(),
            ClusterOperations::CommitReadHashRing(op) => op.validate(),
            ClusterOperations::CommitWriteHashRing(op) => op.validate(),
        }
    }
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MoveShard {
    pub shard_id: ShardId,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(skip)]
    pub to_shard_id: Option<ShardId>,
    pub to_peer_id: PeerId,
    pub from_peer_id: PeerId,
    /// Method for transferring the shard from one node to another
    pub method: Option<ShardTransferMethod>,
}

impl Validate for MoveShard {
    fn validate(&self) -> Result<(), ValidationErrors> {
        validate_shard_different_peers(self.from_peer_id, self.to_peer_id)
    }
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ReplicateShard {
    pub shard_id: ShardId,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(skip)]
    pub to_shard_id: Option<ShardId>,
    pub to_peer_id: PeerId,
    pub from_peer_id: PeerId,
    /// Method for transferring the shard from one node to another
    pub method: Option<ShardTransferMethod>,
}

impl Validate for ReplicateShard {
    fn validate(&self) -> Result<(), ValidationErrors> {
        validate_shard_different_peers(self.from_peer_id, self.to_peer_id)
    }
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AbortShardTransfer {
    pub shard_id: ShardId,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(skip)]
    pub to_shard_id: Option<ShardId>,
    pub to_peer_id: PeerId,
    pub from_peer_id: PeerId,
}

impl Validate for AbortShardTransfer {
    fn validate(&self) -> Result<(), ValidationErrors> {
        validate_shard_different_peers(self.from_peer_id, self.to_peer_id)
    }
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct RestartTransfer {
    pub shard_id: ShardId,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(skip)]
    pub to_shard_id: Option<ShardId>,
    pub from_peer_id: PeerId,
    pub to_peer_id: PeerId,
    pub method: ShardTransferMethod,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
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
    pub placement: Option<Vec<PeerId>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct DropShardingKey {
    pub shard_key: ShardKey,
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct Replica {
    pub shard_id: ShardId,
    pub peer_id: PeerId,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct StartResharding {
    pub direction: ReshardingDirection,
    pub peer_id: Option<PeerId>,
    pub shard_key: Option<ShardKey>,
}

/// Resharding direction, scale up or down in number of shards
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReshardingDirection {
    /// Scale up, add a new shard
    Up,
    /// Scale down, remove a shard
    Down,
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct AbortResharding {}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct CommitReadHashRing {}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct CommitWriteHashRing {}
