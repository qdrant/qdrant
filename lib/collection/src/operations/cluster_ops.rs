use std::num::NonZeroU32;

use common::validation::validate_shard_different_peers;
use schemars::JsonSchema;
use segment::types::ShardKey;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use validator::{Validate, ValidationErrors};

use crate::shards::replica_set::ReplicaState;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::transfer::ShardTransferMethod;

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
    /// Restart transfer
    RestartTransfer(RestartTransferOperation),

    /// Start resharding
    StartResharding(StartReshardingOperation),
    /// Finish migrating points on specified shard, mark shard as `Active`
    #[schemars(skip)] // hide for internal use
    FinishMigratingPoints(FinishMigratingPointsOperation),
    /// Commit read hashring
    #[schemars(skip)] // hide for internal use
    CommitReadHashRing(CommitReadHashRingOperation),
    /// Commit write hashring
    #[schemars(skip)] // hide for internal use
    CommitWriteHashRing(CommitWriteHashRingOperation),
    /// Finish resharding
    #[schemars(skip)] // hide for internal use
    FinishResharding(FinishReshardingOperation),
    /// Abort resharding
    AbortResharding(AbortReshardingOperation),
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
pub struct RestartTransferOperation {
    pub restart_transfer: RestartTransfer,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct CreateShardingKey {
    pub shard_key: ShardKey,
    /// How many shards to create for this key
    /// If not specified, will use the default value from config
    #[serde(alias = "shard_number")]
    pub shards_number: Option<NonZeroU32>,
    /// How many replicas to create for each shard
    /// If not specified, will use the default value from config
    pub replication_factor: Option<NonZeroU32>,
    /// Placement of shards for this key
    /// List of peer ids, that can be used to place shards for this key
    /// If not specified, will be randomly placed among all peers
    pub placement: Option<Vec<PeerId>>,
    /// Initial state of the shards for this key
    /// If not specified, will be `Initializing` first and then `Active`

    #[schemars(skip)] // unstable feature, hide for internal use
    pub initial_state: Option<ReplicaState>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct DropShardingKey {
    pub shard_key: ShardKey,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct RestartTransfer {
    pub shard_id: ShardId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(skip)] // hide for internal use
    pub to_shard_id: Option<ShardId>,
    pub from_peer_id: PeerId,
    pub to_peer_id: PeerId,
    pub method: ShardTransferMethod,
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
            ClusterOperations::RestartTransfer(op) => op.validate(),
            ClusterOperations::StartResharding(op) => op.validate(),
            ClusterOperations::FinishMigratingPoints(op) => op.validate(),
            ClusterOperations::CommitReadHashRing(op) => op.validate(),
            ClusterOperations::CommitWriteHashRing(op) => op.validate(),
            ClusterOperations::FinishResharding(op) => op.validate(),
            ClusterOperations::AbortResharding(op) => op.validate(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct MoveShardOperation {
    #[validate(nested)]
    pub move_shard: MoveShard,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct ReplicateShardOperation {
    #[validate(nested)]
    pub replicate_shard: ReplicateShard,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct DropReplicaOperation {
    #[validate(nested)]
    pub drop_replica: Replica,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct AbortTransferOperation {
    #[validate(nested)]
    pub abort_transfer: AbortShardTransfer,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct StartReshardingOperation {
    #[validate(nested)]
    pub start_resharding: StartResharding,
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct FinishMigratingPointsOperation {
    #[validate(nested)]
    pub finish_migrating_points: FinishMigratingPoints,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct AbortReshardingOperation {
    #[validate(nested)]
    pub abort_resharding: AbortResharding,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct CommitReadHashRingOperation {
    #[validate(nested)]
    pub commit_read_hash_ring: CommitReadHashRing,
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct CommitWriteHashRingOperation {
    #[validate(nested)]
    pub commit_write_hash_ring: CommitWriteHashRing,
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct FinishReshardingOperation {
    #[validate(nested)]
    pub finish_resharding: FinishResharding,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct ReplicateShard {
    pub shard_id: ShardId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(skip)] // hide for internal use
    pub to_shard_id: Option<ShardId>,
    pub to_peer_id: PeerId,
    pub from_peer_id: PeerId,
    /// Method for transferring the shard from one node to another
    pub method: Option<ShardTransferMethod>,
}

impl Validate for ReplicateShard {
    fn validate(&self) -> Result<(), ValidationErrors> {
        validate_shard_different_peers(
            self.from_peer_id,
            self.to_peer_id,
            self.shard_id,
            self.to_shard_id,
        )
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct MoveShard {
    pub shard_id: ShardId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(skip)] // hide for internal use
    pub to_shard_id: Option<ShardId>,
    pub to_peer_id: PeerId,
    pub from_peer_id: PeerId,
    /// Method for transferring the shard from one node to another
    pub method: Option<ShardTransferMethod>,
}

impl Validate for MoveShard {
    fn validate(&self) -> Result<(), ValidationErrors> {
        validate_shard_different_peers(
            self.from_peer_id,
            self.to_peer_id,
            self.shard_id,
            self.to_shard_id,
        )
    }
}

impl Validate for AbortShardTransfer {
    fn validate(&self) -> Result<(), ValidationErrors> {
        validate_shard_different_peers(
            self.from_peer_id,
            self.to_peer_id,
            self.shard_id,
            self.to_shard_id,
        )
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Replica {
    pub shard_id: ShardId,
    pub peer_id: PeerId,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct AbortShardTransfer {
    pub shard_id: ShardId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(skip)] // hide for internal use
    pub to_shard_id: Option<ShardId>,
    pub to_peer_id: PeerId,
    pub from_peer_id: PeerId,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct StartResharding {
    #[schemars(skip)]
    pub uuid: Option<Uuid>,
    pub direction: ReshardingDirection,
    pub peer_id: Option<PeerId>,
    pub shard_key: Option<ShardKey>,
}

/// Resharding direction, scale up or down in number of shards
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReshardingDirection {
    /// Scale up, add a new shard
    #[default]
    Up,
    /// Scale down, remove a shard
    Down,
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct FinishMigratingPoints {
    pub shard_id: Option<ShardId>,
    pub peer_id: Option<PeerId>,
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct CommitReadHashRing {}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct CommitWriteHashRing {}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct FinishResharding {}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct AbortResharding {}
