use std::borrow::Cow;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use validator::{Validate, ValidationError, ValidationErrors};

use crate::shards::shard::{PeerId, ShardId};

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
}

impl Validate for MoveShard {
    fn validate(&self) -> Result<(), ValidationErrors> {
        // Custom struct validator: source and target peer may not be the same
        if self.to_peer_id == self.from_peer_id {
            let mut errors = ValidationErrors::new();
            errors.add("to_peer_id", {
                let mut error = ValidationError::new("must_not_match");
                error.add_param(Cow::from("value"), &self.to_peer_id.to_string());
                error.add_param(Cow::from("other_field"), &"from_peer_id");
                error.add_param(Cow::from("other_value"), &self.from_peer_id.to_string());
                error.add_param(
                    Cow::from("message"),
                    &format!("cannot move shard to itself, \"to_peer_id\" must be different than {} in \"from_peer_id\"", self.from_peer_id),
                );
                error
            });
            return Err(errors);
        }

        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Replica {
    pub shard_id: ShardId,
    pub peer_id: PeerId,
}
