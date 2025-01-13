use std::fmt;

use schemars::JsonSchema;
use segment::types::ShardKey;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::shard::{PeerId, ShardId};
use crate::operations::cluster_ops::ReshardingDirection;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ReshardState {
    pub uuid: Uuid,
    pub peer_id: PeerId,
    pub shard_id: ShardId,
    pub shard_key: Option<ShardKey>,
    pub direction: ReshardingDirection,
    pub stage: ReshardStage,
}

impl ReshardState {
    pub fn new(
        uuid: Uuid,
        direction: ReshardingDirection,
        peer_id: PeerId,
        shard_id: ShardId,
        shard_key: Option<ShardKey>,
    ) -> Self {
        Self {
            uuid,
            direction,
            peer_id,
            shard_id,
            shard_key,
            stage: ReshardStage::MigratingPoints,
        }
    }

    pub fn matches(&self, key: &ReshardKey) -> bool {
        self.uuid == key.uuid
            && self.direction == key.direction
            && self.peer_id == key.peer_id
            && self.shard_id == key.shard_id
            && self.shard_key == key.shard_key
    }

    pub fn key(&self) -> ReshardKey {
        ReshardKey {
            uuid: self.uuid,
            direction: self.direction,
            peer_id: self.peer_id,
            shard_id: self.shard_id,
            shard_key: self.shard_key.clone(),
        }
    }
}

/// Reshard stages
///
/// # Warning
///
/// This enum is ordered!
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReshardStage {
    #[default]
    MigratingPoints,
    ReadHashRingCommitted,
    WriteHashRingCommitted,
}

/// Unique identifier of a resharding task
#[derive(Clone, Debug, Eq, PartialEq, Hash, Deserialize, Serialize, JsonSchema)]
pub struct ReshardKey {
    #[schemars(skip)]
    pub uuid: Uuid,
    #[serde(default)]
    pub direction: ReshardingDirection,
    pub peer_id: PeerId,
    pub shard_id: ShardId,
    pub shard_key: Option<ShardKey>,
}

impl fmt::Display for ReshardKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{:?}", self.peer_id, self.shard_id, self.shard_key)
    }
}
