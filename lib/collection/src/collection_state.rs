use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::collection::payload_index_schema::PayloadIndexSchema;
use crate::collection::resharding::ReshardingState;
use crate::config::CollectionConfig;
use crate::shards::replica_set::ReplicaState;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::ShardKeyMapping;
use crate::shards::transfer::ShardTransfer;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ShardInfo {
    pub replicas: HashMap<PeerId, ReplicaState>,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone, PartialEq)]
pub struct State {
    #[validate]
    pub config: CollectionConfig,
    pub shards: HashMap<ShardId, ShardInfo>,
    pub resharding: Option<ReshardingState>,
    #[serde(default)]
    pub transfers: HashSet<ShardTransfer>,
    #[serde(default)]
    pub shards_key_mapping: ShardKeyMapping,
    #[serde(default)]
    pub payload_index_schema: PayloadIndexSchema,
}

impl State {
    pub fn max_shard_id(&self) -> ShardId {
        self.shards_key_mapping
            .values()
            .flat_map(|shard_ids| shard_ids.iter())
            .max()
            .copied()
            .unwrap_or(0)
    }
}
