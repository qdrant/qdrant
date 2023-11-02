use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::config::CollectionConfig;
use crate::shards::replica_set::ReplicaState;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::ShardKeyMapping;
use crate::shards::transfer::shard_transfer::ShardTransfer;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ShardInfo {
    pub replicas: HashMap<PeerId, ReplicaState>,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone, PartialEq)]
pub struct State {
    #[validate]
    pub config: CollectionConfig,
    pub shards: HashMap<ShardId, ShardInfo>,
    #[serde(default)]
    pub transfers: HashSet<ShardTransfer>,
    #[serde(default)]
    pub shards_key_mapping: ShardKeyMapping,
}
