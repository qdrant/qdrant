use schemars::JsonSchema;
use segment::types::ShardKey;
use serde::{Deserialize, Serialize};

use super::shard::{PeerId, ShardId};

/// Unique identifier of a resharding operation
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReshardingKey {
    pub peer_id: PeerId,
    pub shard_id: ShardId,
    pub shard_key: Option<ShardKey>,
}
