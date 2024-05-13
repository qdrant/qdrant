use serde::{Deserialize, Serialize};

use crate::shards::shard::{PeerId, ShardId};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct State {
    pub peer_id: PeerId,
    pub shard_id: ShardId,
}

impl State {
    #[allow(dead_code)]
    pub fn new(peer_id: PeerId, shard_id: ShardId) -> Self {
        Self { peer_id, shard_id }
    }
}
