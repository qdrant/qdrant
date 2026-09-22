use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::data_types::attention::{AttentionRequest, AttentionResponse};

use super::ShardReplicaSet;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::shard::Shard;

impl ShardReplicaSet {
    pub(crate) async fn attention(
        &self,
        requests: Vec<AttentionRequest>,
        hw: HwMeasurementAcc,
    ) -> CollectionResult<Vec<AttentionResponse>> {
        if self.peers().len() != 1
            || self.peer_state(self.this_peer_id()) != Some(ReplicaState::Active)
        {
            return Err(CollectionError::bad_request(
                "attention requires one active local replica",
            ));
        }
        let local = self.local.read().await;
        match local.as_ref() {
            Some(Shard::Local(shard)) => shard.attention(requests, hw).await,
            _ => Err(CollectionError::bad_request(
                "attention requires a local shard without proxies",
            )),
        }
    }
}
