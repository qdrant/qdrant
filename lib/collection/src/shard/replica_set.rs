use std::collections::HashMap;
use std::sync::Arc;

use segment::types::{
    ExtendedPointId, Filter, ScoredPoint, WithPayload, WithPayloadInterface, WithVector,
};
use tokio::runtime::Handle;

use super::local_shard::{drop_and_delete_from_disk, LocalShard};
use super::remote_shard::RemoteShard;
use super::{PeerId, ShardId, ShardOperation};
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CountRequest, CountResult, PointRequest,
    Record, SearchRequestBatch, UpdateResult,
};
use crate::operations::CollectionUpdateOperations;

pub type IsActive = bool;

/// A set of shard replicas.
/// Handles operations so that the state is consistent across all the replicas of the shard.
pub struct ReplicaSet {
    shard_id: ShardId,
    this_peer_id: PeerId,
    local: Option<LocalShard>,
    // TODO: Remote shard should be able to query several peers
    remote: Option<RemoteShard>,
    pub(crate) replica_state: HashMap<PeerId, IsActive>,
}

impl ReplicaSet {
    pub fn peer_ids(&self) -> Vec<PeerId> {
        todo!()
    }

    pub fn set_active(&mut self, peer_id: &PeerId, active: bool) -> CollectionResult<()> {
        *self
            .replica_state
            .get_mut(peer_id)
            .ok_or_else(|| CollectionError::NotFound {
                what: format!("Shard {} replica on peer {peer_id}", self.shard_id),
            })? = active;
        Ok(())
    }

    pub async fn apply_state(
        &mut self,
        replicas: HashMap<PeerId, IsActive>,
    ) -> CollectionResult<()> {
        let removed_peers = self
            .replica_state
            .keys()
            .filter(|peer_id| !replicas.contains_key(peer_id))
            .copied()
            .collect::<Vec<_>>();
        for peer_id in removed_peers {
            if peer_id == self.this_peer_id {
                if let Some(mut shard) = self.local.take() {
                    shard.before_drop().await;
                    drop_and_delete_from_disk(shard).await?;
                } else {
                    debug_assert!(false, "inconsistent `replica_set` map with actual shards")
                }
            } else if let Some(_remote_shard) = &mut self.remote {
                todo!("remote_shard.remove_peer(peer_id)")
            }
            self.replica_state.remove(&peer_id);
        }
        for (peer_id, is_active) in replicas {
            if let Some(state) = self.replica_state.get_mut(&peer_id) {
                *state = is_active;
            } else if peer_id == self.this_peer_id {
                todo!("clone replica from another peer or log error that it should be cloned with normal operation")
            } else {
                todo!("Add remote replica")
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ShardOperation for ReplicaSet {
    async fn update(
        &self,
        _operation: CollectionUpdateOperations,
        _wait: bool,
    ) -> CollectionResult<UpdateResult> {
        todo!()
    }

    #[allow(clippy::too_many_arguments)]
    async fn scroll_by(
        &self,
        _offset: Option<ExtendedPointId>,
        _limit: usize,
        _with_payload_interface: &WithPayloadInterface,
        _with_vector: &WithVector,
        _filter: Option<&Filter>,
    ) -> CollectionResult<Vec<Record>> {
        todo!()
    }

    async fn info(&self) -> CollectionResult<CollectionInfo> {
        todo!()
    }

    async fn search(
        &self,
        _request: Arc<SearchRequestBatch>,
        _search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        todo!()
    }

    async fn count(&self, _request: Arc<CountRequest>) -> CollectionResult<CountResult> {
        todo!()
    }

    async fn retrieve(
        &self,
        _request: Arc<PointRequest>,
        _with_payload: &WithPayload,
        _with_vector: &WithVector,
    ) -> CollectionResult<Vec<Record>> {
        todo!()
    }
}
