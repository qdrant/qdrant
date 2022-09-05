use std::collections::HashMap;
use std::sync::Arc;

use segment::types::{ExtendedPointId, Filter, ScoredPoint, WithPayload, WithPayloadInterface};
use tokio::runtime::Handle;

use super::local_shard::LocalShard;
use super::remote_shard::RemoteShard;
use super::{PeerId, ShardOperation};
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CountRequest, CountResult, PointRequest,
    Record, SearchRequestBatch, UpdateResult,
};
use crate::operations::CollectionUpdateOperations;

pub struct Replica<T: ShardOperation> {
    shard: T,
    // TODO: include into consensus snapshot
    pub is_active: bool,
}

impl<T: ShardOperation> Replica<T> {
    pub fn get(&self) -> &dyn ShardOperation {
        &self.shard
    }

    pub fn get_mut(&mut self) -> &mut dyn ShardOperation {
        &mut self.shard
    }
}

/// A set of shard replicas.
/// Handles operations so that the state is consistent across all the replicas of the shard.
pub struct ReplicaSet {
    this_peer_id: PeerId,
    local: Option<Replica<LocalShard>>,
    remote: HashMap<PeerId, Replica<RemoteShard>>,
}

impl ReplicaSet {
    pub fn get(&self, peer_id: &PeerId) -> Option<&dyn ShardOperation> {
        if *peer_id == self.this_peer_id {
            self.local.as_ref().map(Replica::get)
        } else {
            self.remote.get(peer_id).map(Replica::get)
        }
    }

    pub fn get_mut(&mut self, peer_id: &PeerId) -> Option<&mut dyn ShardOperation> {
        if *peer_id == self.this_peer_id {
            self.local.as_mut().map(Replica::get_mut)
        } else {
            self.remote.get_mut(peer_id).map(Replica::get_mut)
        }
    }

    pub fn set_active(&mut self, peer_id: &PeerId, active: bool) -> CollectionResult<()> {
        if *peer_id == self.this_peer_id {
            self.local
                .as_mut()
                .ok_or_else(|| CollectionError::NotFound {
                    what: format!("Replica on peer {peer_id}"),
                })?
                .is_active = active;
        } else {
            self.remote
                .get_mut(peer_id)
                .ok_or_else(|| CollectionError::NotFound {
                    what: format!("Replica on peer {peer_id}"),
                })?
                .is_active = active;
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
        _with_vector: bool,
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
        _with_vector: bool,
    ) -> CollectionResult<Vec<Record>> {
        todo!()
    }
}
