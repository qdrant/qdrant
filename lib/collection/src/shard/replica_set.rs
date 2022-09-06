use std::sync::Arc;

use segment::types::{ExtendedPointId, Filter, ScoredPoint, WithPayload, WithPayloadInterface};
use tokio::runtime::Handle;

use super::local_shard::LocalShard;
use super::remote_shard::RemoteShard;
use super::ShardOperation;
use crate::operations::types::{
    CollectionInfo, CollectionResult, CountRequest, CountResult, PointRequest, Record,
    SearchRequestBatch, UpdateResult,
};
use crate::operations::CollectionUpdateOperations;

pub struct Replica<T: ShardOperation> {
    shard: T,
    pub is_active: bool,
}

/// A set of shard replicas.
/// Handles operations so that the state is consistent across all the replicas of the shard.
pub struct ReplicaSet {
    local: Option<Replica<LocalShard>>,
    remote: Vec<Replica<RemoteShard>>,
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
