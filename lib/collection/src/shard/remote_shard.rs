use crate::shard::{PeerId, ShardId, ShardOperation};
use crate::{
    CollectionId, CollectionInfo, CollectionResult, CollectionSearcher, CollectionUpdateOperations,
    OptimizersConfigDiff, PointRequest, Record, SearchRequest, UpdateResult,
};
use async_trait::async_trait;
use segment::types::{ExtendedPointId, Filter, ScoredPoint, WithPayload, WithPayloadInterface};
use std::sync::Arc;
use tokio::runtime::Handle;

/// RemoteShard
///
/// Remote Shard is a representation of a shard that is located on a remote peer.
/// Currently a placeholder implementation for later work.
#[allow(dead_code)]
pub struct RemoteShard {
    id: ShardId,
    collection_id: CollectionId,
    peer_id: PeerId,
}

#[async_trait]
#[allow(unused_variables)]
impl ShardOperation for &RemoteShard {
    async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        todo!()
    }

    async fn scroll_by(
        &self,
        segment_searcher: &(dyn CollectionSearcher + Sync),
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: bool,
        filter: Option<&Filter>,
    ) -> CollectionResult<Vec<Record>> {
        todo!()
    }

    async fn update_optimizer_params(
        &self,
        optimizer_config_diff: OptimizersConfigDiff,
    ) -> CollectionResult<()> {
        todo!()
    }

    async fn info(&self) -> CollectionResult<CollectionInfo> {
        todo!()
    }

    async fn search(
        &self,
        request: Arc<SearchRequest>,
        segment_searcher: &(dyn CollectionSearcher + Sync),
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        todo!()
    }

    async fn retrieve(
        &self,
        request: Arc<PointRequest>,
        segment_searcher: &(dyn CollectionSearcher + Sync),
        with_payload: &WithPayload,
        with_vector: bool,
    ) -> CollectionResult<Vec<Record>> {
        todo!()
    }
}
