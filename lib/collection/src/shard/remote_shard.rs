use crate::collection_manager::holders::segment_holder::SegmentHolder;
use crate::shard::{PeerId, ShardId, ShardOperation};
use crate::{
    CollectionId, CollectionInfo, CollectionResult, CollectionSearcher, CollectionUpdateOperations,
    OptimizersConfigDiff, Record, UpdateResult,
};
use async_trait::async_trait;
use parking_lot::RwLock;
use segment::types::{ExtendedPointId, Filter, WithPayloadInterface};

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
impl ShardOperation for RemoteShard {
    async fn before_drop(&mut self) {
        todo!()
    }

    async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        todo!()
    }

    fn segments(&self) -> &RwLock<SegmentHolder> {
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
}
