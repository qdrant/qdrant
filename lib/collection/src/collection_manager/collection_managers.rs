use std::sync::Arc;

use parking_lot::RwLock;
use tokio::runtime::Handle;

use segment::types::{PointIdType, ScoredPoint, SeqNumberType, WithPayload};

use crate::collection_manager::holders::segment_holder::SegmentHolder;
use crate::operations::types::{CollectionResult, Record, SearchRequest};
use crate::operations::CollectionUpdateOperations;

#[async_trait::async_trait]
pub trait CollectionSearcher {
    async fn search(
        &self,
        segments: &RwLock<SegmentHolder>,
        // Request is supposed to be a read only, that is why no mutex used
        request: Arc<SearchRequest>,
        runtime_handle: &Handle,
    ) -> CollectionResult<Vec<ScoredPoint>>;

    async fn retrieve(
        &self,
        segments: &RwLock<SegmentHolder>,
        points: &[PointIdType],
        with_payload: &WithPayload,
        with_vector: bool,
    ) -> CollectionResult<Vec<Record>>;
}

pub trait CollectionUpdater {
    fn update(
        &self,
        segments: &RwLock<SegmentHolder>,
        op_num: SeqNumberType,
        operation: CollectionUpdateOperations,
    ) -> CollectionResult<usize>;
}
