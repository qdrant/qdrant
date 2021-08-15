use std::sync::Arc;

use segment::types::{PointIdType, ScoredPoint, SeqNumberType};

use crate::operations::types::{CollectionResult, Record, SearchRequest};
use crate::operations::CollectionUpdateOperations;

#[async_trait::async_trait]
pub trait CollectionSearcher {
    async fn search(
        &self,
        // Request is supposed to be a read only, that is why no mutex used
        request: Arc<SearchRequest>,
    ) -> CollectionResult<Vec<ScoredPoint>>;

    async fn retrieve(
        &self,
        points: &[PointIdType],
        with_payload: bool,
        with_vector: bool,
    ) -> CollectionResult<Vec<Record>>;
}

pub trait CollectionUpdater {
    fn update(
        &self,
        op_num: SeqNumberType,
        operation: CollectionUpdateOperations,
    ) -> CollectionResult<usize>;
}
