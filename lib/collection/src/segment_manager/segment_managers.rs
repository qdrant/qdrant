use segment::types::{SeqNumberType, ScoredPoint, PointIdType};
use crate::collection::{CollectionResult};
use crate::operations::CollectionUpdateOperations;
use crate::operations::types::{Record, SearchRequest};
use std::sync::Arc;

pub trait SegmentSearcher {
    fn search(&self,
              // Request is supposed to be a read only, that is why no mutex used
              request: Arc<SearchRequest>,
    ) -> CollectionResult<Vec<ScoredPoint>>;

    fn retrieve(
        &self,
        points: &Vec<PointIdType>,
        with_payload: bool,
        with_vector: bool,
    ) -> CollectionResult<Vec<Record>>;
}


pub trait SegmentUpdater {
    fn update(&self, op_num: SeqNumberType, operation: &CollectionUpdateOperations) -> CollectionResult<usize>;
}


