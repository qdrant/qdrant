use segment::types::{SeqNumberType, ScoredPoint, PointIdType};
use crate::collection::{OperationResult};
use crate::operations::CollectionUpdateOperations;
use crate::operations::types::{Record, CollectionInfo, SearchRequest};
use std::sync::Arc;

pub trait SegmentSearcher {
    fn info(&self) -> OperationResult<CollectionInfo>;

    fn search(&self,
              // Request is supposed to be a read only, that is why no mutex used
              request: Arc<SearchRequest>,
    ) -> OperationResult<Vec<ScoredPoint>>;

    fn retrieve(
        &self,
        points: &Vec<PointIdType>,
        with_payload: bool,
        with_vector: bool,
    ) -> OperationResult<Vec<Record>>;
}


pub trait SegmentUpdater {
    fn update(&self, op_num: SeqNumberType, operation: &CollectionUpdateOperations) -> OperationResult<usize>;
}


pub trait SegmentOptimizer {
    /// Checks if segment optimization is required
    fn check_condition(&self, op_num: SeqNumberType) -> bool;

    /// Performs optimization of collections's segments, including:
    ///     - Segment rebuilding
    ///     - Segment joining
    fn optimize(&self) -> OperationResult<bool>;
}