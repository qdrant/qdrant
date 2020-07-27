use segment::types::{VectorElementType, Filter, SeqNumberType, SearchParams, ScoredPoint, PointIdType};
use crate::collection::{OperationResult, CollectionInfo};
use crate::operations::CollectionUpdateOperations;
use crate::operations::types::Record;

pub trait SegmentSearcher {
    fn info(&self) -> OperationResult<CollectionInfo>;

    fn search(&self,
              vector: &Vec<VectorElementType>,
              filter: Option<&Filter>,
              top: usize,
              params: Option<&SearchParams>
    ) -> Vec<ScoredPoint>;

    fn retrieve(
        &self,
        points: &Vec<PointIdType>,
        with_payload: bool,
        with_vector: bool
    ) -> Vec<Record>;
}



pub trait SegmentUpdater {
    fn update(&self, op_num: SeqNumberType, operation: &CollectionUpdateOperations) -> OperationResult<usize>;
}