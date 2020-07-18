use segment::types::{VectorElementType, Filter, SeqNumberType, SearchParams, ScoredPoint};
use crate::collection::{OperationResult, CollectionInfo};
use crate::operations::CollectionUpdateOperations;

pub trait SegmentManager {
    fn update(&self, op_num: SeqNumberType, operation: CollectionUpdateOperations) -> OperationResult<bool>;

    fn info(&self) -> OperationResult<CollectionInfo>;

    fn search(&self,
              vector: &Vec<VectorElementType>,
              filter: Option<&Filter>,
              top: usize,
              params: Option<&SearchParams>
    ) -> Vec<ScoredPoint>;
}