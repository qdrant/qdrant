use crate::operations::CollectionUpdateOperations;
use crate::storage::collection::collection::{OperationResult, CollectionInfo};
use segment::types::{VectorElementType, Filter, PointIdType, ScoreType, SeqNumberType, SearchParams};

pub trait SegmentManager {
    fn update(&self, op_num: SeqNumberType, operation: CollectionUpdateOperations) -> OperationResult<bool>;

    fn info(&self) -> OperationResult<CollectionInfo>;

    fn search(&self,
              vector: &Vec<VectorElementType>,
              filter: Option<&Filter>,
              top: usize,
              params: Option<&SearchParams>
    ) -> Vec<(PointIdType, ScoreType)>;
}