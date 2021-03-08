use crate::types::{Filter, PointOffsetType, VectorElementType, SearchParams, PayloadKeyType};
use crate::vector_storage::vector_storage::ScoredPointOffset;
use crate::entry::entry_point::OperationResult;
use crate::index::field_index::CardinalityEstimation;

/// Trait for vector searching
pub trait Index {
    /// Return list of Ids with fitting
    fn search(&self,
              vector: &Vec<VectorElementType>,
              filter: Option<&Filter>,
              top: usize,
              params: Option<&SearchParams>,
    ) -> Vec<ScoredPointOffset>;


    /// Force internal index rebuild.
    fn build_index(&mut self) -> OperationResult<()>;
}

pub trait PayloadIndex {
    /// Get indexed fields
    fn indexed_fields(&self) -> Vec<PayloadKeyType>;

    /// Mark field as one which should be indexed
    fn mark_indexed(&mut self, field: &PayloadKeyType) -> OperationResult<()>;

    /// Remove index
    fn drop_index(&mut self, field: &PayloadKeyType) -> OperationResult<()>;

    /// Estimate amount of points (min, max) which satisfies filtering condition.
    fn estimate_cardinality(&self, query: &Filter) -> CardinalityEstimation;

    /// Return list of all point ids, which satisfy filtering criteria
    fn query_points(&self, query: &Filter) -> Box<dyn Iterator<Item=PointOffsetType> + '_>;
}
