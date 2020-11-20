use crate::types::{Filter, PointOffsetType, VectorElementType, SearchParams};
use crate::vector_storage::vector_storage::ScoredPointOffset;
use crate::entry::entry_point::OperationResult;
use crate::index::field_index::Estimation;

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
    fn build_index(&mut self) -> OperationResult<()> ;
}


pub trait PayloadIndex {
    /// Estimate amount of points (min, max) which satisfies filtering condition.
    fn estimate_cardinality(&self, query: &Filter) -> Estimation;

    /// Return list of all point ids, which satisfy filtering criteria
    fn query_points(&self, query: &Filter) -> Vec<PointOffsetType>;
}
