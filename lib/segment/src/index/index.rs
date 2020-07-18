use crate::types::{Filter, PointOffsetType, ScoreType, VectorElementType, SearchParams};
use crate::vector_storage::vector_storage::ScoredPointOffset;

/// Trait for vector searching
pub trait Index {
    /// Return list of Ids with fitting
    fn search(&self,
              vector: &Vec<VectorElementType>,
              filter: Option<&Filter>,
              top: usize,
              params: Option<&SearchParams>,
    ) -> Vec<ScoredPointOffset>;
}


pub trait PayloadIndex {
    /// Estimate amount of points (min, max) which satisfies filtering condition.
    fn estimate_cardinality(&self, query: &Filter) -> (usize, usize);

    /// Return list of all point ids, which satisfy filtering criteria
    fn query_points(&self, query: &Filter) -> Vec<PointOffsetType>;
}
