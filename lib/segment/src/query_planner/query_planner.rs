use crate::types::{VectorElementType, Filter, PointOffsetType, ScoreType, SearchParams};

/// Similar to `Index`, but should operate with multiple possible indexes + post-filtering
pub trait QueryPlanner {
    /// Performs search of vector in the most efficient way according to heuristics
    fn search(&self,
              vector: &Vec<VectorElementType>,
              filter: Option<&Filter>,
              top: usize,
              params: Option<&SearchParams>,
    ) -> Vec<(PointOffsetType, ScoreType)>;
}