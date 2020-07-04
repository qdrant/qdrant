use crate::index::index::Index;
use crate::query_planner::query_planner::QueryPlanner;
use crate::types::{Filter, VectorElementType, PointOffsetType, ScoreType};

struct SimpleQueryPlanner<'s> {
    index: Box<&'s dyn Index>
}

impl QueryPlanner for SimpleQueryPlanner<'_> {
    fn search(&self,
              vector: &Vec<VectorElementType>,
              filter: Option<&Filter>,
              top: usize
    ) -> Vec<(PointOffsetType, ScoreType)> {
        self.index.search(vector, filter, top)
    }
}