use common::types::PointOffsetType;

use crate::index::query_optimization::optimized_filter::{check_optimized_filter, OptimizedFilter};
use crate::payload_storage::FilterContext;

pub struct StructFilterContext<'a> {
    optimized_filter: OptimizedFilter<'a>,
}

impl<'a> StructFilterContext<'a> {
    pub fn new(optimized_filter: OptimizedFilter<'a>) -> Self {
        Self { optimized_filter }
    }
}

impl FilterContext for StructFilterContext<'_> {
    fn check(&self, point_id: PointOffsetType) -> bool {
        check_optimized_filter(&self.optimized_filter, point_id)
    }
}
