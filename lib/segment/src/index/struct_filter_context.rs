use crate::id_tracker::IdTrackerSS;
use crate::index::field_index::CardinalityEstimation;
use crate::index::query_optimization::optimized_filter::{check_optimized_filter, OptimizedFilter};
use crate::index::query_optimization::optimizer::{optimize_filter, IndexesMap};
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::payload_storage::FilterContext;
use crate::types::{Condition, Filter, PointOffsetType};

pub struct StructFilterContext<'a> {
    optimized_filter: OptimizedFilter<'a>,
}

impl<'a> StructFilterContext<'a> {
    pub fn new<F>(
        filter: &'a Filter,
        id_tracker: &IdTrackerSS,
        payload_provider: PayloadProvider,
        field_indexes: &'a IndexesMap,
        estimator: &F,
        total: usize,
    ) -> Self
    where
        F: Fn(&Condition) -> CardinalityEstimation,
    {
        let (optimized_filter, _) = optimize_filter(
            filter,
            id_tracker,
            field_indexes,
            payload_provider,
            estimator,
            total,
            None,
        );

        Self { optimized_filter }
    }
}

impl<'a> FilterContext for StructFilterContext<'a> {
    fn check(&self, point_id: PointOffsetType) -> bool {
        check_optimized_filter(&self.optimized_filter, point_id)
    }
}
