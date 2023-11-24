use common::types::PointOffsetType;

use crate::common::utils::IndexesMap;
use crate::id_tracker::IdTrackerSS;
use crate::index::field_index::CardinalityEstimation;
use crate::index::query_optimization::optimized_filter::{
    check_optimized_filter, OptimizedConditionFilter,
};
use crate::index::query_optimization::optimizer::optimize_filter;
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::payload_storage::FilterContext;
use crate::types::{Condition, Filter};

pub struct StructFilterContext<'a> {
    optimized_filter: OptimizedConditionFilter<'a>,
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
        let optimized_filter = optimize_filter(
            filter,
            id_tracker,
            field_indexes,
            payload_provider,
            estimator,
            total,
        );

        Self { optimized_filter }
    }
}

impl<'a> FilterContext for StructFilterContext<'a> {
    fn check(&self, point_id: PointOffsetType) -> bool {
        check_optimized_filter(&self.optimized_filter.filter, point_id)
    }
}
