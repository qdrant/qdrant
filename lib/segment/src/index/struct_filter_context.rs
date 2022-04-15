use crate::id_tracker::IdTrackerSS;
use crate::index::field_index::{CardinalityEstimation, PrimaryCondition};
use crate::index::query_optimization::condition_converter::{
    get_geo_bounding_box_checkers, get_geo_radius_checkers, get_match_checkers, get_range_checkers,
};
use crate::index::query_optimization::optimized_filter::{
    check_optimized_filter, ConditionCheckerFn, OptimizedFilter,
};
use crate::index::query_optimization::optimizer::{optimize_filter, IndexesMap};
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::payload_storage::query_checker::check_filter;
use crate::payload_storage::{ConditionCheckerSS, FilterContext};
use crate::types::{Condition, Filter, Payload, PointOffsetType};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

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
        );

        Self { optimized_filter }
    }
}

impl<'a> FilterContext for StructFilterContext<'a> {
    fn check(&self, point_id: PointOffsetType) -> bool {
        check_optimized_filter(&self.optimized_filter, point_id)
    }
}
