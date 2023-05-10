use crate::common::utils::{IndexesMap, JsonPathPayload};
use crate::index::field_index::CardinalityEstimation;
use crate::index::query_estimator::{
    combine_must_estimations, combine_should_estimations, invert_estimation,
};
use crate::index::query_optimization::nested_filter::{
    find_indices_matching_all_conditions, find_indices_matching_any_conditions,
    find_indices_matching_none_conditions, nested_conditions_converter,
};
use crate::index::query_optimization::optimized_filter::OptimizedCondition;
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::types::{Condition, PointOffsetType};

pub fn optimize_nested_must<'a, F>(
    conditions: &'a [Condition],
    field_indexes: &'a IndexesMap,
    payload_provider: PayloadProvider,
    estimator: &F,
    total: usize,
    nested_path: JsonPathPayload,
) -> (Vec<OptimizedCondition<'a>>, CardinalityEstimation)
where
    F: Fn(&Condition) -> CardinalityEstimation,
{
    let nested_checker_fns =
        nested_conditions_converter(conditions, payload_provider, field_indexes, nested_path);
    let estimations: Vec<_> = conditions.iter().map(estimator).collect();

    let merged = Box::new(move |point_id: PointOffsetType| {
        let matches = find_indices_matching_all_conditions(point_id, &nested_checker_fns);
        // if any of the nested path is matching for ALL nested condition
        matches.count_ones() > 0
    });

    let estimation = combine_must_estimations(&estimations, total);
    (vec![OptimizedCondition::Checker(merged)], estimation)
}

pub fn optimize_nested_must_not<'a, F>(
    conditions: &'a [Condition],
    field_indexes: &'a IndexesMap,
    payload_provider: PayloadProvider,
    estimator: &F,
    total: usize,
    nested_path: JsonPathPayload,
) -> (Vec<OptimizedCondition<'a>>, CardinalityEstimation)
where
    F: Fn(&Condition) -> CardinalityEstimation,
{
    let nested_checker_fns =
        nested_conditions_converter(conditions, payload_provider, field_indexes, nested_path);
    let estimations: Vec<_> = conditions
        .iter()
        .map(|condition| invert_estimation(&estimator(condition), total))
        .collect();

    let merged = Box::new(move |point_id: PointOffsetType| {
        let not_matching = find_indices_matching_none_conditions(point_id, &nested_checker_fns);
        // if they are no nested path not matching ANY nested conditions
        not_matching.count_ones() == 0
    });

    let estimation = combine_must_estimations(&estimations, total);
    (vec![OptimizedCondition::Checker(merged)], estimation)
}

pub fn optimize_nested_should<'a, F>(
    conditions: &'a [Condition],
    field_indexes: &'a IndexesMap,
    payload_provider: PayloadProvider,
    estimator: &F,
    total: usize,
    nested_path: JsonPathPayload,
) -> (Vec<OptimizedCondition<'a>>, CardinalityEstimation)
where
    F: Fn(&Condition) -> CardinalityEstimation,
{
    let nested_checker_fns =
        nested_conditions_converter(conditions, payload_provider, field_indexes, nested_path);
    let estimations: Vec<_> = conditions.iter().map(estimator).collect();

    let merged = Box::new(move |point_id: PointOffsetType| {
        let matches =
            find_indices_matching_any_conditions(point_id, &nested_checker_fns).unwrap_or_default();
        // if any of the nested path is matching for any nested condition
        matches.count_ones() > 0
    });

    let estimation = combine_should_estimations(&estimations, total);
    (vec![OptimizedCondition::Checker(merged)], estimation)
}
