use std::cmp::Reverse;

use itertools::Itertools;

use crate::common::utils::{IndexesMap, JsonPathPayload};
use crate::id_tracker::IdTrackerSS;
use crate::index::field_index::CardinalityEstimation;
use crate::index::query_estimator::{
    combine_must_estimations, combine_should_estimations, invert_estimation,
};
use crate::index::query_optimization::condition_converter::condition_converter;
use crate::index::query_optimization::nested_optimizer::{
    optimize_nested_must, optimize_nested_must_not, optimize_nested_should,
};
use crate::index::query_optimization::optimized_filter::{OptimizedCondition, OptimizedFilter};
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::types::{Condition, Filter};

/// Converts user-provided filtering condition into optimized representation
///
/// Optimizations:
///
/// * Convert each condition into a checker function
/// * Use column index, avoid reading Payload, if possible
/// * Re-order operations using estimated cardinalities
///
/// ToDo: Add optimizations between clauses
///
/// # Arguments
///
/// * `filter` - original filter
/// * `id_tracker` - used for converting collection-level ids into segment-level offsets of HasId condition
/// * `estimator` - function to estimate cardinality of individual conditions
/// * `total` - total number of points in segment (used for cardinality estimation)
///
/// # Result
///
/// Optimized query + Cardinality estimation
pub fn optimize_filter<'a, F>(
    filter: &'a Filter,
    id_tracker: &IdTrackerSS,
    field_indexes: &'a IndexesMap,
    payload_provider: PayloadProvider,
    estimator: &F,
    total: usize,
    nested_path: Option<JsonPathPayload>,
) -> (OptimizedFilter<'a>, CardinalityEstimation)
where
    F: Fn(&Condition) -> CardinalityEstimation,
{
    let mut filter_estimations: Vec<CardinalityEstimation> = vec![];

    let optimized_filter = OptimizedFilter {
        should: filter.should.as_ref().and_then(|conditions| {
            if !conditions.is_empty() {
                let (optimized_conditions, estimation) = if let Some(np) = &nested_path {
                    optimize_nested_should(
                        conditions,
                        field_indexes,
                        payload_provider.clone(),
                        estimator,
                        total,
                        np.clone(),
                    )
                } else {
                    optimize_should(
                        conditions,
                        id_tracker,
                        field_indexes,
                        payload_provider.clone(),
                        estimator,
                        total,
                    )
                };
                filter_estimations.push(estimation);
                Some(optimized_conditions)
            } else {
                None
            }
        }),
        must: filter.must.as_ref().and_then(|conditions| {
            if !conditions.is_empty() {
                let (optimized_conditions, estimation) = if let Some(np) = &nested_path {
                    optimize_nested_must(
                        conditions,
                        field_indexes,
                        payload_provider.clone(),
                        estimator,
                        total,
                        np.clone(),
                    )
                } else {
                    optimize_must(
                        conditions,
                        id_tracker,
                        field_indexes,
                        payload_provider.clone(),
                        estimator,
                        total,
                    )
                };
                filter_estimations.push(estimation);
                Some(optimized_conditions)
            } else {
                None
            }
        }),
        must_not: filter.must_not.as_ref().and_then(|conditions| {
            if !conditions.is_empty() {
                let (optimized_conditions, estimation) = if let Some(np) = &nested_path {
                    optimize_nested_must_not(
                        conditions,
                        field_indexes,
                        payload_provider.clone(),
                        estimator,
                        total,
                        np.clone(),
                    )
                } else {
                    optimize_must_not(
                        conditions,
                        id_tracker,
                        field_indexes,
                        payload_provider.clone(),
                        estimator,
                        total,
                    )
                };
                filter_estimations.push(estimation);
                Some(optimized_conditions)
            } else {
                None
            }
        }),
    };

    (
        optimized_filter,
        combine_must_estimations(&filter_estimations, total),
    )
}

fn convert_conditions<'a, F>(
    conditions: &'a [Condition],
    id_tracker: &IdTrackerSS,
    field_indexes: &'a IndexesMap,
    payload_provider: PayloadProvider,
    estimator: &F,
    total: usize,
) -> Vec<(OptimizedCondition<'a>, CardinalityEstimation)>
where
    F: Fn(&Condition) -> CardinalityEstimation,
{
    conditions
        .iter()
        .map(|condition| match condition {
            Condition::Nested(nested_filter) => {
                let (optimized_filter, estimation) = optimize_filter(
                    nested_filter.filter(),
                    id_tracker,
                    field_indexes,
                    payload_provider.clone(),
                    estimator,
                    total,
                    Some(JsonPathPayload::new(nested_filter.array_key())),
                );
                (OptimizedCondition::Filter(optimized_filter), estimation)
            }
            Condition::Filter(filter) => {
                let (optimized_filter, estimation) = optimize_filter(
                    filter,
                    id_tracker,
                    field_indexes,
                    payload_provider.clone(),
                    estimator,
                    total,
                    None,
                );
                (OptimizedCondition::Filter(optimized_filter), estimation)
            }
            _ => {
                let estimation = estimator(condition);
                let condition_checker = condition_converter(
                    condition,
                    field_indexes,
                    payload_provider.clone(),
                    id_tracker,
                );
                (OptimizedCondition::Checker(condition_checker), estimation)
            }
        })
        .collect()
}

fn optimize_should<'a, F>(
    conditions: &'a [Condition],
    id_tracker: &IdTrackerSS,
    field_indexes: &'a IndexesMap,
    payload_provider: PayloadProvider,
    estimator: &F,
    total: usize,
) -> (Vec<OptimizedCondition<'a>>, CardinalityEstimation)
where
    F: Fn(&Condition) -> CardinalityEstimation,
{
    let mut converted = convert_conditions(
        conditions,
        id_tracker,
        field_indexes,
        payload_provider,
        estimator,
        total,
    );
    // More probable conditions first
    converted.sort_by_key(|(_, estimation)| Reverse(estimation.exp));
    let (conditions, estimations): (Vec<_>, Vec<_>) = converted.into_iter().unzip();

    (conditions, combine_should_estimations(&estimations, total))
}

fn optimize_must<'a, F>(
    conditions: &'a [Condition],
    id_tracker: &IdTrackerSS,
    field_indexes: &'a IndexesMap,
    payload_provider: PayloadProvider,
    estimator: &F,
    total: usize,
) -> (Vec<OptimizedCondition<'a>>, CardinalityEstimation)
where
    F: Fn(&Condition) -> CardinalityEstimation,
{
    let mut converted = convert_conditions(
        conditions,
        id_tracker,
        field_indexes,
        payload_provider,
        estimator,
        total,
    );
    // Less probable conditions first
    converted.sort_by_key(|(_, estimation)| estimation.exp);
    let (conditions, estimations): (Vec<_>, Vec<_>) = converted.into_iter().unzip();

    (conditions, combine_must_estimations(&estimations, total))
}

fn optimize_must_not<'a, F>(
    conditions: &'a [Condition],
    id_tracker: &IdTrackerSS,
    field_indexes: &'a IndexesMap,
    payload_provider: PayloadProvider,
    estimator: &F,
    total: usize,
) -> (Vec<OptimizedCondition<'a>>, CardinalityEstimation)
where
    F: Fn(&Condition) -> CardinalityEstimation,
{
    let mut converted = convert_conditions(
        conditions,
        id_tracker,
        field_indexes,
        payload_provider,
        estimator,
        total,
    );
    // More probable conditions first, as it will be reverted
    converted.sort_by_key(|(_, estimation)| estimation.exp);
    let (conditions, estimations): (Vec<_>, Vec<_>) = converted.into_iter().unzip();

    (
        conditions,
        combine_must_estimations(
            &estimations
                .into_iter()
                .map(|estimation| invert_estimation(&estimation, total))
                .collect_vec(),
            total,
        ),
    )
}
