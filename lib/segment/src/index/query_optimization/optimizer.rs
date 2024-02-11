use std::cmp::Reverse;

use itertools::Itertools;

use crate::common::utils::IndexesMap;
use crate::id_tracker::IdTrackerSS;
use crate::index::field_index::CardinalityEstimation;
use crate::index::query_estimator::{
    combine_min_should_estimations, combine_must_estimations, combine_should_estimations,
    invert_estimation,
};
use crate::index::query_optimization::condition_converter::condition_converter;
use crate::index::query_optimization::optimized_filter::{
    OptimizedCondition, OptimizedFilter, OptimizedMinShould,
};
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::types::{Condition, Filter, MinShould};

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
) -> (OptimizedFilter<'a>, CardinalityEstimation)
where
    F: Fn(&Condition) -> CardinalityEstimation,
{
    let mut filter_estimations: Vec<CardinalityEstimation> = vec![];

    let optimized_filter = OptimizedFilter {
        should: filter.should.as_ref().and_then(|conditions| {
            if !conditions.is_empty() {
                let (optimized_conditions, estimation) = optimize_should(
                    conditions,
                    id_tracker,
                    field_indexes,
                    payload_provider.clone(),
                    estimator,
                    total,
                );
                filter_estimations.push(estimation);
                Some(optimized_conditions)
            } else {
                None
            }
        }),
        min_should: filter.min_should.as_ref().and_then(
            |MinShould {
                 conditions,
                 min_count,
             }| {
                if !conditions.is_empty() {
                    let (optimized_conditions, estimation) = optimize_min_should(
                        conditions,
                        *min_count,
                        id_tracker,
                        field_indexes,
                        payload_provider.clone(),
                        estimator,
                        total,
                    );
                    filter_estimations.push(estimation);
                    Some(OptimizedMinShould {
                        conditions: optimized_conditions,
                        min_count: *min_count,
                    })
                } else {
                    None
                }
            },
        ),
        must: filter.must.as_ref().and_then(|conditions| {
            if !conditions.is_empty() {
                let (optimized_conditions, estimation) = optimize_must(
                    conditions,
                    id_tracker,
                    field_indexes,
                    payload_provider.clone(),
                    estimator,
                    total,
                );
                filter_estimations.push(estimation);
                Some(optimized_conditions)
            } else {
                None
            }
        }),
        must_not: filter.must_not.as_ref().and_then(|conditions| {
            if !conditions.is_empty() {
                let (optimized_conditions, estimation) = optimize_must_not(
                    conditions,
                    id_tracker,
                    field_indexes,
                    payload_provider.clone(),
                    estimator,
                    total,
                );
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
            Condition::Filter(filter) => {
                let (optimized_filter, estimation) = optimize_filter(
                    filter,
                    id_tracker,
                    field_indexes,
                    payload_provider.clone(),
                    estimator,
                    total,
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

fn optimize_min_should<'a, F>(
    conditions: &'a [Condition],
    min_count: usize,
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
    // More probable conditions first if min_count < number of conditions
    if min_count < conditions.len() / 2 {
        converted.sort_by_key(|(_, estimation)| Reverse(estimation.exp));
    } else {
        // Less probable conditions first
        converted.sort_by_key(|(_, estimation)| estimation.exp);
    }
    let (conditions, estimations): (Vec<_>, Vec<_>) = converted.into_iter().unzip();

    (
        conditions,
        combine_min_should_estimations(&estimations, min_count, total),
    )
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
