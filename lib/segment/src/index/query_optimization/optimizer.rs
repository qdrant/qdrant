use std::cmp::Reverse;

use itertools::Itertools;

use crate::common::utils::IndexesMap;
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::id_tracker::IdTrackerSS;
use crate::index::field_index::CardinalityEstimation;
use crate::index::query_estimator::{
    combine_must_estimations, combine_should_estimations, invert_estimation,
};
use crate::index::query_optimization::condition_converter::condition_converter;
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
) -> OperationResult<(OptimizedFilter<'a>, CardinalityEstimation)>
where
    F: Fn(&Condition) -> OperationResult<CardinalityEstimation>,
{
    let mut filter_estimations: Vec<CardinalityEstimation> = vec![];

    let optimized_filter = OptimizedFilter {
        should: if let Some(conditions) = filter.should.as_ref() {
            if !conditions.is_empty() {
                let (optimized_conditions, estimation) = optimize_should(
                    conditions,
                    id_tracker,
                    field_indexes,
                    payload_provider.clone(),
                    estimator,
                    total,
                )?;
                filter_estimations.push(estimation);
                Ok::<Option<Vec<OptimizedCondition>>, OperationError>(Some(optimized_conditions))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }?,
        must: if let Some(conditions) = filter.must.as_ref() {
            if !conditions.is_empty() {
                let (optimized_conditions, estimation) = optimize_must(
                    conditions,
                    id_tracker,
                    field_indexes,
                    payload_provider.clone(),
                    estimator,
                    total,
                )?;
                filter_estimations.push(estimation);
                Ok::<Option<Vec<OptimizedCondition>>, OperationError>(Some(optimized_conditions))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }?,
        must_not: if let Some(conditions) = filter.must_not.as_ref() {
            if !conditions.is_empty() {
                let (optimized_conditions, estimation) = optimize_must_not(
                    conditions,
                    id_tracker,
                    field_indexes,
                    payload_provider,
                    estimator,
                    total,
                )?;
                filter_estimations.push(estimation);
                Ok::<Option<Vec<OptimizedCondition>>, OperationError>(Some(optimized_conditions))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }?,
    };

    Ok((
        optimized_filter,
        combine_must_estimations(&filter_estimations, total),
    ))
}

fn convert_conditions<'a, F>(
    conditions: &'a [Condition],
    id_tracker: &IdTrackerSS,
    field_indexes: &'a IndexesMap,
    payload_provider: PayloadProvider,
    estimator: &F,
    total: usize,
) -> OperationResult<Vec<(OptimizedCondition<'a>, CardinalityEstimation)>>
where
    F: Fn(&Condition) -> OperationResult<CardinalityEstimation>,
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
                )?;
                Ok((OptimizedCondition::Filter(optimized_filter), estimation))
            }
            _ => {
                let estimation = estimator(condition);
                let condition_checker = condition_converter(
                    condition,
                    field_indexes,
                    payload_provider.clone(),
                    id_tracker,
                );
                Ok((OptimizedCondition::Checker(condition_checker), estimation?))
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
) -> OperationResult<(Vec<OptimizedCondition<'a>>, CardinalityEstimation)>
where
    F: Fn(&Condition) -> OperationResult<CardinalityEstimation>,
{
    let mut converted = convert_conditions(
        conditions,
        id_tracker,
        field_indexes,
        payload_provider,
        estimator,
        total,
    )?;
    // More probable conditions first
    converted.sort_by_key(|(_, estimation)| Reverse(estimation.exp));
    let (conditions, estimations): (Vec<_>, Vec<_>) = converted.into_iter().unzip();

    Ok((conditions, combine_should_estimations(&estimations, total)))
}

fn optimize_must<'a, F>(
    conditions: &'a [Condition],
    id_tracker: &IdTrackerSS,
    field_indexes: &'a IndexesMap,
    payload_provider: PayloadProvider,
    estimator: &F,
    total: usize,
) -> OperationResult<(Vec<OptimizedCondition<'a>>, CardinalityEstimation)>
where
    F: Fn(&Condition) -> OperationResult<CardinalityEstimation>,
{
    let mut converted = convert_conditions(
        conditions,
        id_tracker,
        field_indexes,
        payload_provider,
        estimator,
        total,
    )?;
    // Less probable conditions first
    converted.sort_by_key(|(_, estimation)| estimation.exp);
    let (conditions, estimations): (Vec<_>, Vec<_>) = converted.into_iter().unzip();

    Ok((conditions, combine_must_estimations(&estimations, total)))
}

fn optimize_must_not<'a, F>(
    conditions: &'a [Condition],
    id_tracker: &IdTrackerSS,
    field_indexes: &'a IndexesMap,
    payload_provider: PayloadProvider,
    estimator: &F,
    total: usize,
) -> OperationResult<(Vec<OptimizedCondition<'a>>, CardinalityEstimation)>
where
    F: Fn(&Condition) -> OperationResult<CardinalityEstimation>,
{
    let mut converted = convert_conditions(
        conditions,
        id_tracker,
        field_indexes,
        payload_provider,
        estimator,
        total,
    )?;
    // More probable conditions first, as it will be reverted
    converted.sort_by_key(|(_, estimation)| estimation.exp);
    let (conditions, estimations): (Vec<_>, Vec<_>) = converted.into_iter().unzip();

    Ok((
        conditions,
        combine_must_estimations(
            &estimations
                .into_iter()
                .map(|estimation| invert_estimation(&estimation, total))
                .collect_vec(),
            total,
        ),
    ))
}
