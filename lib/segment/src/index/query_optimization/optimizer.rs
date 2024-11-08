use std::cmp::Reverse;

use itertools::Itertools;

use crate::index::field_index::CardinalityEstimation;
use crate::index::query_estimator::{
    combine_min_should_estimations, combine_must_estimations, combine_should_estimations,
    invert_estimation,
};
use crate::index::query_optimization::optimized_filter::{
    OptimizedCondition, OptimizedFilter, OptimizedMinShould,
};
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::types::{Condition, Filter, MinShould};

impl StructPayloadIndex {
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
    /// * `payload_provider` - provides the payload storage
    /// * `total` - total number of points in segment (used for cardinality estimation)
    ///
    /// # Result
    ///
    /// Optimized query + Cardinality estimation
    pub fn optimize_filter<'a>(
        &'a self,
        filter: &'a Filter,
        payload_provider: PayloadProvider,
        total: usize,
    ) -> (OptimizedFilter<'a>, CardinalityEstimation) {
        let mut filter_estimations: Vec<CardinalityEstimation> = vec![];

        let optimized_filter = OptimizedFilter {
            should: filter.should.as_ref().and_then(|conditions| {
                if !conditions.is_empty() {
                    let (optimized_conditions, estimation) =
                        self.optimize_should(conditions, payload_provider.clone(), total);
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
                        let (optimized_conditions, estimation) = self.optimize_min_should(
                            conditions,
                            *min_count,
                            payload_provider.clone(),
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
                    let (optimized_conditions, estimation) =
                        self.optimize_must(conditions, payload_provider.clone(), total);
                    filter_estimations.push(estimation);
                    Some(optimized_conditions)
                } else {
                    None
                }
            }),
            must_not: filter.must_not.as_ref().and_then(|conditions| {
                if !conditions.is_empty() {
                    let (optimized_conditions, estimation) =
                        self.optimize_must_not(conditions, payload_provider, total);
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

    fn convert_conditions<'a>(
        &'a self,
        conditions: &'a [Condition],
        payload_provider: PayloadProvider,
        total: usize,
    ) -> Vec<(OptimizedCondition<'a>, CardinalityEstimation)> {
        conditions
            .iter()
            .map(|condition| match condition {
                Condition::Filter(filter) => {
                    let (optimized_filter, estimation) =
                        self.optimize_filter(filter, payload_provider.clone(), total);
                    (OptimizedCondition::Filter(optimized_filter), estimation)
                }
                _ => {
                    let estimation = self.condition_cardinality(condition, None);
                    let condition_checker =
                        self.condition_converter(condition, payload_provider.clone());
                    (OptimizedCondition::Checker(condition_checker), estimation)
                }
            })
            .collect()
    }

    fn optimize_should<'a>(
        &'a self,
        conditions: &'a [Condition],
        payload_provider: PayloadProvider,
        total: usize,
    ) -> (Vec<OptimizedCondition<'a>>, CardinalityEstimation) {
        let mut converted = self.convert_conditions(conditions, payload_provider, total);
        // More probable conditions first
        converted.sort_by_key(|(_, estimation)| Reverse(estimation.exp));
        let (conditions, estimations): (Vec<_>, Vec<_>) = converted.into_iter().unzip();

        (conditions, combine_should_estimations(&estimations, total))
    }

    fn optimize_min_should<'a>(
        &'a self,
        conditions: &'a [Condition],
        min_count: usize,
        payload_provider: PayloadProvider,
        total: usize,
    ) -> (Vec<OptimizedCondition<'a>>, CardinalityEstimation) {
        let mut converted = self.convert_conditions(conditions, payload_provider, total);
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

    fn optimize_must<'a>(
        &'a self,
        conditions: &'a [Condition],
        payload_provider: PayloadProvider,
        total: usize,
    ) -> (Vec<OptimizedCondition<'a>>, CardinalityEstimation) {
        let mut converted = self.convert_conditions(conditions, payload_provider, total);
        // Less probable conditions first
        converted.sort_by_key(|(_, estimation)| estimation.exp);
        let (conditions, estimations): (Vec<_>, Vec<_>) = converted.into_iter().unzip();

        (conditions, combine_must_estimations(&estimations, total))
    }

    fn optimize_must_not<'a>(
        &'a self,
        conditions: &'a [Condition],
        payload_provider: PayloadProvider,
        total: usize,
    ) -> (Vec<OptimizedCondition<'a>>, CardinalityEstimation) {
        let mut converted = self.convert_conditions(conditions, payload_provider, total);
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
}
