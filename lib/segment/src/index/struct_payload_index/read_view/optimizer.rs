use std::cmp::Reverse;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::DeferredBehavior;
use itertools::Itertools;

use super::StructPayloadIndexReadView;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerRead;
use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::field_index::{CardinalityEstimation, FieldIndexRead};
use crate::index::query_estimator::{
    combine_min_should_estimations, combine_must_estimations, combine_should_estimations,
    invert_estimation,
};
use crate::index::query_optimization::optimized_filter::OptimizedFilter;
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::payload_storage::PayloadStorageRead;
use crate::types::{Condition, Filter, MinShould};
use crate::vector_storage::VectorStorageRead;

impl<'a, P, I, V, F> StructPayloadIndexReadView<'a, P, I, V, F>
where
    P: PayloadStorageRead,
    I: IdTrackerRead,
    V: VectorStorageRead,
    F: FieldIndexRead,
{
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
    pub fn optimize_filter<'b, S: PayloadStorageRead + 'b>(
        &'b self,
        filter: &'b Filter,
        payload_provider: PayloadProvider<S>,
        total: usize,
        deferred_behavior: DeferredBehavior,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<(OptimizedFilter<'b>, CardinalityEstimation)> {
        let mut filter_estimations: Vec<CardinalityEstimation> = vec![];
        let Filter {
            should,
            min_should,
            must,
            must_not,
        } = filter;

        let (should, estimation) = self.optimize_should(
            should.as_deref().unwrap_or(&[]),
            payload_provider.clone(),
            total,
            deferred_behavior,
            hw_counter,
        )?;
        filter_estimations.push(estimation);

        let (min_should, min_should_count) = match min_should.as_ref() {
            Some(MinShould {
                conditions,
                min_count,
            }) => (conditions.as_slice(), *min_count),
            None => (&[][..], 0),
        };
        let (min_should, estimation) = self.optimize_min_should(
            min_should,
            min_should_count,
            payload_provider.clone(),
            total,
            deferred_behavior,
            hw_counter,
        )?;
        filter_estimations.push(estimation);

        let (must, estimation) = self.optimize_must(
            must.as_deref().unwrap_or(&[]),
            payload_provider.clone(),
            total,
            deferred_behavior,
            hw_counter,
        )?;
        filter_estimations.push(estimation);

        let (must_not, estimation) = self.optimize_must_not(
            must_not.as_deref().unwrap_or(&[]),
            payload_provider,
            total,
            deferred_behavior,
            hw_counter,
        )?;
        filter_estimations.push(estimation);

        let optimized_filter = OptimizedFilter {
            should,
            min_should,
            min_should_count,
            must,
            must_not,
        };

        Ok((
            optimized_filter,
            combine_must_estimations(&filter_estimations, total),
        ))
    }

    pub fn convert_conditions<'b, S: PayloadStorageRead + 'b>(
        &'b self,
        conditions: &'b [Condition],
        payload_provider: PayloadProvider<S>,
        total: usize,
        deferred_behavior: DeferredBehavior,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<(ConditionCheckerEnum<'b>, CardinalityEstimation)>> {
        conditions
            .iter()
            .map(|condition| match condition {
                Condition::Filter(filter) => {
                    let (optimized_filter, estimation) = self.optimize_filter(
                        filter,
                        payload_provider.clone(),
                        total,
                        deferred_behavior,
                        hw_counter,
                    )?;
                    Ok((ConditionCheckerEnum::Filter(optimized_filter), estimation))
                }
                Condition::Field(_)
                | Condition::IsEmpty(_)
                | Condition::IsNull(_)
                | Condition::HasId(_)
                | Condition::HasVector(_)
                | Condition::Nested(_)
                | Condition::CustomIdChecker(_) => {
                    let estimation =
                        self.condition_cardinality(condition, None, deferred_behavior, hw_counter)?;
                    let condition_checker = self.condition_converter(
                        condition,
                        payload_provider.clone(),
                        deferred_behavior,
                        hw_counter,
                    )?;
                    Ok((condition_checker, estimation))
                }
            })
            .collect()
    }

    fn optimize_should<'b, S: PayloadStorageRead + 'b>(
        &'b self,
        conditions: &'b [Condition],
        payload_provider: PayloadProvider<S>,
        total: usize,
        deferred_behavior: DeferredBehavior,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<(Vec<ConditionCheckerEnum<'b>>, CardinalityEstimation)> {
        if conditions.is_empty() {
            // Empty `should` => match every point.
            return Ok((Vec::new(), CardinalityEstimation::exact(total)));
        }

        let mut converted = self.convert_conditions(
            conditions,
            payload_provider,
            total,
            deferred_behavior,
            hw_counter,
        )?;
        // More probable conditions first
        converted.sort_by_key(|(_, estimation)| Reverse(estimation.exp));
        let (conditions, estimations): (Vec<_>, Vec<_>) = converted.into_iter().unzip();

        Ok((conditions, combine_should_estimations(&estimations, total)))
    }

    fn optimize_min_should<'b, S: PayloadStorageRead + 'b>(
        &'b self,
        conditions: &'b [Condition],
        min_count: usize,
        payload_provider: PayloadProvider<S>,
        total: usize,
        deferred_behavior: DeferredBehavior,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<(Vec<ConditionCheckerEnum<'b>>, CardinalityEstimation)> {
        let mut converted = self.convert_conditions(
            conditions,
            payload_provider,
            total,
            deferred_behavior,
            hw_counter,
        )?;
        // More probable conditions first if min_count < number of conditions
        if min_count < conditions.len() / 2 {
            converted.sort_by_key(|(_, estimation)| Reverse(estimation.exp));
        } else {
            // Less probable conditions first
            converted.sort_by_key(|(_, estimation)| estimation.exp);
        }
        let (conditions, estimations): (Vec<_>, Vec<_>) = converted.into_iter().unzip();

        Ok((
            conditions,
            combine_min_should_estimations(&estimations, min_count, total),
        ))
    }

    fn optimize_must<'b, S: PayloadStorageRead + 'b>(
        &'b self,
        conditions: &'b [Condition],
        payload_provider: PayloadProvider<S>,
        total: usize,
        deferred_behavior: DeferredBehavior,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<(Vec<ConditionCheckerEnum<'b>>, CardinalityEstimation)> {
        let mut converted = self.convert_conditions(
            conditions,
            payload_provider,
            total,
            deferred_behavior,
            hw_counter,
        )?;
        // Less probable conditions first
        converted.sort_by_key(|(_, estimation)| estimation.exp);
        let (conditions, estimations): (Vec<_>, Vec<_>) = converted.into_iter().unzip();

        Ok((conditions, combine_must_estimations(&estimations, total)))
    }

    fn optimize_must_not<'b, S: PayloadStorageRead + 'b>(
        &'b self,
        conditions: &'b [Condition],
        payload_provider: PayloadProvider<S>,
        total: usize,
        deferred_behavior: DeferredBehavior,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<(Vec<ConditionCheckerEnum<'b>>, CardinalityEstimation)> {
        let mut converted = self.convert_conditions(
            conditions,
            payload_provider,
            total,
            deferred_behavior,
            hw_counter,
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
}
