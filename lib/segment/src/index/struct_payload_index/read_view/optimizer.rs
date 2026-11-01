use std::cmp::Reverse;

use common::types::{DeferredBehavior, PointOffsetType};
use itertools::Itertools;

use super::StructPayloadIndexReadView;
use super::materialization::{
    ConvertedCondition, MaterializationHint, PostingMerge, merge_postings, reads_posting_list,
};
use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerRead;
use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::field_index::{CardinalityEstimation, FieldIndexRead};
use crate::index::query_estimator::{
    combine_min_should_estimations, combine_must_estimations, combine_should_estimations,
    invert_estimation,
};
use crate::index::query_optimization::expected_checks::ExpectedChecks;
use crate::index::query_optimization::optimized_filter::OptimizedFilter;
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::index::query_optimization::point_set::{PointSet, PointSetContext};
use crate::payload_storage::PayloadStorageRead;
use crate::types::{Condition, FieldCondition, Filter, MinShould};
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
    /// * `materialization` - how many points the caller will check against the result,
    ///   and which primary clauses produce them, if known. A field condition whose posting
    ///   list is small relative to the number of checks is read up front instead of being
    ///   checked through its index point by point; the posting lists of one clause are
    ///   merged into a single [`PointSet`].
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
        materialization: Option<MaterializationHint<'_>>,
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
            materialization,
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
            materialization,
        )?;
        filter_estimations.push(estimation);

        let (must, estimation) = self.optimize_must(
            must.as_deref().unwrap_or(&[]),
            payload_provider.clone(),
            total,
            deferred_behavior,
            materialization,
        )?;
        filter_estimations.push(estimation);

        let (must_not, estimation) = self.optimize_must_not(
            must_not.as_deref().unwrap_or(&[]),
            payload_provider,
            total,
            deferred_behavior,
            materialization,
        )?;
        filter_estimations.push(estimation);

        let optimized_filter =
            OptimizedFilter::new(should, min_should, min_should_count, must, must_not);

        Ok((
            optimized_filter,
            combine_must_estimations(&filter_estimations, total),
        ))
    }

    /// Convert each condition into a checker. Posting lists read up front (see
    /// [`MaterializationHint`]) become one [`PointSet`] each, not merged.
    pub fn convert_conditions<'b, S: PayloadStorageRead + 'b>(
        &'b self,
        conditions: &'b [Condition],
        payload_provider: PayloadProvider<S>,
        total: usize,
        deferred_behavior: DeferredBehavior,
        materialization: Option<MaterializationHint<'_>>,
    ) -> OperationResult<Vec<(ConditionCheckerEnum<'b>, CardinalityEstimation)>> {
        let context =
            materialization.map(|hint| hint.point_set_context(self.id_tracker.total_point_count()));
        Ok(self
            .convert_conditions_with_postings(
                conditions,
                payload_provider,
                total,
                deferred_behavior,
                materialization,
            )?
            .into_iter()
            .map(|(converted, estimation)| {
                let checker = match converted {
                    ConvertedCondition::Checker(checker) => checker,
                    ConvertedCondition::Posting(posting) => {
                        // A posting is only read when there is a hint, so a context too.
                        let context = context.expect("posting read without a hint");
                        ConditionCheckerEnum::Points(PointSet::union(vec![posting], context))
                    }
                };
                (checker, estimation)
            })
            .collect())
    }

    /// Convert each condition into a checker, or read its posting list up front when that
    /// is cheaper than checking it point by point; the caller merges the posting lists.
    fn convert_conditions_with_postings<'b, S: PayloadStorageRead + 'b>(
        &'b self,
        conditions: &'b [Condition],
        payload_provider: PayloadProvider<S>,
        total: usize,
        deferred_behavior: DeferredBehavior,
        materialization: Option<MaterializationHint<'_>>,
    ) -> OperationResult<Vec<(ConvertedCondition<'b>, CardinalityEstimation)>> {
        conditions
            .iter()
            .map(|condition| match condition {
                Condition::Filter(filter) => {
                    let (optimized_filter, estimation) = self.optimize_filter(
                        filter,
                        payload_provider.clone(),
                        total,
                        deferred_behavior,
                        materialization,
                    )?;
                    Ok((
                        ConvertedCondition::Checker(ConditionCheckerEnum::Filter(optimized_filter)),
                        estimation,
                    ))
                }
                Condition::Field(_)
                | Condition::IsEmpty(_)
                | Condition::IsNull(_)
                | Condition::HasId(_)
                | Condition::HasVector(_)
                | Condition::Slice(_)
                | Condition::Nested(_)
                | Condition::CustomIdChecker(_) => {
                    let estimation =
                        self.condition_cardinality(condition, None, deferred_behavior)?;
                    if let Some(hint) = materialization
                        && let Condition::Field(field_condition) = condition
                        && !hint.is_primary(condition)
                        && let Some(posting) =
                            self.read_posting(field_condition, &estimation, hint.expected_checks)?
                    {
                        return Ok((ConvertedCondition::Posting(posting), estimation));
                    }
                    let condition_checker = self.condition_converter(
                        condition,
                        payload_provider.clone(),
                        deferred_behavior,
                    )?;
                    Ok((ConvertedCondition::Checker(condition_checker), estimation))
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
        materialization: Option<MaterializationHint<'_>>,
    ) -> OperationResult<(Vec<ConditionCheckerEnum<'b>>, CardinalityEstimation)> {
        if conditions.is_empty() {
            // Empty `should` => match every point.
            return Ok((Vec::new(), CardinalityEstimation::exact(total)));
        }

        let mut converted = self.convert_conditions_with_postings(
            conditions,
            payload_provider,
            total,
            deferred_behavior,
            materialization,
        )?;
        // More probable conditions first
        converted.sort_by_key(|(_, estimation)| Reverse(estimation.exp));
        let (conditions, estimations): (Vec<_>, Vec<_>) = converted.into_iter().unzip();
        let conditions = merge_postings(
            conditions,
            PostingMerge::Union,
            self.point_set_context(materialization),
        );

        Ok((conditions, combine_should_estimations(&estimations, total)))
    }

    fn optimize_min_should<'b, S: PayloadStorageRead + 'b>(
        &'b self,
        conditions: &'b [Condition],
        min_count: usize,
        payload_provider: PayloadProvider<S>,
        total: usize,
        deferred_behavior: DeferredBehavior,
        materialization: Option<MaterializationHint<'_>>,
    ) -> OperationResult<(Vec<ConditionCheckerEnum<'b>>, CardinalityEstimation)> {
        // "At least `min_count` of" needs a count per point, so posting lists read up
        // front stay one set each instead of being merged.
        let mut converted = self.convert_conditions(
            conditions,
            payload_provider,
            total,
            deferred_behavior,
            materialization,
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
        materialization: Option<MaterializationHint<'_>>,
    ) -> OperationResult<(Vec<ConditionCheckerEnum<'b>>, CardinalityEstimation)> {
        let mut converted = self.convert_conditions_with_postings(
            conditions,
            payload_provider,
            total,
            deferred_behavior,
            materialization,
        )?;
        // Less probable conditions first
        converted.sort_by_key(|(_, estimation)| estimation.exp);
        let (conditions, estimations): (Vec<_>, Vec<_>) = converted.into_iter().unzip();
        let conditions = merge_postings(
            conditions,
            PostingMerge::Intersection,
            self.point_set_context(materialization),
        );

        Ok((conditions, combine_must_estimations(&estimations, total)))
    }

    fn optimize_must_not<'b, S: PayloadStorageRead + 'b>(
        &'b self,
        conditions: &'b [Condition],
        payload_provider: PayloadProvider<S>,
        total: usize,
        deferred_behavior: DeferredBehavior,
        materialization: Option<MaterializationHint<'_>>,
    ) -> OperationResult<(Vec<ConditionCheckerEnum<'b>>, CardinalityEstimation)> {
        let mut converted = self.convert_conditions_with_postings(
            conditions,
            payload_provider,
            total,
            deferred_behavior,
            materialization,
        )?;
        // More probable conditions first, as it will be reverted
        converted.sort_by_key(|(_, estimation)| estimation.exp);
        let (conditions, estimations): (Vec<_>, Vec<_>) = converted.into_iter().unzip();
        // None of the conditions may match: one set of the points matching any of them.
        let conditions = merge_postings(
            conditions,
            PostingMerge::Union,
            self.point_set_context(materialization),
        );

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

    fn point_set_context(
        &self,
        materialization: Option<MaterializationHint<'_>>,
    ) -> Option<PointSetContext> {
        materialization.map(|hint| hint.point_set_context(self.id_tracker.total_point_count()))
    }

    /// Read the posting list of `condition`, if that is cheaper than checking the
    /// expected points through the index one by one
    /// (see [`ExpectedChecks::max_posting_len`]).
    fn read_posting(
        &self,
        condition: &FieldCondition,
        estimation: &CardinalityEstimation,
        expected_checks: ExpectedChecks,
    ) -> OperationResult<Option<Vec<PointOffsetType>>> {
        if !reads_posting_list(condition) {
            return Ok(None);
        }
        let Some(indexes) = self.field_indexes.get(&condition.key) else {
            return Ok(None);
        };
        for index in indexes {
            if estimation.exp > expected_checks.max_posting_len(index.checks_read_disk()) {
                continue;
            }
            if let Some(points) = index.filter(condition)? {
                return Ok(Some(points.collect()));
            }
        }
        Ok(None)
    }
}
