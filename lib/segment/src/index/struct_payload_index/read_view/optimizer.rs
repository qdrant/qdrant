use std::cmp::Reverse;

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
use crate::index::query_optimization::bits_checker::BitsConditionChecker;
use crate::index::query_optimization::optimized_filter::OptimizedFilter;
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::payload_storage::PayloadStorageRead;
use crate::types::{Condition, FieldCondition, Filter, MinShould};
use crate::vector_storage::VectorStorageRead;

/// Largest posting list, relative to the expected number of checks, worth reading into
/// a bitset for an on-disk index (a check is a random read when the page cache is cold).
const DISK_POSTING_TO_CHECKS_RATIO: usize = 64;

/// Same as [`DISK_POSTING_TO_CHECKS_RATIO`], for an index held in RAM.
const RAM_POSTING_TO_CHECKS_RATIO: usize = 4;

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
    /// * `expected_checks` - how many points the caller will check against the result,
    ///   if known. A field condition whose posting list is small relative to that is read
    ///   into a bitset up front instead of being checked through its index point by point.
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
        expected_checks: Option<usize>,
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
            expected_checks,
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
            expected_checks,
        )?;
        filter_estimations.push(estimation);

        let (must, estimation) = self.optimize_must(
            must.as_deref().unwrap_or(&[]),
            payload_provider.clone(),
            total,
            deferred_behavior,
            expected_checks,
        )?;
        filter_estimations.push(estimation);

        let (must_not, estimation) = self.optimize_must_not(
            must_not.as_deref().unwrap_or(&[]),
            payload_provider,
            total,
            deferred_behavior,
            expected_checks,
        )?;
        filter_estimations.push(estimation);

        let optimized_filter =
            OptimizedFilter::new(should, min_should, min_should_count, must, must_not);

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
        expected_checks: Option<usize>,
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
                        expected_checks,
                    )?;
                    Ok((ConditionCheckerEnum::Filter(optimized_filter), estimation))
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
                    if let Some(expected_checks) = expected_checks
                        && let Condition::Field(field_condition) = condition
                        && let Some(bits) =
                            self.materialize(field_condition, &estimation, expected_checks)?
                    {
                        return Ok((ConditionCheckerEnum::Bits(bits), estimation));
                    }
                    let condition_checker = self.condition_converter(
                        condition,
                        payload_provider.clone(),
                        deferred_behavior,
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
        expected_checks: Option<usize>,
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
            expected_checks,
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
        expected_checks: Option<usize>,
    ) -> OperationResult<(Vec<ConditionCheckerEnum<'b>>, CardinalityEstimation)> {
        let mut converted = self.convert_conditions(
            conditions,
            payload_provider,
            total,
            deferred_behavior,
            expected_checks,
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
        expected_checks: Option<usize>,
    ) -> OperationResult<(Vec<ConditionCheckerEnum<'b>>, CardinalityEstimation)> {
        let mut converted = self.convert_conditions(
            conditions,
            payload_provider,
            total,
            deferred_behavior,
            expected_checks,
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
        expected_checks: Option<usize>,
    ) -> OperationResult<(Vec<ConditionCheckerEnum<'b>>, CardinalityEstimation)> {
        let mut converted = self.convert_conditions(
            conditions,
            payload_provider,
            total,
            deferred_behavior,
            expected_checks,
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

    /// Read the posting list of `condition` into a bitset, if that is cheaper than
    /// checking `expected_checks` points through the index one by one.
    ///
    /// A posting entry is 4 sequential bytes, while a per-point check is a lookup that,
    /// on an on-disk index with a cold page cache, is a random read. So reading a posting
    /// list pays off up to [`DISK_POSTING_TO_CHECKS_RATIO`] times the number of checks for
    /// an on-disk index, and up to [`RAM_POSTING_TO_CHECKS_RATIO`] times for an in-RAM one.
    fn materialize(
        &self,
        condition: &FieldCondition,
        estimation: &CardinalityEstimation,
        expected_checks: usize,
    ) -> OperationResult<Option<BitsConditionChecker>> {
        if !reads_posting_list(condition) {
            return Ok(None);
        }
        let Some(indexes) = self.field_indexes.get(&condition.key) else {
            return Ok(None);
        };
        for index in indexes {
            let ratio = if index.is_on_disk() {
                DISK_POSTING_TO_CHECKS_RATIO
            } else {
                RAM_POSTING_TO_CHECKS_RATIO
            };
            if estimation.exp > expected_checks.saturating_mul(ratio) {
                continue;
            }
            if let Some(points) = index.filter(condition)? {
                let len = self.id_tracker.total_point_count();
                return Ok(Some(BitsConditionChecker::from_points(len, points)));
            }
        }
        Ok(None)
    }
}

/// Whether the index answers `condition` by reading posting lists or sorted values,
/// so that materializing it costs about one sequential entry per match. Geo conditions
/// re-check every point of the matching geohash cells and `values_count` scans points,
/// so those stay per-point checks.
fn reads_posting_list(condition: &FieldCondition) -> bool {
    let FieldCondition {
        key: _,
        r#match,
        range,
        geo_bounding_box,
        geo_radius,
        geo_polygon,
        values_count,
        is_empty,
        is_null,
    } = condition;
    (r#match.is_some() || range.is_some())
        && geo_bounding_box.is_none()
        && geo_radius.is_none()
        && geo_polygon.is_none()
        && values_count.is_none()
        && is_empty.is_none()
        && is_null.is_none()
}
