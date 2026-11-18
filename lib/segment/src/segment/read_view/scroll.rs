use std::cmp::Reverse;
use std::sync::atomic::AtomicBool;

use common::ambient::profile::{self, SearchStrategy, StrategyReason};
use common::condition_checker::ConditionChecker;
use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::iterator_ext::IteratorExt;
use common::types::DeferredBehavior;
use itertools::Itertools;

use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::index::field_index::CardinalityEstimation;
use crate::payload_storage::PayloadStorageRead;
use crate::segment::read_view::SegmentReadView;
use crate::segment::vector_data_read::VectorDataRead;
use crate::types::{Filter, PointIdType};

impl<'s, TIdT, TPI, TPS, TVD> SegmentReadView<'s, TIdT, TPI, TPS, TVD>
where
    TIdT: IdTrackerRead,
    TPI: PayloadIndexRead,
    TPS: PayloadStorageRead,
    TVD: VectorDataRead,
{
    /// Estimates how many checks it would need for getting `limit` amount of points by streaming
    /// and then filtering, versus getting all filtered points from the index and then sorting them
    /// afterwards.
    ///
    /// If the filter is restrictive enough to yield fewer points than the amount of points a
    /// streaming approach would need to advance, it returns true.
    pub fn should_pre_filter(
        &self,
        query_cardinality: &CardinalityEstimation,
        limit: Option<usize>,
    ) -> bool {
        // ToDo: Add telemetry for this heuristics

        // Calculate expected number of condition checks required for
        // this scroll request with stream strategy.
        let available_points = self.id_tracker.available_point_count() + 1 /* + 1 for division-by-zero */;
        // Expected number of successful checks per point
        let check_probability =
            (query_cardinality.exp as f64 + 1.0/* protect from zero */) / available_points as f64;
        let exp_stream_checks =
            (limit.unwrap_or(available_points) as f64 / check_probability) as usize;

        // Assume it would require about `query cardinality` checks.
        let exp_index_checks = query_cardinality.max;

        exp_stream_checks > exp_index_checks
    }

    pub fn read_by_id_stream(
        &self,
        offset: Option<PointIdType>,
        limit: Option<usize>,
        deferred_behavior: DeferredBehavior,
    ) -> Vec<PointIdType> {
        self.id_tracker
            .point_mappings()
            .iter_from_with_behavior(offset, deferred_behavior)
            .map(|x| x.0)
            .take(limit.unwrap_or(usize::MAX))
            .collect()
    }

    pub fn filtered_read_by_index(
        &self,
        offset: Option<PointIdType>,
        limit: Option<usize>,
        condition: &Filter,
        cardinality_estimation: &CardinalityEstimation,
        is_stopped: &AtomicBool,
        deferred_behavior: DeferredBehavior,
    ) -> OperationResult<Vec<PointIdType>> {
        let internal_ids = self.payload_index.iter_filtered_points(
            condition,
            cardinality_estimation,
            is_stopped,
            deferred_behavior,
        )?;

        if limit == Some(0) {
            return Ok(vec![]);
        }

        // The candidate set is fully drained either way: feed the whole
        // iterator into one batch pass and let the IO layer pipeline the
        // lookups. With a limit, only the `limit` smallest ids are kept
        // (mirrors `peek_top_smallest_iterable`, which cannot consume a
        // callback-fed stream).
        let mut page: Vec<PointIdType> = Vec::new();
        let mut top = limit.map(FixedLengthPriorityQueue::new);
        self.id_tracker
            .external_ids_batch(internal_ids, |_, external_id| {
                let past_offset = match offset {
                    Some(offset) => external_id >= offset,
                    None => true,
                };
                if !past_offset {
                    return;
                }
                match &mut top {
                    Some(top) => {
                        top.push(Reverse(external_id));
                    }
                    None => page.push(external_id),
                }
            })?;
        if let Some(top) = top {
            page = top
                .into_sorted_vec()
                .into_iter()
                .map(|Reverse(x)| x)
                .collect();
        }
        page.sort_unstable();
        Ok(page)
    }

    pub fn filtered_read_by_id_stream(
        &self,
        offset: Option<PointIdType>,
        limit: Option<usize>,
        condition: &Filter,
        is_stopped: &AtomicBool,
        deferred_behavior: DeferredBehavior,
    ) -> OperationResult<Vec<PointIdType>> {
        let filter_context = self.payload_index.filter_context(condition)?;
        self.id_tracker
            .point_mappings()
            .iter_from_with_behavior(offset, deferred_behavior)
            .stop_if(is_stopped)
            .try_filter(move |(_, internal_id)| filter_context.check(*internal_id))
            .map_ok(|(external_id, _)| external_id)
            .take(limit.unwrap_or(usize::MAX))
            .collect()
    }

    pub fn read_filtered<'a>(
        &'a self,
        offset: Option<PointIdType>,
        limit: Option<usize>,
        filter: Option<&'a Filter>,
        is_stopped: &AtomicBool,
        deferred_behavior: DeferredBehavior,
    ) -> OperationResult<Vec<PointIdType>> {
        match filter {
            None => {
                if note_unfiltered_read() {
                    return Ok(Vec::new());
                }
                Ok(self.read_by_id_stream(offset, limit, deferred_behavior))
            }
            Some(condition) => {
                let cardinality_estimation = self.payload_index.estimate_cardinality(condition)?;
                let pre_filter = self.should_pre_filter(&cardinality_estimation, limit);
                if note_read_plan(&cardinality_estimation, pre_filter) {
                    return Ok(Vec::new());
                }
                if pre_filter {
                    self.filtered_read_by_index(
                        offset,
                        limit,
                        condition,
                        &cardinality_estimation,
                        is_stopped,
                        deferred_behavior,
                    )
                } else {
                    self.filtered_read_by_id_stream(
                        offset,
                        limit,
                        condition,
                        is_stopped,
                        deferred_behavior,
                    )
                }
            }
        }
    }
}

/// Record the plan of an unfiltered read in the query profile, if profiled.
/// Returns true in explain mode: the caller must return without reading.
pub(super) fn note_unfiltered_read() -> bool {
    if profile::mode().is_none() {
        return false;
    }
    profile::note_strategy(SearchStrategy::UnfilteredRead, StrategyReason::Unfiltered);
    profile::is_explain()
}

/// Record the plan of a filtered read in the query profile, if profiled.
/// Returns true in explain mode: the caller must return without reading.
pub(super) fn note_read_plan(cardinality: &CardinalityEstimation, pre_filter: bool) -> bool {
    if profile::mode().is_none() {
        return false;
    }
    cardinality.note_to_profile();
    // Without primary clauses, reading "by index" iterates all points too
    let (strategy, reason) = match pre_filter {
        true if !cardinality.primary_clauses.is_empty() => (
            SearchStrategy::IndexedRead,
            StrategyReason::CardinalityBelowThreshold,
        ),
        true => (
            SearchStrategy::FullScanRead,
            StrategyReason::CardinalityBelowThreshold,
        ),
        false => (
            SearchStrategy::FullScanRead,
            StrategyReason::CardinalityAboveThreshold,
        ),
    };
    profile::note_strategy(strategy, reason);
    profile::is_explain()
}
