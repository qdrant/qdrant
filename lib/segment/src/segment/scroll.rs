use std::sync::atomic::{AtomicBool, Ordering};

use common::counter::hardware_counter::HardwareCounterCell;
use common::iterator_ext::IteratorExt;

use super::Segment;
use crate::entry::entry_point::SegmentEntry;
use crate::index::PayloadIndex;
use crate::spaces::tools::peek_top_smallest_iterable;
use crate::types::{Filter, PointIdType};

impl Segment {
    /// Estimates how many checks it would need for getting `limit` amount of points by streaming and then
    /// filtering, versus getting all filtered points from the index and then sorting them afterwards.
    ///
    /// If the filter is restrictive enough to yield fewer points than the amount of points a streaming
    /// approach would need to advance, it returns true.
    pub(super) fn should_pre_filter(
        &self,
        filter: &Filter,
        limit: Option<usize>,
        hw_counter: &HardwareCounterCell,
    ) -> bool {
        let query_cardinality = {
            let payload_index = self.payload_index.borrow();
            payload_index.estimate_cardinality(filter, hw_counter)
        };

        // ToDo: Add telemetry for this heuristics

        // Calculate expected number of condition checks required for
        // this scroll request with is stream strategy.
        // Example:
        //  - cardinality = 1000
        //  - limit = 10
        //  - total = 10000
        //  - point filter prob = 1000 / 10000 = 0.1
        //  - expected_checks = 10 / 0.1  = 100
        //  -------------------------------
        //  - cardinality = 10
        //  - limit = 10
        //  - total = 10000
        //  - point filter prob = 10 / 10000 = 0.001
        //  - expected_checks = 10 / 0.001  = 10000

        let available_points = self.available_point_count() + 1 /* + 1 for division-by-zero */;
        // Expected number of successful checks per point
        let check_probability =
            (query_cardinality.exp as f64 + 1.0/* protect from zero */) / available_points as f64;
        let exp_stream_checks =
            (limit.unwrap_or(available_points) as f64 / check_probability) as usize;

        // Assume it would require about `query cardinality` checks.
        // We are interested in approximate number of checks, so we can
        // use `query cardinality` as a starting point.
        let exp_index_checks = query_cardinality.max;

        exp_stream_checks > exp_index_checks
    }

    pub fn filtered_read_by_id_stream(
        &self,
        offset: Option<PointIdType>,
        limit: Option<usize>,
        condition: &Filter,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> Vec<PointIdType> {
        let payload_index = self.payload_index.borrow();
        let filter_context = payload_index.filter_context(condition, hw_counter);
        self.id_tracker
            .borrow()
            .iter_from(offset)
            .check_stop(|| is_stopped.load(Ordering::Relaxed))
            .filter(move |(_, internal_id)| filter_context.check(*internal_id))
            .map(|(external_id, _)| external_id)
            .take(limit.unwrap_or(usize::MAX))
            .collect()
    }

    pub(super) fn read_by_id_stream(
        &self,
        offset: Option<PointIdType>,
        limit: Option<usize>,
    ) -> Vec<PointIdType> {
        self.id_tracker
            .borrow()
            .iter_from(offset)
            .map(|x| x.0)
            .take(limit.unwrap_or(usize::MAX))
            .collect()
    }

    pub fn filtered_read_by_index(
        &self,
        offset: Option<PointIdType>,
        limit: Option<usize>,
        condition: &Filter,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> Vec<PointIdType> {
        let payload_index = self.payload_index.borrow();
        let id_tracker = self.id_tracker.borrow();
        let cardinality_estimation = payload_index.estimate_cardinality(condition, hw_counter);

        let ids_iterator = payload_index
            .iter_filtered_points(condition, &*id_tracker, &cardinality_estimation, hw_counter)
            .check_stop(|| is_stopped.load(Ordering::Relaxed))
            .filter_map(|internal_id| {
                let external_id = id_tracker.external_id(internal_id);
                match external_id {
                    Some(external_id) => match offset {
                        Some(offset) if external_id < offset => None,
                        _ => Some(external_id),
                    },
                    None => None,
                }
            });

        let mut page = match limit {
            Some(limit) => peek_top_smallest_iterable(ids_iterator, limit),
            None => ids_iterator.collect(),
        };

        page.sort_unstable();

        page
    }
}
