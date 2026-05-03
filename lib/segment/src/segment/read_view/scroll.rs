use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::iterator_ext::IteratorExt;
use common::types::DeferredBehavior;

use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
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
        filter: &Filter,
        limit: Option<usize>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        let query_cardinality = self
            .payload_index
            .estimate_cardinality(filter, hw_counter)?;

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

        Ok(exp_stream_checks > exp_index_checks)
    }

    pub fn read_by_id_stream(
        &self,
        offset: Option<PointIdType>,
        limit: Option<usize>,
        deferred_behavior: DeferredBehavior,
    ) -> Vec<PointIdType> {
        let effective_deferred_id = deferred_behavior.apply(self.deferred_internal_id());

        self.id_tracker
            .point_mappings()
            .iter_from_visible(offset, effective_deferred_id)
            .map(|x| x.0)
            .take(limit.unwrap_or(usize::MAX))
            .collect()
    }

    pub fn filtered_read_by_id_stream(
        &self,
        offset: Option<PointIdType>,
        limit: Option<usize>,
        condition: &Filter,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
        deferred_behavior: DeferredBehavior,
    ) -> OperationResult<Vec<PointIdType>> {
        let effective_deferred_id = deferred_behavior.apply(self.deferred_internal_id());

        let filter_context = self.payload_index.filter_context(condition, hw_counter)?;
        Ok(self
            .id_tracker
            .point_mappings()
            .iter_from_visible(offset, effective_deferred_id)
            .stop_if(is_stopped)
            .filter(move |(_, internal_id)| filter_context.check(*internal_id))
            .map(|(external_id, _)| external_id)
            .take(limit.unwrap_or(usize::MAX))
            .collect())
    }
}
