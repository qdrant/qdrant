use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::DeferredBehavior;

use super::Segment;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::spaces::tools::peek_top_smallest_iterable;
use crate::types::{Filter, PointIdType};

impl Segment {
    pub fn filtered_read_by_index(
        &self,
        offset: Option<PointIdType>,
        limit: Option<usize>,
        condition: &Filter,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
        deferred_behavior: DeferredBehavior,
    ) -> OperationResult<Vec<PointIdType>> {
        let effective_deferred_id = deferred_behavior.apply(self.deferred_internal_id());

        let payload_index = self.payload_index.borrow();
        let id_tracker = self.id_tracker.borrow();
        let cardinality_estimation = payload_index.estimate_cardinality(condition, hw_counter)?;
        let point_mappings = id_tracker.point_mappings();

        let ids_iterator = payload_index
            .iter_filtered_points(
                condition,
                &id_tracker,
                &point_mappings,
                &cardinality_estimation,
                hw_counter,
                is_stopped,
                effective_deferred_id,
            )?
            .filter_map(|internal_id| {
                let external_id = id_tracker.external_id(internal_id)?;
                match offset {
                    Some(offset) if external_id < offset => None,
                    _ => Some(external_id),
                }
            });

        let mut page = match limit {
            Some(limit) => peek_top_smallest_iterable(ids_iterator, limit),
            None => ids_iterator.collect(),
        };
        page.sort_unstable();
        Ok(page)
    }
}
