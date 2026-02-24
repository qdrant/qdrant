use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::iterator_ext::IteratorExt;

use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::VectorInternal;
use crate::id_tracker::IdTracker;
use crate::segment::Segment;
use crate::types::{PointIdType, VectorName};

impl Segment {
    pub(crate) fn read_vectors(
        &self,
        vector_names: &VectorName,
        point_ids: &[PointIdType],
        hw_counter: &HardwareCounterCell,
        is_stopped: &AtomicBool,
        mut callback: impl FnMut(PointIdType, VectorInternal),
    ) -> OperationResult<()> {
        let mut error = None;
        let internal_ids = point_ids
            .iter()
            .copied()
            .stop_if(is_stopped)
            .filter_map(|point_id| match self.lookup_internal_id(point_id) {
                Ok(point_offset) => Some(point_offset),
                Err(err) => {
                    error = Some(err);
                    None
                }
            });
        self.vectors_by_offsets(
            vector_names,
            internal_ids,
            hw_counter,
            |point_offset, vector_internal| {
                if let Some(point_id) = self.id_tracker.borrow().external_id(point_offset) {
                    callback(point_id, vector_internal);
                }
            },
        )?;
        if let Some(err) = error {
            return Err(err);
        }
        Ok(())
    }
}
