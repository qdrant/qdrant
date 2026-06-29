use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::DeferredBehavior;
use segment::common::operation_error::OperationResult;
use segment::entry::ReadSegmentEntry;
use segment::index::field_index::EstimationMerge;
use shard::count::CountRequestInternal;

use crate::read_view::{EdgeReadView, ReadSegmentHandle};

impl<H: ReadSegmentHandle> EdgeReadView<H> {
    pub(crate) fn count(&self, request: CountRequestInternal) -> OperationResult<usize> {
        let CountRequestInternal { filter, exact } = request;

        let points_count = if exact {
            let per_segment = self.par_map_segments(|segment| {
                segment.read_segment().read_filtered(
                    None,
                    None,
                    filter.as_ref(),
                    &AtomicBool::new(false),
                    &HardwareCounterCell::disposable(),
                    DeferredBehavior::VisibleOnly,
                )
            })?;

            per_segment.into_iter().flatten().count()
        } else {
            let estimations = self.par_map_segments(|segment| {
                segment
                    .read_segment() // blocking sync lock
                    .estimate_point_count(filter.as_ref(), &HardwareCounterCell::disposable())
            })?;

            estimations.into_iter().merge_independent().exp
        };

        Ok(points_count)
    }
}
