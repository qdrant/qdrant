use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::DeferredBehavior;
use itertools::Itertools as _;
use segment::common::operation_error::OperationResult;
use segment::entry::ReadSegmentEntry;
use segment::index::field_index::EstimationMerge;
use shard::count::CountRequestInternal;

use crate::read_view::{EdgeReadView, ReadSegmentHandle};

impl<H: ReadSegmentHandle> EdgeReadView<H> {
    pub(crate) fn count(&self, request: CountRequestInternal) -> OperationResult<usize> {
        let CountRequestInternal { filter, exact } = request;

        let points_count = if exact {
            self.segments
                .iter()
                .map(|segment| {
                    segment.read_segment().read_filtered(
                        None,
                        None,
                        filter.as_ref(),
                        &AtomicBool::new(false),
                        &HardwareCounterCell::disposable(),
                        DeferredBehavior::VisibleOnly,
                    )
                })
                .process_results(|iter| iter.flatten().count())?
        } else {
            let cardinality = self
                .segments
                .iter()
                .map(|segment| {
                    segment
                        .read_segment() // blocking sync lock
                        .estimate_point_count(filter.as_ref(), &HardwareCounterCell::disposable())
                })
                .process_results(|iter| iter.merge_independent())?;

            cardinality.exp
        };

        Ok(points_count)
    }
}
