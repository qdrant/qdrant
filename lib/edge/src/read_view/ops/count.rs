use std::collections::BTreeSet;

use common::ambient;
use common::reason::Reason;
use common::types::DeferredBehavior;
use segment::common::operation_error::OperationResult;
use segment::entry::ReadSegmentEntry;
use segment::index::field_index::EstimationMerge;
use shard::count::CountRequestInternal;

use crate::read_view::{EdgeReadView, ReadSegmentHandle};

impl<H: ReadSegmentHandle> EdgeReadView<H> {
    pub(crate) fn count(&self, request: CountRequestInternal) -> OperationResult<usize> {
        self.check_stopped()?;
        let _hw = ambient::unmeasured_guard(Reason::EDGE_UNMEASURED);
        let CountRequestInternal { filter, exact } = request;

        let points_count = if exact {
            let per_segment = self.par_map_segments(|segment| {
                segment.read_segment().read_filtered(
                    None,
                    None,
                    filter.as_ref(),
                    &self.is_stopped,
                    DeferredBehavior::VisibleOnly,
                )
            })?;

            per_segment
                .into_iter()
                .flatten()
                .collect::<BTreeSet<_>>()
                .len()
        } else {
            let estimations = self.par_map_segments(|segment| {
                segment
                    .read_segment() // blocking sync lock
                    .estimate_point_count(filter.as_ref())
            })?;

            estimations.into_iter().merge_independent().exp
        };

        Ok(points_count)
    }
}
