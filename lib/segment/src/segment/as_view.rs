use std::ops::Deref;

use crate::segment::Segment;
use crate::segment::read_view::SegmentReadViewFor;

impl Segment {
    pub fn with_view<T>(&self, f: impl FnOnce(SegmentReadViewFor<'_>) -> T) -> T {
        let id_tracker = self.id_tracker.borrow();
        let payload_index = self.payload_index.borrow();
        let payload_storage = self.payload_storage.borrow();

        let view = SegmentReadViewFor {
            id_tracker: id_tracker.deref(),
            payload_index: payload_index.deref(),
            payload_storage: payload_storage.deref(),
            vector_data: &self.vector_data,
            segment_config: &self.segment_config,
            deferred_point_status: self.deferred_point_status.as_ref(),
            appendable_flag: self.appendable_flag,
        };

        f(view)
    }
}
