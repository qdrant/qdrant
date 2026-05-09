use std::ops::Deref;

use crate::segment::Segment;
use crate::segment::read_view::SegmentReadViewFor;

impl Segment {
    pub fn with_view<T>(&self, f: impl FnOnce(SegmentReadViewFor<'_>) -> T) -> T {
        let id_tracker = self.id_tracker.borrow();
        let payload_index = self.payload_index.borrow();
        let payload_storage = self.payload_storage.borrow();

        // Nest the payload-index view inside the segment view so reads on
        // the segment view go through `StructPayloadIndexReadView`'s
        // `PayloadIndexRead` impl. The inner `with_view` borrows the index's
        // `id_tracker` cell once for the closure scope.
        payload_index.with_view(|payload_index_view| {
            let view = SegmentReadViewFor {
                id_tracker: id_tracker.deref(),
                payload_index: &payload_index_view,
                payload_storage: payload_storage.deref(),
                vector_data: &self.vector_data,
                segment_config: &self.segment_config,
                deferred_point_status: self.deferred_point_status.as_ref(),
                appendable_flag: self.appendable_flag,
            };

            f(view)
        })
    }
}
