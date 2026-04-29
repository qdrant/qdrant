use std::ops::Deref;

use crate::id_tracker::IdTrackerEnum;
use crate::segment::Segment;
use crate::segment::read_view::SegmentReadView;

impl Segment {
    pub fn with_view<T>(&self, f: impl Fn(SegmentReadView<'_, IdTrackerEnum>) -> T) -> T {
        let id_tracker = self.id_tracker.borrow();

        let view = SegmentReadView {
            id_tracker: id_tracker.deref(),
        };

        f(view)
    }
}
