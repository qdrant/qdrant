use crate::proxy_segment::ProxySegment;
use parking_lot::RwLockReadGuard;
use segment::entry::SegmentEntry;

pub struct ProxyReadSegmentGuard<'a> {
    pub(super) proxy: &'a ProxySegment,

    pub(super) wrapped_guard: RwLockReadGuard<'a, dyn SegmentEntry>,
    pub(super) write_guard: RwLockReadGuard<'a, dyn SegmentEntry>,
}

impl ProxySegment {
    pub fn as_read_guard(&self) -> ProxyReadSegmentGuard<'_> {
        ProxyReadSegmentGuard {
            proxy: self,
            wrapped_guard: self.wrapped_segment.get().read(),
            write_guard: self.write_segment.get().read(),
        }
    }
}
