use std::sync::Arc;

use parking_lot::{RwLock, RwLockReadGuard};
use segment::entry::ReadSegmentEntry;
use segment::index::UniversalReadExt;
use segment::segment::read_only::ReadOnlySegment;
use shard::locked_segment::LockedSegment;

/// A handle to a single segment that can be read-locked to yield a [`ReadSegmentEntry`].
///
/// Abstracting over the handle (rather than the segment type) keeps the read path monomorphic for
/// homogeneous callers — a read-only follower's handle is the concrete
/// `Arc<RwLock<ReadOnlySegment<S>>>` — while the read-write shard, whose holder is intrinsically
/// heterogeneous (`Segment` and `ProxySegment` coexist during optimization), uses the
/// [`LockedSegment`] enum. Dynamic dispatch is confined to the `LockedSegment` impl, mirroring the
/// pre-existing [`LockedSegment::get_read`].
///
/// `Send + Sync` so a snapshot of handles can be shared across the shard's search thread pool and
/// read in parallel (see [`EdgeReadView::par_map_segments`]). Both concrete handles —
/// `Arc<RwLock<ReadOnlySegment<S>>>` and [`LockedSegment`] — already satisfy this.
///
/// [`EdgeReadView::par_map_segments`]: crate::read_view::EdgeReadView::par_map_segments
pub trait ReadSegmentHandle: Send + Sync {
    type Segment: ReadSegmentEntry + ?Sized;

    /// Acquire a read guard. One guard is held for the whole per-segment operation, so reads that
    /// call several methods on the same segment observe a consistent state.
    fn read_segment(&self) -> RwLockReadGuard<'_, Self::Segment>;

    /// Owned handle for the retrieval / version-dedup path ([`retrieve_over`]).
    ///
    /// [`retrieve_over`]: shard::retrieve::retrieve_blocking::retrieve_over
    fn segment_arc(&self) -> Arc<RwLock<Self::Segment>>;
}

impl<S: UniversalReadExt + 'static> ReadSegmentHandle for Arc<RwLock<ReadOnlySegment<S>>>
where
    S::Fs: Send + Sync,
{
    type Segment = ReadOnlySegment<S>;

    fn read_segment(&self) -> RwLockReadGuard<'_, ReadOnlySegment<S>> {
        self.read()
    }

    fn segment_arc(&self) -> Arc<RwLock<ReadOnlySegment<S>>> {
        self.clone()
    }
}

impl ReadSegmentHandle for LockedSegment {
    type Segment = dyn ReadSegmentEntry;

    fn read_segment(&self) -> RwLockReadGuard<'_, dyn ReadSegmentEntry> {
        self.get_read().read()
    }

    fn segment_arc(&self) -> Arc<RwLock<dyn ReadSegmentEntry>> {
        self.get_read_arc()
    }
}
