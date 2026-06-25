use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::types::DeferredBehavior;
use parking_lot::{RwLock, RwLockReadGuard};
use segment::common::check_stopped;
use segment::common::operation_error::OperationResult;
use segment::entry::ReadSegmentEntry;
use segment::types::PointIdType;

use crate::locked_segment::LockedSegment;
use crate::segment_holder::SegmentHolder;

impl SegmentHolder {
    #[inline]
    fn _read_points<R, F>(
        segments: impl IntoIterator<Item = Arc<RwLock<R>>>,
        ids: &[PointIdType],
        is_stopped: &AtomicBool,
        deferred_behavior: DeferredBehavior,
        mut f: F,
    ) -> OperationResult<usize>
    where
        R: ReadSegmentEntry + ?Sized,
        F: FnMut(&[PointIdType], &RwLockReadGuard<R>) -> OperationResult<usize>,
    {
        let mut read_points = 0;
        for segment in segments {
            let read_segment = segment.read();
            let segment_point_ids: Vec<PointIdType> = ids
                .iter()
                .copied()
                .filter(|id| read_segment.has_point(*id, deferred_behavior))
                .collect();
            check_stopped(is_stopped)?;
            if !segment_point_ids.is_empty() {
                read_points += f(&segment_point_ids, &read_segment)?;
            }
        }
        Ok(read_points)
    }

    /// Read points over an explicit, pre-collected set of type-erased read handles.
    ///
    /// Lets callers that hold their own snapshot of segments (e.g. `EdgeReadView`, or a read-only
    /// follower shard) reuse the same read/version-dedup core as the holder-based entry points.
    pub fn read_points_over<R, F>(
        segments: impl IntoIterator<Item = Arc<RwLock<R>>>,
        ids: &[PointIdType],
        is_stopped: &AtomicBool,
        deferred_behavior: DeferredBehavior,
        f: F,
    ) -> OperationResult<usize>
    where
        R: ReadSegmentEntry + ?Sized,
        F: FnMut(&[PointIdType], &RwLockReadGuard<R>) -> OperationResult<usize>,
    {
        Self::_read_points(segments, ids, is_stopped, deferred_behavior, f)
    }

    fn segments_for_retrieval(&self) -> impl Iterator<Item = LockedSegment> {
        // We must go over non-appendable segments first, then go over appendable segments after
        // Points may be moved from non-appendable to appendable, because we don't lock all
        // segments together read ordering is very important here!
        //
        // Consider the following sequence:
        //
        // 1. Read-lock non-appendable segment A
        // 2. Atomic move from A to B
        // 3. Read-lock appendable segment B
        //
        // We are guaranteed to read all data consistently, and don't lose any points
        self.non_appendable_then_appendable_segments()
    }

    /// Provides an entry point of reading multiple points in all segments.
    ///
    pub fn read_points<F>(
        &self,
        ids: &[PointIdType],
        is_stopped: &AtomicBool,
        deferred_behavior: DeferredBehavior,
        f: F,
    ) -> OperationResult<usize>
    where
        F: FnMut(&[PointIdType], &RwLockReadGuard<dyn ReadSegmentEntry>) -> OperationResult<usize>,
    {
        let segments = self
            .segments_for_retrieval()
            .map(|segment| segment.get_read_arc());
        Self::_read_points(segments, ids, is_stopped, deferred_behavior, f)
    }
}
