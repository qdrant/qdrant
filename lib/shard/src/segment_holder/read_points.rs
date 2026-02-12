use std::sync::atomic::AtomicBool;
use std::time::Duration;

use parking_lot::RwLockReadGuard;
use segment::common::check_stopped;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::entry::NonAppendableSegmentEntry;
use segment::types::PointIdType;

use crate::locked_segment::LockedSegment;
use crate::segment_holder::SegmentHolder;
use crate::segment_holder::locked::LockedSegmentHolder;

impl SegmentHolder {
    #[inline]
    fn _read_points<F>(
        segments: impl IntoIterator<Item = LockedSegment>,
        ids: &[PointIdType],
        is_stopped: &AtomicBool,
        mut f: F,
    ) -> OperationResult<usize>
    where
        F: FnMut(
            &[PointIdType],
            &RwLockReadGuard<dyn NonAppendableSegmentEntry>,
        ) -> OperationResult<usize>,
    {
        let mut read_points = 0;
        for segment in segments {
            let segment_arc = segment.get_non_appendable();
            let read_segment = segment_arc.read();
            let segment_point_ids: Vec<PointIdType> = ids
                .iter()
                .copied()
                .filter(|id| read_segment.has_point(*id))
                .collect();
            check_stopped(is_stopped)?;
            if !segment_point_ids.is_empty() {
                read_points += f(&segment_point_ids, &read_segment)?;
            }
        }
        Ok(read_points)
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
    /// Uses LockedSegmentHolder instead of &SegmentHolder to release lock earlier.
    ///
    pub fn read_points_locked<F>(
        locked_holder: &LockedSegmentHolder,
        ids: &[PointIdType],
        is_stopped: &AtomicBool,
        timeout: Duration,
        f: F,
    ) -> OperationResult<usize>
    where
        F: FnMut(
            &[PointIdType],
            &RwLockReadGuard<dyn NonAppendableSegmentEntry>,
        ) -> OperationResult<usize>,
    {
        let segments: Vec<_> = {
            let Some(holder_guard) = locked_holder.try_read_for(timeout) else {
                return Err(OperationError::timeout(timeout, "fill query context"));
            };

            holder_guard.segments_for_retrieval().collect()
        };

        Self::_read_points(segments, ids, is_stopped, f)
    }

    /// Provides an entry point of reading multiple points in all segments.
    ///
    pub fn read_points<F>(
        &self,
        ids: &[PointIdType],
        is_stopped: &AtomicBool,
        f: F,
    ) -> OperationResult<usize>
    where
        F: FnMut(
            &[PointIdType],
            &RwLockReadGuard<dyn NonAppendableSegmentEntry>,
        ) -> OperationResult<usize>,
    {
        let segments = self.segments_for_retrieval();
        Self::_read_points(segments, ids, is_stopped, f)
    }
}
