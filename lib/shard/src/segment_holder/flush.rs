use std::cmp::{max, min};
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;

use parking_lot::{RwLock, RwLockReadGuard};
use segment::common::operation_error::{OperationError, OperationResult};
use segment::entry::SegmentEntry;
use segment::types::SeqNumberType;

use crate::locked_segment::LockedSegment;
use crate::segment_holder::{SegmentHolder, SegmentId};

impl SegmentHolder {
    /// Flushes all segments and returns maximum version to persist
    ///
    /// Before flushing, this read-locks all segments. It prevents writes to all not-yet-flushed
    /// segments during flushing. All locked segments are flushed and released one by one.
    ///
    /// If there are unsaved changes after flush - detects lowest unsaved change version.
    /// If all changes are saved - returns max version.
    pub fn flush_all(&self, sync: bool, force: bool) -> OperationResult<SeqNumberType> {
        let lock_order: Vec<_> = self.segment_flush_ordering().collect();

        // Grab and keep to segment RwLock's until the end of this function
        let segments = self.segment_locks(lock_order.iter().cloned())?;

        // We can never have zero segments
        // Having zero segments could permanently corrupt the WAL by acknowledging u64::MAX
        assert!(
            !segments.is_empty(),
            "must always have at least one segment",
        );

        // Read-lock all segments before flushing any, must prevent any writes to any segment
        // That is to prevent any copy-on-write operation on two segments from occurring in between
        // flushing the two segments. If that would happen, segments could end up in an
        // inconsistent state on disk that is not recoverable after a crash.
        //
        // E.g.: we have a point on an immutable segment. If we use a set-payload operation, we do
        // copy-on-write. The point from immutable segment A is deleted, the updated point is
        // stored on appendable segment B.
        // Because of flush ordering segment B (appendable) is flushed before segment A
        // (not-appendable). If the copy-on-write operation happens in between, the point is
        // deleted from A, but the new point in B is not persisted. We cannot recover this by
        // replaying the WAL in case of a crash because the point in A does not exist anymore,
        // making copy-on-write impossible.
        // Locking all segments prevents copy-on-write operations from occurring in between
        // flushes.
        //
        // WARNING: Ordering is very important here. Specifically:
        // - We MUST lock non-appendable first, then appendable.
        // - We MUST flush appendable first, then non-appendable
        // Because of this, two rev(erse) calls are used below here.
        //
        // Locking must happen in this order because `apply_points_to_appendable` can take two
        // write locks, also in this order. If we'd use different ordering we will eventually end
        // up with a deadlock.
        let mut segment_reads: Vec<_> = segments
            .iter()
            .rev()
            .map(|segment| Self::aloha_lock_segment_read(segment))
            .collect();
        segment_reads.reverse();

        // Assert we flush appendable segments first
        debug_assert!(
            segment_reads
                .windows(2)
                .all(|s| s[0].is_appendable() || !s[1].is_appendable()),
            "Must flush appendable segments first",
        );

        if !sync && self.is_background_flushing() {
            // There is already a background flush ongoing, return current max persisted version
            return self.get_max_persisted_version(segment_reads, lock_order);
        }

        // This lock also prevents multiple parallel sync flushes
        // as it is exclusive
        let mut background_flush_lock = self.lock_flushing()?;

        if sync {
            for read_segment in segment_reads.iter() {
                read_segment.flush(force)?;
            }
        } else {
            let flushers: Vec<_> = segment_reads
                .iter()
                .filter_map(|read_segment| read_segment.flusher(force))
                .collect();

            *background_flush_lock = Some(
                std::thread::Builder::new()
                    .name("background_flush".to_string())
                    .spawn(move || {
                        for flusher in flushers {
                            flusher()?;
                        }
                        Ok(())
                    })
                    .unwrap(),
            );
        }

        self.get_max_persisted_version(segment_reads, lock_order)
    }

    /// Defines flush ordering for segments.
    ///
    /// Flush appendable segments first, then non-appendable.
    /// This is done to ensure that all data, transferred from non-appendable segments to appendable segments
    /// is persisted, before marking records in non-appendable segments as removed.
    fn segment_flush_ordering(&self) -> impl Iterator<Item = SegmentId> {
        let appendable_segments = self.appendable_segments_ids();
        let non_appendable_segments = self.non_appendable_segments_ids();

        appendable_segments
            .into_iter()
            .chain(non_appendable_segments)
    }

    // Joins flush thread if exists
    // Returns lock to guarantee that there will be no other flush in a different thread
    pub(super) fn lock_flushing(
        &self,
    ) -> OperationResult<parking_lot::MutexGuard<'_, Option<JoinHandle<OperationResult<()>>>>> {
        let mut lock = self.flush_thread.lock();
        let mut join_handle: Option<JoinHandle<OperationResult<()>>> = None;
        std::mem::swap(&mut join_handle, &mut lock);
        if let Some(join_handle) = join_handle {
            // Flush result was reported to segment, so we don't need this value anymore
            join_handle
                .join()
                .map_err(|_err| OperationError::service_error("failed to join flush thread"))??;
        }
        Ok(lock)
    }

    pub(super) fn is_background_flushing(&self) -> bool {
        let lock = self.flush_thread.lock();
        if let Some(join_handle) = lock.as_ref() {
            !join_handle.is_finished()
        } else {
            false
        }
    }

    /// Calculates the version of the segments that is safe to acknowledge in WAL
    ///
    /// If there are unsaved changes after flush - detects lowest unsaved change version.
    /// If all changes are saved - returns max version.
    fn get_max_persisted_version(
        &self,
        segment_reads: Vec<RwLockReadGuard<'_, dyn SegmentEntry>>,
        lock_order: Vec<SegmentId>,
    ) -> OperationResult<SeqNumberType> {
        // Start with the max_persisted_vesrion at the set overwrite value, which may just be 0
        // Any of the segments we flush may increase this if they have a higher persisted version
        // The overwrite is required to ensure we acknowledge no-op operations in WAL that didn't hit any segment
        //
        // Only affects returned version if all changes are saved
        let mut max_persisted_version: SeqNumberType = self
            .max_persisted_segment_version_overwrite
            .load(Ordering::Relaxed);

        let mut min_unsaved_version: SeqNumberType = SeqNumberType::MAX;
        let mut has_unsaved = false;

        for (read_segment, segment_id) in segment_reads.into_iter().zip(lock_order.into_iter()) {
            let segment_version = read_segment.version();
            let segment_persisted_version = read_segment.persistent_version();

            log::trace!(
                "Flushed segment {segment_id}:{:?} version: {segment_version} to persisted: {segment_persisted_version}",
                &read_segment.data_path(),
            );

            if segment_version > segment_persisted_version {
                has_unsaved = true;
                min_unsaved_version = min(min_unsaved_version, segment_persisted_version);
            }

            max_persisted_version = max(max_persisted_version, segment_persisted_version);

            drop(read_segment);
        }

        if has_unsaved {
            log::trace!(
                "Some segments have unsaved changes, lowest unsaved version: {min_unsaved_version}"
            );
            Ok(min_unsaved_version)
        } else {
            log::trace!(
                "All segments flushed successfully, max persisted version: {max_persisted_version}"
            );
            Ok(max_persisted_version)
        }
    }

    /// Grab the RwLock's for all the given segment IDs.
    fn segment_locks(
        &self,
        segment_ids: impl IntoIterator<Item = SegmentId>,
    ) -> OperationResult<Vec<&RwLock<dyn SegmentEntry>>> {
        segment_ids
            .into_iter()
            .map(|segment_id| {
                self.get(segment_id)
                    .ok_or_else(|| {
                        OperationError::service_error(format!("No segment with ID {segment_id}"))
                    })
                    .map(LockedSegment::get)
            })
            .collect()
    }
}
