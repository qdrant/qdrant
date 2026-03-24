mod flush;
pub mod locked;
pub mod read_points;
mod snapshot;
#[cfg(test)]
mod tests;

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use ahash::{AHashMap, AHashSet};
use common::counter::hardware_counter::HardwareCounterCell;
use common::process_counter::ProcessCounter;
use common::save_on_disk::SaveOnDisk;
use common::toposort::TopoSort;
use common::types::PointOffsetType;
use itertools::Itertools;
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard};
use rand::seq::IndexedRandom;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::named_vectors::NamedVectors;
use segment::entry::{
    NonAppendableSegmentEntry, ReadSegmentEntry, SegmentEntry, StorageSegmentEntry,
};
use segment::segment::Segment;
use segment::segment_constructor::build_segment;
use segment::types::{ExtendedPointId, Payload, PointIdType, SegmentConfig, SeqNumberType};
use smallvec::SmallVec;

use crate::locked_segment::LockedSegment;
use crate::payload_index_schema::PayloadIndexSchema;

pub type SegmentId = usize;

/// All occurrences of a point across segments: (segment_id, version, is_deferred).
type PointOccurrences = SmallVec<[(SegmentId, SeqNumberType, bool); 2]>;

#[derive(Debug, Default)]
pub struct SegmentHolder {
    /// Keep segments sorted by their ID for deterministic iteration order
    appendable_segments: BTreeMap<SegmentId, LockedSegment>,
    non_appendable_segments: BTreeMap<SegmentId, LockedSegment>,

    /// Source for unique (virtual) IDs for newly added segments
    id_source: AtomicUsize,

    /// Seq number of the first un-recovered operation.
    /// If there are no failed operation - None
    pub failed_operation: BTreeSet<SeqNumberType>,

    /// Holds the first uncorrected error happened with optimizer
    pub optimizer_errors: Option<String>,

    /// A special segment version that is usually used to keep track of manually bumped segment versions.
    /// An example for this are operations that don't modify any points but could be expensive to recover from during WAL recovery.
    /// To acknowledge them in WAL, we overwrite the max_persisted value in `Self::flush_all` with the segment version stored here.
    max_persisted_segment_version_overwrite: AtomicU64,

    /// Dependency map for flushing segments.
    /// This structure defines which segments must be flushed before others.
    /// Dependency graph also stores the maximum version of the operation, which created the dependency,
    /// so we can clear all dependencies after flushing up to certain operation.
    flush_dependency: Arc<Mutex<TopoSort<SegmentId, SeqNumberType>>>,

    /// Holder for a thread, which does flushing of all segments sequentially.
    /// This is used to avoid multiple concurrent flushes.
    pub flush_thread: Mutex<Option<JoinHandle<OperationResult<()>>>>,

    /// The amount of currently running optimizations.
    pub running_optimizations: ProcessCounter,
}

impl Drop for SegmentHolder {
    fn drop(&mut self) {
        if let Err(flushing_err) = self.lock_flushing() {
            log::error!("Failed to flush segments holder during drop: {flushing_err}");
        }
    }
}

impl SegmentHolder {
    /// Iterate over all segments with their IDs
    ///
    /// Appendable first, then non-appendable.
    pub fn iter(&self) -> impl Iterator<Item = (SegmentId, &LockedSegment)> {
        self.appendable_segments
            .iter()
            .chain(self.non_appendable_segments.iter())
            .map(|(id, segment)| (*id, segment))
    }

    /// Iterate over all non-proxy segments with their IDs
    pub fn iter_original(&self) -> impl Iterator<Item = (SegmentId, &Arc<RwLock<Segment>>)> {
        self.iter().filter_map(|(id, segment)| match segment {
            LockedSegment::Original(original) => Some((id, original)),
            LockedSegment::Proxy(_) => None,
        })
    }

    pub fn len(&self) -> usize {
        self.appendable_segments.len() + self.non_appendable_segments.len()
    }

    pub fn is_empty(&self) -> bool {
        self.appendable_segments.is_empty() && self.non_appendable_segments.is_empty()
    }

    fn generate_new_key(&self) -> SegmentId {
        let key: SegmentId = self.id_source.fetch_add(1, Ordering::SeqCst);
        if self.get(key).is_some() {
            debug_assert!(false, "generated new key that already exists");
            self.generate_new_key()
        } else {
            key
        }
    }

    /// Add new segment to storage
    ///
    /// The segment gets assigned a new unique ID.
    pub fn add_new<T>(&mut self, segment: T) -> SegmentId
    where
        T: Into<LockedSegment>,
    {
        let segment_id = self.generate_new_key();
        self.add_existing(segment_id, segment);
        segment_id
    }

    /// Add new segment to storage which is already LockedSegment
    ///
    /// The segment gets assigned a new unique ID.
    pub fn add_new_locked(&mut self, segment: LockedSegment) -> SegmentId {
        let segment_id = self.generate_new_key();
        self.add_existing_locked(segment_id, segment);
        segment_id
    }

    /// Add an existing segment to storage
    ///
    /// The segment gets the provided ID, which must not be in the segment holder yet.
    pub fn add_existing<T>(&mut self, segment_id: SegmentId, segment: T)
    where
        T: Into<LockedSegment>,
    {
        let locked_segment = segment.into();
        self.add_existing_locked(segment_id, locked_segment);
    }

    /// Add an existing segment to storage which is already LockedSegment
    ///
    /// The segment gets the provided ID, which must not be in the segment holder yet.
    pub fn add_existing_locked(&mut self, segment_id: SegmentId, segment: LockedSegment) {
        debug_assert!(
            self.get(segment_id).is_none(),
            "cannot add segment with ID {segment_id}, it already exists",
        );
        if segment.get().read().is_appendable() {
            self.appendable_segments.insert(segment_id, segment);
        } else {
            self.non_appendable_segments.insert(segment_id, segment);
        }
    }

    pub fn remove(&mut self, remove_ids: &[SegmentId]) -> Vec<LockedSegment> {
        let mut removed_segments = vec![];
        for remove_id in remove_ids {
            let removed_segment = self.appendable_segments.remove(remove_id);
            if let Some(segment) = removed_segment {
                removed_segments.push(segment);
            }
            let removed_segment = self.non_appendable_segments.remove(remove_id);
            if let Some(segment) = removed_segment {
                removed_segments.push(segment);
            }
        }
        removed_segments
    }

    /// Replace old segments with a new one
    ///
    /// # Arguments
    ///
    /// * `segment` - segment to insert
    /// * `remove_ids` - ids of segments to replace
    ///
    /// # Result
    ///
    /// Pair of (id of newly inserted segment, Vector of replaced segments)
    ///
    /// The inserted segment gets assigned a new unique ID.
    pub fn swap_new<T>(
        &mut self,
        segment: T,
        remove_ids: &[SegmentId],
    ) -> (SegmentId, Vec<LockedSegment>)
    where
        T: Into<LockedSegment>,
    {
        let new_id = self.add_new(segment);
        (new_id, self.remove(remove_ids))
    }

    /// Replace an existing segment
    ///
    /// # Arguments
    ///
    /// * `segment_id` - segment ID to replace
    /// * `segment` - segment to replace with
    ///
    /// # Result
    ///
    /// Returns the replaced segment. Errors if the segment ID did not exist.
    pub fn replace<T>(
        &mut self,
        segment_id: SegmentId,
        segment: T,
    ) -> OperationResult<LockedSegment>
    where
        T: Into<LockedSegment>,
    {
        // Remove existing segment, check precondition
        let mut removed = self.remove(&[segment_id]);
        if removed.is_empty() {
            return Err(OperationError::service_error(
                "cannot replace segment with ID {segment_id}, it does not exists",
            ));
        }
        debug_assert_eq!(removed.len(), 1);

        self.add_existing(segment_id, segment);

        Ok(removed.pop().unwrap())
    }

    pub fn get(&self, id: SegmentId) -> Option<&LockedSegment> {
        self.appendable_segments
            .get(&id)
            .or_else(|| self.non_appendable_segments.get(&id))
    }

    pub fn has_appendable_segment(&self) -> bool {
        !self.appendable_segments.is_empty()
    }

    /// Get all locked segments, non-appendable first, then appendable.
    pub fn non_appendable_then_appendable_segments(&self) -> impl Iterator<Item = LockedSegment> {
        self.non_appendable_segments
            .values()
            .chain(self.appendable_segments.values())
            .cloned()
    }

    /// Iterates appendable segments only.
    pub fn iter_appendable(&self) -> impl Iterator<Item = LockedSegment> {
        self.appendable_segments.values().cloned()
    }

    /// Get two separate lists for non-appendable and appendable locked segments
    pub fn split_segments(&self) -> (Vec<LockedSegment>, Vec<LockedSegment>) {
        (
            self.non_appendable_segments.values().cloned().collect(),
            self.appendable_segments.values().cloned().collect(),
        )
    }

    /// Return appendable segment IDs sorted by IDs
    pub fn appendable_segments_ids(&self) -> Vec<SegmentId> {
        self.appendable_segments.keys().copied().collect()
    }

    /// Return non-appendable segment IDs sorted by IDs
    pub fn non_appendable_segments_ids(&self) -> Vec<SegmentId> {
        self.non_appendable_segments.keys().copied().collect()
    }

    /// Suggests a new maximum persisted segment version when calling `flush_all`. This can be used to make WAL acknowledge no-op operations,
    /// so we don't replay them on startup. This is especially helpful if the no-op operation is computational expensive and could cause
    /// WAL replay, and thus Qdrant startup, take a significant amount of time.
    pub fn bump_max_segment_version_overwrite(&self, op_num: SeqNumberType) {
        self.max_persisted_segment_version_overwrite
            .fetch_max(op_num, Ordering::Relaxed);
    }

    pub fn segment_ids(&self) -> Vec<SegmentId> {
        self.appendable_segments_ids()
            .into_iter()
            .chain(self.non_appendable_segments_ids())
            .collect()
    }

    /// Get a random appendable segment
    ///
    /// If you want the smallest segment, use `random_appendable_segment_with_capacity` instead.
    pub fn random_appendable_segment(&self) -> Option<LockedSegment> {
        let segment_ids: Vec<_> = self.appendable_segments_ids();
        segment_ids
            .choose(&mut rand::rng())
            .and_then(|idx| self.appendable_segments.get(idx).cloned())
    }

    /// Get the smallest appendable segment
    ///
    /// The returned segment likely has the most capacity for new points, which will help balance
    /// new incoming data over all segments we have.
    ///
    /// This attempts a non-blocking read-lock on all segments to find the smallest one. Segments
    /// that cannot be read-locked at this time are skipped. If no segment can be read-locked at
    /// all, a random one is returned.
    ///
    /// If capacity is not important use `random_appendable_segment` instead because it is cheaper.
    pub fn smallest_appendable_segment(&self) -> Option<LockedSegment> {
        let segment_ids: Vec<_> = self.appendable_segments_ids();

        // Try a non-blocking read lock on all segments and return the smallest one
        let smallest_segment = segment_ids
            .iter()
            .filter_map(|segment_id| self.get(*segment_id))
            .filter_map(|locked_segment| {
                match locked_segment
                    .get()
                    .try_read()
                    .map(|segment| segment.max_available_vectors_size_in_bytes())?
                {
                    Ok(size) => Some((locked_segment, size)),
                    Err(err) => {
                        log::error!("Failed to get segment size, ignoring: {err}");
                        None
                    }
                }
            })
            .min_by_key(|(_, segment_size)| *segment_size);
        if let Some((smallest_segment, _)) = smallest_segment {
            return Some(LockedSegment::clone(smallest_segment));
        }

        // Fall back to picking a random segment
        segment_ids
            .choose(&mut rand::rng())
            .and_then(|idx| self.appendable_segments.get(idx).cloned())
    }

    /// Selects point ids, which is stored in this segment
    fn segment_points(ids: &[PointIdType], segment: &dyn ReadSegmentEntry) -> Vec<PointIdType> {
        ids.iter()
            .cloned()
            .filter(|id| segment.has_point(*id))
            .collect()
    }

    /// Select what point IDs to update and delete in each segment
    ///
    /// Each external point ID might have multiple point versions across all segments.
    ///
    /// This finds all point versions and groups them per segment. The newest point versions are
    /// selected to be updated, all older versions are marked to be deleted.
    ///
    /// Deferred points are never deleted here — the optimizer handles their lifecycle.
    ///
    /// Points that are already soft deleted are not included.
    fn find_points_to_update_and_delete(
        &self,
        ids: &[PointIdType],
    ) -> (
        AHashMap<SegmentId, Vec<PointIdType>>,
        AHashMap<SegmentId, Vec<PointIdType>>,
    ) {
        // Two-pass approach for order-independent correctness.
        //
        // Rules:
        // - Always update the latest version regardless of deferred status
        // - Never delete deferred points — optimizer handles them
        // - Delete older non-deferred copies only if the latest version has a non-deferred copy too
        // - Keep older non-deferred copies when the latest is deferred

        let segment_count = self.len().max(1);
        let default_capacity = ids.len() / segment_count;

        // Pass 1: collect all occurrences of each point across segments
        let mut all_occurrences: AHashMap<PointIdType, PointOccurrences> =
            AHashMap::with_capacity(ids.len());

        for (segment_id, segment) in self.iter() {
            let segment_arc = segment.get();
            let segment_lock = segment_arc.read();
            let segment_points = Self::segment_points(ids, segment_lock.deref());
            for segment_point in segment_points {
                let Some(point_version) = segment_lock.point_version(segment_point) else {
                    continue;
                };
                let is_deferred = segment_lock.point_is_deferred(segment_point);
                all_occurrences.entry(segment_point).or_default().push((
                    segment_id,
                    point_version,
                    is_deferred,
                ));
            }
        }

        // Pass 2: for each point, determine what to update and what to delete
        let mut to_update: AHashMap<SegmentId, Vec<PointIdType>> =
            AHashMap::with_capacity(segment_count);
        let mut to_delete: AHashMap<SegmentId, Vec<PointIdType>> = AHashMap::new();

        for (point_id, occurrences) in all_occurrences {
            let latest_version = occurrences
                .iter()
                .map(|(_, version, _)| *version)
                .max()
                .unwrap();

            let latest_has_non_deferred = occurrences
                .iter()
                .any(|(_, version, is_deferred)| *version == latest_version && !*is_deferred);

            for (segment_id, version, is_deferred) in occurrences {
                if version == latest_version {
                    // Latest version: always update
                    to_update
                        .entry(segment_id)
                        .or_insert_with(|| Vec::with_capacity(default_capacity))
                        .push(point_id);
                } else if !is_deferred && latest_has_non_deferred {
                    // Older non-deferred copy: safe to delete only if the latest
                    // version also has a non-deferred copy
                    to_delete
                        .entry(segment_id)
                        .or_insert_with(|| Vec::with_capacity(default_capacity))
                        .push(point_id);
                }
                // Otherwise: deferred copies are never deleted (optimizer handles them),
                // and older non-deferred copies are kept when the latest is deferred-only
            }
        }

        // Assert each segment does not have overlapping updates and deletes
        debug_assert!(
            to_update
                .iter()
                .filter_map(|(segment_id, updates)| {
                    to_delete.get(segment_id).map(|deletes| (updates, deletes))
                })
                .all(|(updates, deletes)| {
                    let updates: HashSet<&ExtendedPointId> = HashSet::from_iter(updates);
                    let deletes = HashSet::from_iter(deletes);
                    updates.is_disjoint(&deletes)
                }),
            "segments should not have overlapping updates and deletes",
        );

        (to_update, to_delete)
    }

    pub fn for_each_segment<F>(&self, mut f: F) -> OperationResult<usize>
    where
        F: FnMut(&RwLockReadGuard<dyn ReadSegmentEntry + 'static>) -> OperationResult<bool>,
    {
        let mut processed_segments = 0;
        for (_id, segment) in self.iter() {
            let is_applied = f(&segment.get_read().read())?;
            processed_segments += usize::from(is_applied);
        }
        Ok(processed_segments)
    }

    pub fn apply_segments<F>(&self, mut f: F) -> OperationResult<usize>
    where
        F: FnMut(
            &mut RwLockUpgradableReadGuard<dyn SegmentEntry + 'static>,
        ) -> OperationResult<bool>,
    {
        let mut processed_segments = 0;
        for (_id, segment) in self.iter() {
            let is_applied = f(&mut segment.get().upgradable_read())?;
            processed_segments += usize::from(is_applied);
        }
        Ok(processed_segments)
    }

    pub fn apply_segments_batched<F>(&self, mut f: F) -> OperationResult<()>
    where
        F: FnMut(
            &mut RwLockWriteGuard<dyn SegmentEntry + 'static>,
            SegmentId,
        ) -> OperationResult<bool>,
    {
        loop {
            let mut did_apply = false;

            // It is important to iterate over all segments for each batch
            // to avoid blocking of a single segment with sequential updates
            for (segment_id, segment) in self.iter() {
                did_apply |= f(&mut segment.get().write(), segment_id)?;
            }

            // No segment update => we're done
            if !did_apply {
                break;
            }
        }

        Ok(())
    }

    /// Apply an operation `point_operation` to a set of points `ids`.
    ///
    /// This operation additionally checks if there are older versions of the points we are
    /// about to update, and deletes those older versions first.
    /// Older points are obsolete and updating them would bump their version,
    /// which could make the inconsistent with actual latest version of the point.
    ///
    /// In case of delete operations, we must apply them to all versions of a point. Otherwise
    /// future operations may revive deletions through older point versions.
    pub fn apply_points<F>(
        &self,
        ids: &[PointIdType],
        hw_counter: &HardwareCounterCell,
        mut point_operation: F,
    ) -> OperationResult<usize>
    where
        F: FnMut(
            PointIdType,
            SegmentId,
            &mut RwLockWriteGuard<dyn SegmentEntry>,
        ) -> OperationResult<bool>,
    {
        let (to_update, to_delete) = self.find_points_to_update_and_delete(ids);

        // Delete old points first, because we want to handle copy-on-write in multiple proxy segments properly
        self.delete_points_from_segments(to_delete, hw_counter)?;

        // Apply point operations to selected segments
        let mut applied_points = 0;
        for (segment_id, points) in to_update {
            let segment = self.get(segment_id).unwrap();
            let segment_arc = segment.get();
            let mut write_segment = segment_arc.write();

            for point_id in points {
                let is_applied = point_operation(point_id, segment_id, &mut write_segment)?;
                applied_points += usize::from(is_applied);
            }
        }

        Ok(applied_points)
    }

    pub fn delete_points_from_segments(
        &self,
        to_delete: AHashMap<SegmentId, Vec<PointIdType>>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        for (segment_id, points) in to_delete {
            let segment = self.get(segment_id).unwrap();
            let segment_arc = segment.get();
            let mut write_segment = segment_arc.write();

            for point_id in points {
                if let Some(version) = write_segment.point_version(point_id) {
                    write_segment.delete_point(version, point_id, hw_counter)?;
                }
            }
        }
        Ok(())
    }

    /// This operation deduplicates subset of points across all segments.
    /// It scans all segments for presence of the points, detects points with the highest version,
    /// and removes all other versions of the points from all segments.
    pub fn deduplicate_points(
        &self,
        points: &[PointIdType],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let (_to_keep, to_delete) = self.find_points_to_update_and_delete(points);
        self.delete_points_from_segments(to_delete, hw_counter)
    }

    /// Try to acquire read lock over the given segment with increasing wait time.
    /// Should prevent deadlock in case if multiple threads tries to lock segments sequentially.
    fn aloha_lock_segment_read(
        segment: &'_ RwLock<dyn StorageSegmentEntry>,
    ) -> RwLockReadGuard<'_, dyn StorageSegmentEntry> {
        let mut interval = Duration::from_nanos(100);
        loop {
            if let Some(guard) = segment.try_read_for(interval) {
                return guard;
            }

            interval = interval.saturating_mul(2);
            if interval.as_secs() >= 10 {
                log::warn!(
                    "Trying to read-lock a segment is taking a long time. This could be a deadlock and may block new updates.",
                );
            }
        }
    }

    /// Try to acquire write lock over random segment with increasing wait time.
    /// Should prevent deadlock in case if multiple threads tries to lock segments sequentially.
    pub fn aloha_random_write<F>(
        &self,
        segment_ids: &[SegmentId],
        mut apply: F,
    ) -> OperationResult<bool>
    where
        F: FnMut(SegmentId, &mut RwLockWriteGuard<dyn SegmentEntry>) -> OperationResult<bool>,
    {
        if segment_ids.is_empty() {
            return Err(OperationError::service_error(
                "No appendable segments exists, expected at least one",
            ));
        }

        let mut entries: Vec<_> = Vec::with_capacity(segment_ids.len());

        // Try to access each segment first without any timeout (fast)
        for segment_id in segment_ids {
            let segment_opt = self.get(*segment_id).map(|x| x.get());
            match segment_opt {
                None => {}
                Some(segment_lock) => {
                    match segment_lock.try_write() {
                        None => {}
                        Some(mut lock) => return apply(*segment_id, &mut lock),
                    }
                    // save segments for further lock attempts
                    entries.push((*segment_id, segment_lock))
                }
            };
        }

        let mut rng = rand::rng();
        let (segment_id, segment_lock) = entries.choose(&mut rng).unwrap();
        let mut segment_write = segment_lock.write();
        apply(*segment_id, &mut segment_write)
    }

    /// Apply an operation `point_operation` to a set of points `ids`, and, if necessary, move the
    /// points into appendable segments.
    ///
    /// If the segment containing the point is appendable, then point is updated in-place without moving.
    /// If the segment containing the point is immutable, then point is moved to a random appendable segment.
    ///
    /// Returns set of point ids which were successfully (already) applied to segments.
    ///
    /// # Warning
    ///
    /// This function must not be used to apply point deletions, and [`apply_points`] must be used
    /// instead. There are two reasons for this:
    ///
    /// 1. moving a point first and deleting it after is unnecessary overhead.
    /// 2. this leaves older point versions in place, which may accidentally be revived by some
    ///    other operation later.
    pub fn apply_points_with_conditional_move<F, G>(
        &self,
        op_num: SeqNumberType,
        ids: &[PointIdType],
        mut point_operation: F,
        mut point_cow_operation: G,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<AHashSet<PointIdType>>
    where
        F: FnMut(PointIdType, &mut RwLockWriteGuard<dyn SegmentEntry>) -> OperationResult<bool>,
        for<'n, 'o, 'p> G: FnMut(PointIdType, &'n mut NamedVectors<'o>, &'p mut Payload),
    {
        // Choose random appendable segment from this
        let appendable_segments = self.appendable_segments_ids();

        let mut applied_points: AHashSet<PointIdType> = Default::default();

        let _ = self.apply_points(ids, hw_counter, |point_id, idx, write_segment| {
            if let Some(point_version) = write_segment.point_version(point_id)
                && point_version >= op_num
            {
                applied_points.insert(point_id);
                return Ok(false);
            }

            let can_apply_operation = !write_segment.is_proxy() && write_segment.is_appendable();

            let is_applied = if can_apply_operation {
                point_operation(point_id, write_segment)?
            } else {
                self.aloha_random_write(
                    &appendable_segments,
                    |appendable_idx, appendable_write_segment| {
                        // If we are moving point from one segment to another,
                        // we must guarantee, that data in new segment will be persisted before
                        // deleting point from old segment.
                        // Do ensure that, we add a flush dependency
                        self.flush_dependency
                            .lock()
                            .add_dependency(idx, appendable_idx, op_num);

                        let mut all_vectors = write_segment.all_vectors(point_id, hw_counter)?;
                        let mut payload = write_segment.payload(point_id, hw_counter)?;

                        point_cow_operation(point_id, &mut all_vectors, &mut payload);

                        appendable_write_segment.upsert_point(
                            op_num,
                            point_id,
                            all_vectors,
                            hw_counter,
                        )?;
                        appendable_write_segment
                            .set_full_payload(op_num, point_id, &payload, hw_counter)?;

                        // Keep the source of the CoW operation as the deferred point is invisible until indexing.
                        if !appendable_write_segment.point_is_deferred(point_id) {
                            write_segment.delete_point(op_num, point_id, hw_counter)?;
                        }

                        Ok(true)
                    },
                )?
            };
            applied_points.insert(point_id);
            Ok(is_applied)
        })?;
        Ok(applied_points)
    }

    /// Out of a list of point IDs, select only those that exist in at least one segment.
    ///
    /// Optimized for many segments and long lists of IDs:
    /// - Removes found IDs from consideration for subsequent segments
    /// - Early terminates when all IDs are found
    /// - Pre-allocates result set
    pub fn select_existing_points(&self, ids: Vec<PointIdType>) -> AHashSet<PointIdType> {
        let mut remaining_ids = ids;
        if remaining_ids.is_empty() {
            return AHashSet::new();
        }

        let mut existing_points = AHashSet::with_capacity(remaining_ids.len());

        // Iterate through segments in proper order (non-appendable first for consistency)
        for segment in self.non_appendable_then_appendable_segments() {
            let segment_guard = segment.get().read();

            // Partition remaining IDs: found ones go to existing_points, rest stay in remaining
            remaining_ids.retain(|&id| {
                if segment_guard.has_point(id) {
                    existing_points.insert(id);
                    false // Remove from remaining
                } else {
                    true // Keep in remaining
                }
            });

            // Early termination if all points found
            if remaining_ids.is_empty() {
                break;
            }
        }

        existing_points
    }

    /// Create a new appendable segment and add it to the segment holder.
    ///
    /// The segment configuration is sourced from the given collection parameters.
    pub fn create_appendable_segment(
        &mut self,
        segments_path: &Path,
        segment_config: SegmentConfig,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> OperationResult<LockedSegment> {
        let segment = self.build_tmp_segment(
            segments_path,
            Some(segment_config),
            payload_index_schema,
            deferred_internal_id,
            true,
        )?;
        self.add_new_locked(segment.clone());
        Ok(segment)
    }

    /// Build a temporary appendable segment, usually for proxying writes into.
    ///
    /// The segment configuration is sourced from the given collection parameters. If none is
    /// specified this will fall back and clone the configuration of any existing appendable
    /// segment in the segment holder.
    ///
    /// # Errors
    ///
    /// Errors if:
    /// - building the segment fails
    /// - no segment configuration is provided, and no appendable segment is in the segment holder
    ///
    /// # Warning
    ///
    /// This builds a segment on disk, but does NOT add it to the current segment holder. That must
    /// be done explicitly. `save_version` must be true for the segment to be loaded when Qdrant
    /// restarts.
    pub fn build_tmp_segment(
        &self,
        segments_path: &Path,
        segment_config: Option<SegmentConfig>,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
        deferred_internal_id: Option<PointOffsetType>,
        save_version: bool,
    ) -> OperationResult<LockedSegment> {
        let config = match segment_config {
            // Base config on collection params
            Some(config) => config,

            // Fall back: base config on existing appendable segment
            None => self
                .random_appendable_segment()
                .ok_or_else(|| {
                    OperationError::service_error(
                        "No existing segment to source temporary segment configuration from",
                    )
                })?
                .get()
                .read()
                .config()
                .clone(),
        };

        let mut segment =
            build_segment(segments_path, &config, deferred_internal_id, save_version)?;

        // Internal operation.
        let hw_counter = HardwareCounterCell::disposable();

        let payload_schema_lock = payload_index_schema.read();
        for (key, schema) in payload_schema_lock.schema.iter() {
            segment.create_field_index(0, key, Some(schema), &hw_counter)?;
        }

        Ok(LockedSegment::new(segment))
    }

    /// Method tries to remove the segment with the given ID under the following conditions:
    ///
    /// - The segment exists in the holder, if not - it is ignored.
    /// - The segment is a raw segment and not some special proxy segment.
    /// - The segment is empty.
    /// - We are not removing the last appendable segment.
    ///
    /// Returns `true` if the segment was removed, `false` otherwise.
    pub fn remove_segment_if_not_needed(&mut self, segment_id: SegmentId) -> OperationResult<bool> {
        let tmp_segment = {
            let mut segments = self.remove(&[segment_id]);
            if segments.is_empty() {
                // Seems like segment is already removed, ignore
                return Ok(false);
            }
            assert_eq!(segments.len(), 1, "expected exactly one segment");
            segments.pop().unwrap()
        };

        // Append a temp segment to collection if it is not empty or there is no other appendable segment
        if !self.has_appendable_segment()
            || !tmp_segment.get().read().is_empty()
            || !tmp_segment.is_original()
        {
            log::trace!(
                "Keeping temporary segment with {} points",
                tmp_segment.get().read().available_point_count(),
            );
            self.add_existing_locked(segment_id, tmp_segment);
            Ok(false)
        } else {
            log::trace!("Dropping temporary segment with no changes");
            tmp_segment.drop_data()?;
            Ok(true)
        }
    }

    pub fn report_optimizer_error<E: ToString>(&mut self, error: E) {
        // Save only the first error
        // If is more likely to be the real cause of all further problems
        if self.optimizer_errors.is_none() {
            self.optimizer_errors = Some(error.to_string());
        }
    }

    /// Duplicated points can appear in case of interrupted optimization.
    /// LocalShard can still work with duplicated points, but it is better to remove them.
    /// Duplicated points should not affect the search results.
    ///
    /// Checks all segments and removes duplicated and outdated points.
    /// If two points have the same id, the point with the highest version is kept.
    /// If two points have the same id and version, one of them is kept.
    pub fn deduplicate_points_tasks(&self) -> Vec<impl Fn() -> OperationResult<usize> + 'static> {
        let points_to_remove = self.find_duplicated_points();

        points_to_remove
            .into_iter()
            .map(|(segment_id, points)| {
                let locked_segment = self.get(segment_id).unwrap().clone();

                move || {
                    let mut removed_points = 0;
                    let segment_arc = locked_segment.get();
                    let mut write_segment = segment_arc.write();

                    let disposable_hw_counter = HardwareCounterCell::disposable();

                    for &point_id in &points {
                        if let Some(point_version) = write_segment.point_version(point_id) {
                            removed_points += 1;
                            write_segment.delete_point(point_version, point_id, &disposable_hw_counter)?; // Internal operation
                        }
                    }

                    log::trace!("Deleted {removed_points} points from segment {segment_id} to deduplicate: {points:?}");

                    OperationResult::Ok(removed_points)
                }
            })
            .collect::<Vec<_>>()
    }

    pub fn find_duplicated_points(&self) -> AHashMap<SegmentId, Vec<PointIdType>> {
        struct DedupPoint {
            segment_id: SegmentId,
            point_id: PointIdType,
            version: Option<SeqNumberType>,
            is_deferred: bool,
        }

        let segments = self
            .iter()
            .map(|(segment_id, locked_segment)| (segment_id, locked_segment.get()))
            .collect::<Vec<_>>();
        let locked_segments = segments
            .iter()
            .map(|(segment_id, locked_segment)| (*segment_id, locked_segment.read()))
            .collect::<BTreeMap<_, _>>();

        // Iterator produces groups of points by point ID
        let point_group_iter = locked_segments
            .iter()
            .map(|(&segment_id, segment)| {
                segment.iter_points().map(move |point_id| DedupPoint {
                    segment_id,
                    point_id,
                    version: None,
                    is_deferred: false,
                })
            })
            .kmerge_by(|a, b| a.point_id < b.point_id)
            .chunk_by(|entry| entry.point_id);

        let mut points = Vec::new();
        let mut points_to_remove: AHashMap<SegmentId, Vec<PointIdType>> = AHashMap::new();

        for (point_id, group_iter) in &point_group_iter {
            // Fill buffer with points of current chunk, need at least 2 points to deduplicate
            points.clear();
            points.extend(group_iter);
            if points.len() < 2 {
                continue;
            }

            // Enrich with point version and deferred status
            for dedup_point in &mut points {
                let segment = &locked_segments[&dedup_point.segment_id];
                dedup_point.version = segment.point_version(point_id);
                dedup_point.is_deferred = segment.point_is_deferred(point_id);
            }

            // Sort points from highest to lowest version
            // If versions are equal, sort by segment ID to make the order deterministic.
            points.sort_unstable_by_key(|p| (Reverse(p.version), p.segment_id));

            let latest_version = points[0].version;

            // Check if the latest version has a non-deferred copy
            let latest_has_non_deferred = points
                .iter()
                .any(|p| p.version == latest_version && !p.is_deferred);

            // Decide which copies to remove:
            // - Deferred: keep the first, remove duplicates
            // - Non-deferred with latest version: keep the first (the winner)
            // - Older non-deferred: remove only if the latest has a non-deferred copy
            //   (otherwise keep visible until the deferred point is indexed)
            let mut kept_deferred = false;
            let mut kept_non_deferred = false;

            for dedup_point in &points {
                let should_remove = if dedup_point.is_deferred {
                    let duplicate = kept_deferred;
                    kept_deferred = true;
                    duplicate
                } else if dedup_point.version == latest_version && !kept_non_deferred {
                    kept_non_deferred = true;
                    false
                } else {
                    latest_has_non_deferred
                };

                if should_remove {
                    points_to_remove
                        .entry(dedup_point.segment_id)
                        .or_default()
                        .push(dedup_point.point_id);
                }
            }
        }

        points_to_remove
    }
}
