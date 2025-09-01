#[cfg(test)]
mod tests;

use std::cmp::{max, min};
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashSet};
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use ahash::{AHashMap, AHashSet};
use common::counter::hardware_counter::HardwareCounterCell;
use common::iterator_ext::IteratorExt;
use common::save_on_disk::SaveOnDisk;
use common::tar_ext;
use io::storage_version::StorageVersion;
use parking_lot::{RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard};
use rand::seq::IndexedRandom;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::manifest::SnapshotManifest;
use segment::data_types::named_vectors::NamedVectors;
use segment::entry::entry_point::SegmentEntry;
use segment::segment::SegmentVersion;
use segment::segment_constructor::build_segment;
use segment::types::{
    ExtendedPointId, Payload, PointIdType, SegmentConfig, SegmentType, SeqNumberType,
    SnapshotFormat,
};
use smallvec::{SmallVec, smallvec};

use crate::locked_segment::LockedSegment;
use crate::payload_index_schema::PayloadIndexSchema;
use crate::proxy_segment::{LockedIndexChanges, LockedRmSet, ProxySegment};

pub type SegmentId = usize;

/// Internal structure for deduplication of points. Used for BinaryHeap
#[derive(Eq, PartialEq)]
struct DedupPoint {
    point_id: PointIdType,
    segment_id: SegmentId,
}

impl Ord for DedupPoint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.point_id.cmp(&other.point_id).reverse()
    }
}

impl PartialOrd for DedupPoint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

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
}

pub type LockedSegmentHolder = Arc<RwLock<SegmentHolder>>;

impl SegmentHolder {
    pub fn snapshot_manifest(&self) -> OperationResult<SnapshotManifest> {
        let mut manifest = SnapshotManifest::default();

        for (_, segment) in self.iter() {
            segment
                .get()
                .read()
                .collect_snapshot_manifest(&mut manifest)?;
        }

        Ok(manifest)
    }

    /// Iterate over all segments with their IDs
    ///
    /// Appendable first, then non-appendable.
    pub fn iter(&self) -> impl Iterator<Item = (&SegmentId, &LockedSegment)> {
        self.appendable_segments
            .iter()
            .chain(self.non_appendable_segments.iter())
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

    /// Replace old segments with a new one
    ///
    /// # Arguments
    ///
    /// * `segment_id` - segment ID to use
    /// * `segment` - segment to insert
    /// * `remove_ids` - ids of segments to replace
    ///
    /// # Result
    ///
    /// The inserted segment uses the provided segment ID. The segment ID must must not be in the
    /// segment holder yet, or it must be one we also remove.
    pub fn swap_existing<T>(
        &mut self,
        segment_id: SegmentId,
        segment: T,
        remove_ids: &[SegmentId],
    ) -> Vec<LockedSegment>
    where
        T: Into<LockedSegment>,
    {
        let removed = self.remove(remove_ids);
        self.add_existing(segment_id, segment);
        removed
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
    fn segment_points(ids: &[PointIdType], segment: &dyn SegmentEntry) -> Vec<PointIdType> {
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
    /// Points that are already soft deleted are not included.
    fn find_points_to_update_and_delete(
        &self,
        ids: &[PointIdType],
    ) -> (
        AHashMap<SegmentId, Vec<PointIdType>>,
        AHashMap<SegmentId, Vec<PointIdType>>,
    ) {
        let mut to_delete: AHashMap<SegmentId, Vec<PointIdType>> = AHashMap::new();

        // Find in which segments latest point versions are located, mark older points for deletion
        let mut latest_points: AHashMap<PointIdType, (SeqNumberType, SmallVec<[SegmentId; 1]>)> =
            AHashMap::with_capacity(ids.len());
        for (segment_id, segment) in self.iter() {
            let segment_arc = segment.get();
            let segment_lock = segment_arc.read();
            let segment_points = Self::segment_points(ids, segment_lock.deref());
            for segment_point in segment_points {
                let Some(point_version) = segment_lock.point_version(segment_point) else {
                    continue;
                };

                match latest_points.entry(segment_point) {
                    // First time we see the point, add it
                    Entry::Vacant(entry) => {
                        entry.insert((point_version, smallvec![*segment_id]));
                    }
                    // Point we have seen before is older, replace it and mark older for deletion
                    Entry::Occupied(mut entry) if entry.get().0 < point_version => {
                        let (old_version, old_segment_ids) =
                            entry.insert((point_version, smallvec![*segment_id]));

                        // Mark other point for deletion if the version is older
                        // TODO(timvisee): remove this check once deleting old points uses correct version
                        if old_version < point_version {
                            for old_segment_id in old_segment_ids {
                                to_delete
                                    .entry(old_segment_id)
                                    .or_default()
                                    .push(segment_point);
                            }
                        }
                    }
                    // Ignore points with the same version, only update one of them
                    // TODO(timvisee): remove this branch once deleting old points uses correct version
                    Entry::Occupied(mut entry) if entry.get().0 == point_version => {
                        entry.get_mut().1.push(*segment_id);
                    }
                    // Point we have seen before is newer, mark this point for deletion
                    Entry::Occupied(_) => {
                        to_delete
                            .entry(*segment_id)
                            .or_default()
                            .push(segment_point);
                    }
                }
            }
        }

        // Group points to update by segments
        let segment_count = self.len();
        let mut to_update = AHashMap::with_capacity(min(segment_count, latest_points.len()));
        let default_capacity = ids.len() / max(segment_count / 2, 1);
        for (point_id, (_point_version, segment_ids)) in latest_points {
            for segment_id in segment_ids {
                to_update
                    .entry(segment_id)
                    .or_insert_with(|| Vec::with_capacity(default_capacity))
                    .push(point_id);
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
        F: FnMut(&RwLockReadGuard<dyn SegmentEntry + 'static>) -> OperationResult<bool>,
    {
        let mut processed_segments = 0;
        for (_id, segment) in self.iter() {
            let is_applied = f(&segment.get().read())?;
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
                did_apply |= f(&mut segment.get().write(), *segment_id)?;
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
    /// A point may exist in multiple segments, having multiple versions. Depending on the kind of
    /// operation, it either needs to be applied to just the latest point version, or to all of
    /// them. This is controllable by the `all_point_versions` flag.
    /// See: <https://github.com/qdrant/qdrant/pull/5956>
    ///
    /// In case of operations that may do a copy-on-write, we must only apply the operation to the
    /// latest point version. Otherwise our copy on write mechanism may repurpose old point data.
    /// See: <https://github.com/qdrant/qdrant/pull/5528>
    ///
    /// In case of delete operations, we must apply them to all versions of a point. Otherwise
    /// future operations may revive deletions through older point versions.
    ///
    /// The `segment_data` function is called no more than once for each segment and its result is
    /// passed to `point_operation`.
    pub fn apply_points<T, D, O>(
        &self,
        ids: &[PointIdType],
        mut segment_data: D,
        mut point_operation: O,
    ) -> OperationResult<usize>
    where
        D: FnMut(&dyn SegmentEntry) -> T,
        O: FnMut(
            PointIdType,
            SegmentId,
            &mut RwLockWriteGuard<dyn SegmentEntry>,
            &T,
        ) -> OperationResult<bool>,
    {
        let (to_update, to_delete) = self.find_points_to_update_and_delete(ids);

        // Delete old points first, because we want to handle copy-on-write in multiple proxy segments properly
        for (segment_id, points) in to_delete {
            let segment = self.get(segment_id).unwrap();
            let segment_arc = segment.get();
            let mut write_segment = segment_arc.write();

            for point_id in points {
                let version = write_segment.point_version(point_id).unwrap_or_default();
                write_segment.delete_point(
                    version,
                    point_id,
                    &HardwareCounterCell::disposable(), // Internal operation: no need to measure.
                )?;
            }
        }

        // Apply point operations to selected segments
        let mut applied_points = 0;
        for (segment_id, points) in to_update {
            let segment = self.get(segment_id).unwrap();
            let segment_arc = segment.get();
            let mut write_segment = segment_arc.write();
            let segment_data = segment_data(write_segment.deref());

            for point_id in points {
                let is_applied =
                    point_operation(point_id, segment_id, &mut write_segment, &segment_data)?;
                applied_points += usize::from(is_applied);
            }
        }

        Ok(applied_points)
    }

    /// Try to acquire read lock over the given segment with increasing wait time.
    /// Should prevent deadlock in case if multiple threads tries to lock segments sequentially.
    fn aloha_lock_segment_read(
        segment: &'_ RwLock<dyn SegmentEntry>,
    ) -> RwLockReadGuard<'_, dyn SegmentEntry> {
        let mut interval = Duration::from_nanos(100);
        loop {
            if let Some(guard) = segment.try_read_for(interval) {
                return guard;
            }

            interval = interval.saturating_mul(2);
            if interval.as_secs() >= 10 {
                log::warn!(
                    "Trying to read-lock all collection segments is taking a long time. This could be a deadlock and may block new updates.",
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
    /// Moving is not performed in the following cases:
    /// - The segment containing the point is appendable.
    /// - The `update_nonappendable` function returns true for the segment.
    ///
    /// Otherwise, the operation is applied to the containing segment in place.
    ///
    /// Rationale: non-appendable segments may contain immutable indexes that could be left in an
    /// inconsistent state after applying the operation. When it's known that the operation will not
    /// affect the indexes, `update_nonappendable` can return true to avoid moving the points as a
    /// performance optimization.
    ///
    /// It's always safe to pass a closure that always returns false (i.e. `|_| false`).
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
    pub fn apply_points_with_conditional_move<F, G, H>(
        &self,
        op_num: SeqNumberType,
        ids: &[PointIdType],
        mut point_operation: F,
        mut point_cow_operation: H,
        update_nonappendable: G,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<AHashSet<PointIdType>>
    where
        F: FnMut(PointIdType, &mut RwLockWriteGuard<dyn SegmentEntry>) -> OperationResult<bool>,
        for<'n, 'o, 'p> H: FnMut(PointIdType, &'n mut NamedVectors<'o>, &'p mut Payload),
        G: FnMut(&dyn SegmentEntry) -> bool,
    {
        // Choose random appendable segment from this
        let appendable_segments = self.appendable_segments_ids();

        let mut applied_points: AHashSet<PointIdType> = Default::default();

        let _applied_points_count = self.apply_points(
            ids,
            update_nonappendable,
            |point_id, _idx, write_segment, &update_nonappendable| {
                if let Some(point_version) = write_segment.point_version(point_id)
                    && point_version >= op_num
                {
                    applied_points.insert(point_id);
                    return Ok(false);
                }

                let is_applied = if update_nonappendable || write_segment.is_appendable() {
                    point_operation(point_id, write_segment)?
                } else {
                    self.aloha_random_write(
                        &appendable_segments,
                        |_appendable_idx, appendable_write_segment| {
                            let mut all_vectors =
                                write_segment.all_vectors(point_id, hw_counter)?;
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

                            write_segment.delete_point(op_num, point_id, hw_counter)?;

                            Ok(true)
                        },
                    )?
                };
                applied_points.insert(point_id);
                Ok(is_applied)
            },
        )?;
        Ok(applied_points)
    }

    pub fn read_points<F>(
        &self,
        ids: &[PointIdType],
        is_stopped: &AtomicBool,
        mut f: F,
    ) -> OperationResult<usize>
    where
        F: FnMut(PointIdType, &RwLockReadGuard<dyn SegmentEntry>) -> OperationResult<bool>,
    {
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
        let segments = self.non_appendable_then_appendable_segments();

        let mut read_points = 0;
        for segment in segments {
            let segment_arc = segment.get();
            let read_segment = segment_arc.read();
            let points = ids
                .iter()
                .cloned()
                .check_stop(|| is_stopped.load(Ordering::Relaxed))
                .filter(|id| read_segment.has_point(*id));
            for point in points {
                let is_ok = f(point, &read_segment)?;
                read_points += usize::from(is_ok);
            }
        }
        Ok(read_points)
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
        // deleted from A but the new point in B is not persisted. We cannot recover this by
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

        let mut max_persisted_version: SeqNumberType = SeqNumberType::MIN;
        let mut min_unsaved_version: SeqNumberType = SeqNumberType::MAX;
        let mut has_unsaved = false;
        let mut proxy_segments = vec![];

        // Flush and release each segment
        for (read_segment, segment_id) in segment_reads.into_iter().zip(lock_order.into_iter()) {
            let segment_version = read_segment.version();
            let segment_persisted_version = read_segment.flush(sync, force)?;

            log::trace!(
                "Flushed segment {segment_id}:{:?} version: {segment_version} to persisted: {segment_persisted_version}",
                &read_segment.data_path(),
            );

            if segment_version > segment_persisted_version {
                has_unsaved = true;
                min_unsaved_version = min(min_unsaved_version, segment_persisted_version);
            }

            max_persisted_version = max(max_persisted_version, segment_persisted_version);

            // Release segment read-lock right away now or keep it until the end
            match read_segment.segment_type() {
                // Regular segment: release now to allow new writes
                SegmentType::Plain | SegmentType::Indexed => drop(read_segment),
                // Proxy segment: release at the end, proxy segments may share the write segment
                // Prevent new updates from changing write segment on yet-to-be-flushed proxies
                SegmentType::Special => proxy_segments.push(read_segment),
            }
        }

        drop(proxy_segments);

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

    /// Temporarily proxify all segments and apply function `f` to it.
    ///
    /// Intended to smoothly accept writes while performing long-running read operations on each
    /// segment, such as during snapshotting. It should prevent blocking reads on segments for any
    /// significant amount of time.
    ///
    /// This calls function `f` on all segments, but each segment is temporarily proxified while
    /// the function is called.
    ///
    /// All segments are proxified at the same time on start. That ensures each wrapped (proxied)
    /// segment is kept at the same point in time. Each segment is unproxied one by one, right
    /// after function `f` has been applied. That helps keeping proxies as shortlived as possible.
    ///
    /// A read lock is kept during the whole process to prevent external actors from messing with
    /// the segment holder while segments are in proxified state. That means no other actors can
    /// take a write lock while this operation is running.
    ///
    /// As part of this process, a new segment is created. All proxies direct their writes to this
    /// segment. The segment is added to the collection if it has any operations, otherwise it is
    /// deleted when all segments are unproxied again.
    ///
    /// It is recommended to provide collection parameters. The segment configuration will be
    /// sourced from it.
    pub fn proxy_all_segments_and_apply<F>(
        segments: LockedSegmentHolder,
        segments_path: &Path,
        segment_config: Option<SegmentConfig>,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
        mut operation: F,
    ) -> OperationResult<()>
    where
        F: FnMut(Arc<RwLock<dyn SegmentEntry>>) -> OperationResult<()>,
    {
        let segments_lock = segments.upgradable_read();

        // Proxy all segments
        log::trace!("Proxying all shard segments to apply function");
        let (mut proxies, tmp_segment, mut segments_lock) = Self::proxy_all_segments(
            segments_lock,
            segments_path,
            segment_config,
            payload_index_schema,
        )?;

        // Apply provided function
        log::trace!("Applying function on all proxied shard segments");
        let mut result = Ok(());
        let mut unproxied_segment_ids = Vec::with_capacity(proxies.len());
        for (segment_id, proxy_segment) in &proxies {
            // Get segment to snapshot
            let segment = match proxy_segment {
                LockedSegment::Proxy(proxy_segment) => {
                    proxy_segment.read().wrapped_segment.get_cloned()
                }
                // All segments to snapshot should be proxy, warn if this is not the case
                LockedSegment::Original(segment) => {
                    debug_assert!(
                        false,
                        "Reached non-proxy segment while applying function to proxies, this should not happen, ignoring",
                    );
                    segment.clone()
                }
            };

            // Call provided function on segment
            if let Err(err) = operation(segment) {
                result = Err(OperationError::service_error(format!(
                    "Applying function to a proxied shard segment {segment_id} failed: {err}"
                )));
                break;
            }

            // Try to unproxy/release this segment since we don't use it anymore
            // Unproxying now lets us release the segment earlier, prevent unnecessary writes to the temporary segment
            match Self::try_unproxy_segment(segments_lock, *segment_id, proxy_segment.clone()) {
                Ok(lock) => {
                    segments_lock = lock;
                    unproxied_segment_ids.push(*segment_id);
                }
                Err(lock) => segments_lock = lock,
            }
        }
        proxies.retain(|(id, _)| !unproxied_segment_ids.contains(id));

        // Unproxy all segments
        // Always do this to prevent leaving proxy segments behind
        log::trace!("Unproxying all shard segments after function is applied");
        Self::unproxy_all_segments(segments_lock, proxies, tmp_segment)?;

        result
    }

    /// Create a new appendable segment and add it to the segment holder.
    ///
    /// The segment configuration is sourced from the given collection parameters.
    pub fn create_appendable_segment(
        &mut self,
        segments_path: &Path,
        segment_config: SegmentConfig,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
    ) -> OperationResult<LockedSegment> {
        let segment = self.build_tmp_segment(
            segments_path,
            Some(segment_config),
            payload_index_schema,
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
    fn build_tmp_segment(
        &self,
        segments_path: &Path,
        segment_config: Option<SegmentConfig>,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
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

        let mut segment = build_segment(segments_path, &config, save_version)?;

        // Internal operation.
        let hw_counter = HardwareCounterCell::disposable();

        let payload_schema_lock = payload_index_schema.read();
        for (key, schema) in payload_schema_lock.schema.iter() {
            segment.create_field_index(0, key, Some(schema), &hw_counter)?;
        }

        Ok(LockedSegment::new(segment))
    }

    /// Proxy all shard segments for [`Self::proxy_all_segments_and_apply`].
    #[allow(clippy::type_complexity)]
    fn proxy_all_segments<'a>(
        segments_lock: RwLockUpgradableReadGuard<'a, SegmentHolder>,
        segments_path: &Path,
        segment_config: Option<SegmentConfig>,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
    ) -> OperationResult<(
        Vec<(SegmentId, LockedSegment)>,
        LockedSegment,
        RwLockUpgradableReadGuard<'a, SegmentHolder>,
    )> {
        // This counter will be used to measure operations on temp segment,
        // which is part of internal process and can be ignored
        let hw_counter = HardwareCounterCell::disposable();

        // Create temporary appendable segment to direct all proxy writes into
        let tmp_segment = segments_lock.build_tmp_segment(
            segments_path,
            segment_config,
            payload_index_schema,
            false,
        )?;

        // List all segments we want to snapshot
        let segment_ids = segments_lock.segment_ids();

        // Create proxy for all segments
        let mut new_proxies = Vec::with_capacity(segment_ids.len());
        for segment_id in segment_ids {
            let segment = segments_lock.get(segment_id).unwrap();
            let mut proxy = ProxySegment::new(
                segment.clone(),
                tmp_segment.clone(),
                LockedRmSet::default(),
                LockedIndexChanges::default(),
            );

            // Write segment is fresh, so it has no operations
            // Operation with number 0 will be applied
            proxy.replicate_field_indexes(0, &hw_counter)?;
            new_proxies.push((segment_id, proxy));
        }

        // Save segment version once all payload indices have been converted
        // If this ends up not being saved due to a crash, the segment will not be used
        match &tmp_segment {
            LockedSegment::Original(segment) => {
                let segment_path = &segment.read().current_path;
                SegmentVersion::save(segment_path)?;
            }
            LockedSegment::Proxy(_) => unreachable!(),
        }

        // Replace all segments with proxies
        // We cannot fail past this point to prevent only having some segments proxified
        let mut proxies = Vec::with_capacity(new_proxies.len());
        let mut write_segments = RwLockUpgradableReadGuard::upgrade(segments_lock);
        for (segment_id, mut proxy) in new_proxies {
            // Replicate field indexes the second time, because optimized segments could have
            // been changed. The probability is small, though, so we can afford this operation
            // under the full collection write lock
            let op_num = proxy.version();
            if let Err(err) = proxy.replicate_field_indexes(op_num, &hw_counter) {
                log::error!("Failed to replicate proxy segment field indexes, ignoring: {err}");
            }

            // We must keep existing segment IDs because ongoing optimizations might depend on the mapping being the same
            write_segments.replace(segment_id, proxy)?;
            let locked_proxy_segment = write_segments
                .get(segment_id)
                .cloned()
                .expect("failed to get segment from segment holder we just swapped in");
            proxies.push((segment_id, locked_proxy_segment));
        }
        let segments_lock = RwLockWriteGuard::downgrade_to_upgradable(write_segments);

        Ok((proxies, tmp_segment, segments_lock))
    }

    /// Try to unproxy a single shard segment for [`Self::proxy_all_segments_and_apply`].
    ///
    /// # Warning
    ///
    /// If unproxying fails an error is returned with the lock and the proxy is left behind in the
    /// shard holder.
    fn try_unproxy_segment(
        segments_lock: RwLockUpgradableReadGuard<SegmentHolder>,
        segment_id: SegmentId,
        proxy_segment: LockedSegment,
    ) -> Result<RwLockUpgradableReadGuard<SegmentHolder>, RwLockUpgradableReadGuard<SegmentHolder>>
    {
        // We must propagate all changes in the proxy into their wrapped segments, as we'll put the
        // wrapped segment back into the segment holder. This can be an expensive step if we
        // collected a lot of changes in the proxy, so we do this in two batches to prevent
        // unnecessary locking. First we propagate all changes with a read lock on the shard
        // holder, to prevent blocking other readers. Second we propagate any new changes again
        // with a write lock on the segment holder, blocking other operations. This second batch
        // should be very fast, as we already propagated all changes in the first, which is why we
        // can hold a write lock. Once done, we can swap out the proxy for the wrapped shard.

        let proxy_segment = match proxy_segment {
            LockedSegment::Proxy(proxy_segment) => proxy_segment,
            LockedSegment::Original(_) => {
                log::warn!(
                    "Unproxying segment {segment_id} that is not proxified, that is unexpected, skipping",
                );
                return Err(segments_lock);
            }
        };

        // Batch 1: propagate changes to wrapped segment with segment holder read lock
        if let Err(err) = proxy_segment.read().propagate_to_wrapped() {
            log::error!(
                "Propagating proxy segment {segment_id} changes to wrapped segment failed, ignoring: {err}",
            );
        }

        let mut write_segments = RwLockUpgradableReadGuard::upgrade(segments_lock);

        // Batch 2: propagate changes to wrapped segment with segment holder write lock
        // Propagate proxied changes to wrapped segment, take it out and swap with proxy
        // Important: put the wrapped segment back with its original segment ID
        let wrapped_segment = {
            let proxy_segment = proxy_segment.read();
            if let Err(err) = proxy_segment.propagate_to_wrapped() {
                log::error!(
                    "Propagating proxy segment {segment_id} changes to wrapped segment failed, ignoring: {err}",
                );
            }
            proxy_segment.wrapped_segment.clone()
        };
        write_segments.replace(segment_id, wrapped_segment).unwrap();

        // Downgrade write lock to read and give it back
        Ok(RwLockWriteGuard::downgrade_to_upgradable(write_segments))
    }

    /// Unproxy all shard segments for [`Self::proxy_all_segments_and_apply`].
    fn unproxy_all_segments(
        segments_lock: RwLockUpgradableReadGuard<SegmentHolder>,
        proxies: Vec<(SegmentId, LockedSegment)>,
        tmp_segment: LockedSegment,
    ) -> OperationResult<()> {
        // We must propagate all changes in the proxy into their wrapped segments, as we'll put the
        // wrapped segment back into the segment holder. This can be an expensive step if we
        // collected a lot of changes in the proxy, so we do this in two batches to prevent
        // unnecessary locking. First we propagate all changes with a read lock on the shard
        // holder, to prevent blocking other readers. Second we propagate any new changes again
        // with a write lock on the segment holder, blocking other operations. This second batch
        // should be very fast, as we already propagated all changes in the first, which is why we
        // can hold a write lock. Once done, we can swap out the proxy for the wrapped shard.

        // Batch 1: propagate changes to wrapped segment with segment holder read lock
        proxies
            .iter()
            .filter_map(|(segment_id, proxy_segment)| match proxy_segment {
                LockedSegment::Proxy(proxy_segment) => Some((segment_id, proxy_segment)),
                LockedSegment::Original(_) => None,
            }).for_each(|(proxy_id, proxy_segment)| {
                if let Err(err) = proxy_segment.read().propagate_to_wrapped() {
                    log::error!("Propagating proxy segment {proxy_id} changes to wrapped segment failed, ignoring: {err}");
                }
            });

        // Batch 2: propagate changes to wrapped segment with segment holder write lock
        // Swap out each proxy with wrapped segment once changes are propagated
        let mut write_segments = RwLockUpgradableReadGuard::upgrade(segments_lock);
        for (segment_id, proxy_segment) in proxies {
            match proxy_segment {
                // Propagate proxied changes to wrapped segment, take it out and swap with proxy
                // Important: put the wrapped segment back with its original segment ID
                LockedSegment::Proxy(proxy_segment) => {
                    let wrapped_segment = {
                        let proxy_segment = proxy_segment.read();
                        if let Err(err) = proxy_segment.propagate_to_wrapped() {
                            log::error!(
                                "Propagating proxy segment {segment_id} changes to wrapped segment failed, ignoring: {err}",
                            );
                        }
                        proxy_segment.wrapped_segment.clone()
                    };
                    write_segments.replace(segment_id, wrapped_segment)?;
                }
                // If already unproxied, do nothing
                LockedSegment::Original(_) => {}
            }
        }

        // Finalize temporary segment we proxied writes to
        // Append a temp segment to collection if it is not empty or there is no other appendable segment
        if !write_segments.has_appendable_segment() || !tmp_segment.get().read().is_empty() {
            log::trace!(
                "Keeping temporary segment with {} points",
                tmp_segment.get().read().available_point_count(),
            );
            write_segments.add_new_locked(tmp_segment);
        } else {
            log::trace!("Dropping temporary segment with no changes");
            tmp_segment.drop_data()?;
        }

        Ok(())
    }

    /// Take a snapshot of all segments into `snapshot_dir_path`
    ///
    /// It is recommended to provide collection parameters. This function internally creates a
    /// temporary segment, which will source the configuration from it.
    ///
    /// Shortcuts at the first failing segment snapshot.
    #[expect(clippy::too_many_arguments)]
    pub fn snapshot_all_segments(
        segments: LockedSegmentHolder,
        segments_path: &Path,
        segment_config: Option<SegmentConfig>,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
        temp_dir: &Path,
        tar: &tar_ext::BuilderExt,
        format: SnapshotFormat,
        manifest: Option<&SnapshotManifest>,
    ) -> OperationResult<()> {
        // Snapshotting may take long-running read locks on segments blocking incoming writes, do
        // this through proxied segments to allow writes to continue.

        let mut snapshotted_segments = HashSet::<String>::new();

        Self::proxy_all_segments_and_apply(
            segments,
            segments_path,
            segment_config,
            payload_index_schema,
            |segment| {
                let read_segment = segment.read();
                read_segment.take_snapshot(
                    temp_dir,
                    tar,
                    format,
                    manifest,
                    &mut snapshotted_segments,
                )?;
                Ok(())
            },
        )
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

    fn find_duplicated_points(&self) -> AHashMap<SegmentId, Vec<PointIdType>> {
        let segments = self
            .iter()
            .map(|(&segment_id, locked_segment)| (segment_id, locked_segment.get()))
            .collect::<Vec<_>>();
        let locked_segments = segments
            .iter()
            .map(|(segment_id, locked_segment)| (*segment_id, locked_segment.read()))
            .collect::<BTreeMap<_, _>>();
        let mut iterators = locked_segments
            .iter()
            .map(|(segment_id, locked_segment)| (*segment_id, locked_segment.iter_points()))
            .collect::<BTreeMap<_, _>>();

        // heap contains the current iterable point id from each segment
        let mut heap = iterators
            .iter_mut()
            .filter_map(|(&segment_id, iter)| {
                iter.next().map(|point_id| DedupPoint {
                    point_id,
                    segment_id,
                })
            })
            .collect::<BinaryHeap<_>>();

        let mut last_point_id_opt = None;
        let mut last_segment_id_opt = None;
        let mut last_point_version_opt = None;
        let mut points_to_remove: AHashMap<SegmentId, Vec<PointIdType>> = Default::default();

        while let Some(entry) = heap.pop() {
            let point_id = entry.point_id;
            let segment_id = entry.segment_id;
            if let Some(next_point_id) = iterators.get_mut(&segment_id).and_then(|i| i.next()) {
                heap.push(DedupPoint {
                    segment_id,
                    point_id: next_point_id,
                });
            }

            if last_point_id_opt == Some(point_id) {
                let last_segment_id = last_segment_id_opt.unwrap();

                let point_version = locked_segments[&segment_id].point_version(point_id);
                let last_point_version = if let Some(last_point_version) = last_point_version_opt {
                    last_point_version
                } else {
                    let version = locked_segments[&last_segment_id].point_version(point_id);
                    last_point_version_opt = Some(version);
                    version
                };

                // choose newer version between point_id and last_point_id
                if point_version < last_point_version {
                    log::trace!(
                        "Selected point {point_id} in segment {segment_id} for deduplication (version {point_version:?} versus {last_point_version:?} in segment {last_segment_id})",
                    );

                    points_to_remove
                        .entry(segment_id)
                        .or_default()
                        .push(point_id);
                } else {
                    log::trace!(
                        "Selected point {point_id} in segment {last_segment_id} for deduplication (version {last_point_version:?} versus {point_version:?} in segment {segment_id})",
                    );

                    points_to_remove
                        .entry(last_segment_id)
                        .or_default()
                        .push(point_id);

                    last_point_id_opt = Some(point_id);
                    last_segment_id_opt = Some(segment_id);
                    last_point_version_opt = Some(point_version);
                }
            } else {
                last_point_id_opt = Some(point_id);
                last_segment_id_opt = Some(segment_id);
                last_point_version_opt = None;
            }
        }

        points_to_remove
    }
}
