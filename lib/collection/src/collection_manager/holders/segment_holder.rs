use std::cmp::{max, min};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use itertools::Itertools;
use parking_lot::{RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard};
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use segment::common::operation_error::{OperationError, OperationResult};
use segment::common::version::StorageVersion;
use segment::entry::entry_point::SegmentEntry;
use segment::segment::{Segment, SegmentVersion};
use segment::segment_constructor::build_segment;
use segment::types::{PayloadStorageType, PointIdType, SegmentConfig, SeqNumberType};

use crate::collection_manager::holders::proxy_segment::ProxySegment;
use crate::config::CollectionParams;
use crate::operations::types::CollectionError;
use crate::shards::update_tracker::UpdateTracker;

pub type SegmentId = usize;

const DROP_SPIN_TIMEOUT: Duration = Duration::from_millis(10);
const DROP_DATA_TIMEOUT: Duration = Duration::from_secs(60 * 60);

/// Object, which unifies the access to different types of segments, but still allows to
/// access the original type of the segment if it is required for more efficient operations.
pub enum LockedSegment {
    Original(Arc<RwLock<Segment>>),
    Proxy(Arc<RwLock<ProxySegment>>),
}

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

fn try_unwrap_with_timeout<T>(
    mut arc: Arc<T>,
    spin: Duration,
    timeout: Duration,
) -> Result<T, Arc<T>> {
    if timeout.is_zero() {
        return Err(arc);
    }

    loop {
        match Arc::try_unwrap(arc) {
            inner @ Ok(_) => return inner,
            Err(inner) => {
                arc = inner;
                sleep(spin);
            }
        }
    }
}

impl LockedSegment {
    pub fn new<T>(segment: T) -> Self
    where
        T: Into<LockedSegment>,
    {
        segment.into()
    }

    pub fn get(&self) -> Arc<RwLock<dyn SegmentEntry>> {
        match self {
            LockedSegment::Original(segment) => segment.clone(),
            LockedSegment::Proxy(proxy) => proxy.clone(),
        }
    }

    /// Consume the LockedSegment and drop the underlying segment data.
    /// Operation fails if the segment is used by other thread for longer than `timeout`.
    pub fn drop_data(self) -> OperationResult<()> {
        match self {
            LockedSegment::Original(segment) => {
                match try_unwrap_with_timeout(segment, DROP_SPIN_TIMEOUT, DROP_DATA_TIMEOUT) {
                    Ok(raw_locked_segment) => raw_locked_segment.into_inner().drop_data(),
                    Err(locked_segment) => Err(OperationError::service_error(format!(
                        "Removing segment which is still in use: {:?}",
                        locked_segment.read().data_path()
                    ))),
                }
            }
            LockedSegment::Proxy(proxy) => {
                match try_unwrap_with_timeout(proxy, DROP_SPIN_TIMEOUT, DROP_DATA_TIMEOUT) {
                    Ok(raw_locked_segment) => raw_locked_segment.into_inner().drop_data(),
                    Err(locked_segment) => Err(OperationError::service_error(format!(
                        "Removing proxy segment which is still in use: {:?}",
                        locked_segment.read().data_path()
                    ))),
                }
            }
        }
    }
}

impl Clone for LockedSegment {
    fn clone(&self) -> Self {
        match self {
            LockedSegment::Original(x) => LockedSegment::Original(x.clone()),
            LockedSegment::Proxy(x) => LockedSegment::Proxy(x.clone()),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        *self = source.clone();
    }
}

impl From<Segment> for LockedSegment {
    fn from(s: Segment) -> Self {
        LockedSegment::Original(Arc::new(RwLock::new(s)))
    }
}

impl From<ProxySegment> for LockedSegment {
    fn from(s: ProxySegment) -> Self {
        LockedSegment::Proxy(Arc::new(RwLock::new(s)))
    }
}

#[derive(Default)]
pub struct SegmentHolder {
    segments: HashMap<SegmentId, LockedSegment>,

    update_tracker: UpdateTracker,

    /// Seq number of the first un-recovered operation.
    /// If there are no failed operation - None
    pub failed_operation: BTreeSet<SeqNumberType>,

    /// Holds the first uncorrected error happened with optimizer
    pub optimizer_errors: Option<CollectionError>,
}

pub type LockedSegmentHolder = Arc<RwLock<SegmentHolder>>;

impl<'s> SegmentHolder {
    pub fn iter(&'s self) -> impl Iterator<Item = (&SegmentId, &LockedSegment)> + 's {
        self.segments.iter()
    }

    pub fn len(&self) -> usize {
        self.segments.len()
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn update_tracker(&self) -> UpdateTracker {
        self.update_tracker.clone()
    }

    fn generate_new_key(&self) -> SegmentId {
        let key = thread_rng().gen::<SegmentId>();
        if self.segments.contains_key(&key) {
            self.generate_new_key()
        } else {
            key
        }
    }

    /// Add new segment to storage
    pub fn add<T>(&mut self, segment: T) -> SegmentId
    where
        T: Into<LockedSegment>,
    {
        let key = self.generate_new_key();
        self.segments.insert(key, segment.into());
        key
    }

    /// Add new segment to storage which is already LockedSegment
    pub fn add_locked(&mut self, segment: LockedSegment) -> SegmentId {
        let key = self.generate_new_key();
        self.segments.insert(key, segment);
        key
    }

    pub fn remove(&mut self, remove_ids: &[SegmentId]) -> Vec<LockedSegment> {
        let mut removed_segments = vec![];
        for remove_id in remove_ids {
            let removed_segment = self.segments.remove(remove_id);
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
    pub fn swap<T>(
        &mut self,
        segment: T,
        remove_ids: &[SegmentId],
    ) -> (SegmentId, Vec<LockedSegment>)
    where
        T: Into<LockedSegment>,
    {
        let new_id = self.add(segment);
        (new_id, self.remove(remove_ids))
    }

    pub fn get(&self, id: SegmentId) -> Option<&LockedSegment> {
        self.segments.get(&id)
    }

    pub fn appendable_segments(&self) -> Vec<SegmentId> {
        self.segments
            .iter()
            .filter(|(_idx, seg)| seg.get().read().is_appendable())
            .map(|(idx, _seg)| *idx)
            .collect()
    }

    pub fn non_appendable_segments(&self) -> Vec<SegmentId> {
        self.segments
            .iter()
            .filter(|(_idx, seg)| !seg.get().read().is_appendable())
            .map(|(idx, _seg)| *idx)
            .collect()
    }

    pub fn random_appendable_segment(&self) -> Option<LockedSegment> {
        let segment_ids: Vec<_> = self.appendable_segments();
        segment_ids
            .choose(&mut rand::thread_rng())
            .and_then(|idx| self.segments.get(idx).cloned())
    }

    /// Selects point ids, which is stored in this segment
    fn segment_points(&self, ids: &[PointIdType], segment: &dyn SegmentEntry) -> Vec<PointIdType> {
        ids.iter()
            .cloned()
            .filter(|id| segment.has_point(*id))
            .collect()
    }

    pub fn for_each_segment<F>(&self, mut f: F) -> OperationResult<usize>
    where
        F: FnMut(&RwLockReadGuard<dyn SegmentEntry + 'static>) -> OperationResult<bool>,
    {
        let mut processed_segments = 0;
        for segment in self.segments.values() {
            let is_applied = f(&segment.get().read())?;
            processed_segments += is_applied as usize;
        }
        Ok(processed_segments)
    }

    pub fn apply_segments<F>(&self, mut f: F) -> OperationResult<usize>
    where
        F: FnMut(&mut RwLockWriteGuard<dyn SegmentEntry + 'static>) -> OperationResult<bool>,
    {
        let _update_guard = self.update_tracker.update();

        let mut processed_segments = 0;
        for segment in self.segments.values() {
            let is_applied = f(&mut segment.get().write())?;
            processed_segments += is_applied as usize;
        }
        Ok(processed_segments)
    }

    /// Apply an operation `point_operation` to a set of points `ids`.
    /// The `segment_data` function is called no more than once for each segment and its result is
    /// passed to `point_operation`.
    pub fn apply_points<T, O, D>(
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
        let _update_guard = self.update_tracker.update();

        let mut applied_points = 0;
        for (idx, segment) in &self.segments {
            // Collect affected points first, we want to lock segment for writing as rare as possible
            let segment_arc = segment.get();
            let segment_lock = segment_arc.upgradable_read();
            let segment_points = self.segment_points(ids, segment_lock.deref());
            if !segment_points.is_empty() {
                let mut write_segment = RwLockUpgradableReadGuard::upgrade(segment_lock);
                let segment_data = segment_data(write_segment.deref());
                for point_id in segment_points {
                    let is_applied =
                        point_operation(point_id, *idx, &mut write_segment, &segment_data)?;
                    applied_points += is_applied as usize;
                }
            }
        }
        Ok(applied_points)
    }

    /// Try to acquire read lock over the given segment with increasing wait time.
    /// Should prevent deadlock in case if multiple threads tries to lock segments sequentially.
    fn aloha_lock_segment_read<'a>(
        &'a self,
        segment: &'a Arc<RwLock<dyn SegmentEntry>>,
    ) -> RwLockReadGuard<dyn SegmentEntry> {
        let mut interval = Duration::from_nanos(100);
        loop {
            if let Some(guard) = segment.try_read_for(interval) {
                return guard;
            }

            interval = interval.saturating_mul(2);
            if interval.as_secs() >= 10 {
                log::warn!("Trying to read-lock all collection segments is taking a long time. This could be a deadlock and may block new updates.");
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

        let mut entries: Vec<_> = Default::default();

        // Try to access each segment first without any timeout (fast)
        for segment_id in segment_ids {
            let segment_opt = self.segments.get(segment_id).map(|x| x.get());
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

        let mut rng = rand::thread_rng();
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
    pub fn apply_points_with_conditional_move<F, G>(
        &self,
        op_num: SeqNumberType,
        ids: &[PointIdType],
        mut point_operation: F,
        update_nonappendable: G,
    ) -> OperationResult<HashSet<PointIdType>>
    where
        F: FnMut(PointIdType, &mut RwLockWriteGuard<dyn SegmentEntry>) -> OperationResult<bool>,
        G: FnMut(&dyn SegmentEntry) -> bool,
    {
        let _update_guard = self.update_tracker.update();

        // Choose random appendable segment from this
        let appendable_segments = self.appendable_segments();

        let mut applied_points: HashSet<PointIdType> = Default::default();

        let _applied_points_count = self.apply_points(
            ids,
            update_nonappendable,
            |point_id, _idx, write_segment, &update_nonappendable| {
                if let Some(point_version) = write_segment.point_version(point_id) {
                    if point_version >= op_num {
                        applied_points.insert(point_id);
                        return Ok(false);
                    }
                }

                let is_applied = if update_nonappendable || write_segment.is_appendable() {
                    point_operation(point_id, write_segment)?
                } else {
                    self.aloha_random_write(
                        &appendable_segments,
                        |_appendable_idx, appendable_write_segment| {
                            let all_vectors = write_segment.all_vectors(point_id)?;
                            let payload = write_segment.payload(point_id)?;

                            appendable_write_segment.upsert_point(op_num, point_id, all_vectors)?;
                            appendable_write_segment
                                .set_full_payload(op_num, point_id, &payload)?;

                            write_segment.delete_point(op_num, point_id)?;

                            point_operation(point_id, appendable_write_segment)
                        },
                    )?
                };
                applied_points.insert(point_id);
                Ok(is_applied)
            },
        )?;
        Ok(applied_points)
    }

    pub fn read_points<F>(&self, ids: &[PointIdType], mut f: F) -> OperationResult<usize>
    where
        F: FnMut(PointIdType, &RwLockReadGuard<dyn SegmentEntry>) -> OperationResult<bool>,
    {
        let mut read_points = 0;
        for segment in self.segments.values() {
            let segment_arc = segment.get();
            let read_segment = segment_arc.read();
            for point in ids.iter().cloned().filter(|id| read_segment.has_point(*id)) {
                let is_ok = f(point, &read_segment)?;
                read_points += is_ok as usize;
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
        let appendable_segments = self.appendable_segments();
        let non_appendable_segments = self.non_appendable_segments();

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
    pub fn flush_all(&self, sync: bool) -> OperationResult<SeqNumberType> {
        // Grab and keep to segment RwLock's until the end of this function
        let segments = self.segment_locks(self.segment_flush_ordering())?;

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
            .map(|segment| self.aloha_lock_segment_read(segment))
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

        // Flush and release each segment
        for read_segment in segment_reads {
            let segment_version = read_segment.version();
            let segment_persisted_version = read_segment.flush(sync)?;

            if segment_version > segment_persisted_version {
                has_unsaved = true;
                min_unsaved_version = min(min_unsaved_version, segment_persisted_version);
            }

            max_persisted_version = max(max_persisted_version, segment_persisted_version);

            // Release read-lock immediately after flush, explicit to make this more clear
            drop(read_segment);
        }

        if has_unsaved {
            Ok(min_unsaved_version)
        } else {
            Ok(max_persisted_version)
        }
    }

    /// Grab the RwLock's for all the given segment IDs.
    fn segment_locks(
        &self,
        segment_ids: impl IntoIterator<Item = SegmentId>,
    ) -> OperationResult<Vec<Arc<RwLock<dyn SegmentEntry>>>> {
        segment_ids
            .into_iter()
            .map(|segment_id| {
                self.segments
                    .get(&segment_id)
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
    /// the function is called. All segments are also unproxied again when the given function
    /// returns.
    ///
    /// A read lock is kept during the whole process to prevent external actors from messing with
    /// the segment holder while segments are in proxified state.
    ///
    /// As part of this process, a new segment is created. All proxies direct their writes to this
    /// segment. The segment is added to the collection if it has any operations, otherwise it is
    /// deleted when all segments are unproxied again.
    ///
    /// It is recommended to provide collection parameters. The segment configuration will be
    /// sourced from it.
    pub fn proxy_all_segments_and_apply<F>(
        segments: LockedSegmentHolder,
        collection_path: &Path,
        collection_params: Option<&CollectionParams>,
        f: F,
    ) -> OperationResult<()>
    where
        F: Fn(Arc<RwLock<dyn SegmentEntry>>) -> OperationResult<()>,
    {
        let segments_lock = segments.upgradable_read();

        // Proxy all segments
        log::trace!("Proxying all shard segments to apply function");
        let (proxies, tmp_segment, segments_lock) =
            Self::proxy_all_segments(segments_lock, collection_path, collection_params)?;

        // Apply provided function
        log::trace!("Applying function on all proxied shard segments");
        let mut result = Ok(());
        for (proxy_id, proxy_segment) in &proxies {
            // Get segment to snapshot
            let segment = match proxy_segment {
                LockedSegment::Proxy(proxy_segment) => {
                    proxy_segment.read().wrapped_segment.clone().get()
                }
                // All segments to snapshot should be proxy, warn if this is not the case
                LockedSegment::Original(segment) => {
                    log::warn!("Reached non-proxy segment while applying function to proxies, this should not happen, ignoring");
                    segment.clone()
                }
            };

            // Call provided function on segment
            if let Err(err) = f(segment) {
                result = Err(OperationError::service_error(format!(
                    "Applying function to a proxied shard segment {proxy_id} failed: {err}"
                )));
                break;
            }
        }

        // Unproxy all segments
        // Always do this to prevent leaving proxy segments behind
        log::trace!("Unproxying all shard segments after function is applied");
        Self::unproxy_all_segments(segments_lock, proxies, tmp_segment)?;

        result
    }

    /// Proxy all shard segments for [`proxy_all_segments_and_apply`]
    #[allow(clippy::type_complexity)]
    fn proxy_all_segments<'a>(
        segments_lock: RwLockUpgradableReadGuard<'a, SegmentHolder>,
        collection_path: &Path,
        collection_params: Option<&CollectionParams>,
    ) -> OperationResult<(
        Vec<(SegmentId, LockedSegment)>,
        LockedSegment,
        RwLockUpgradableReadGuard<'a, SegmentHolder>,
    )> {
        // Source config for temporary segment which we target all writes to
        let config = match collection_params {
            // Base config on collection params
            Some(collection_params) => SegmentConfig {
                vector_data: collection_params
                    .into_base_vector_data()
                    .map_err(|err| OperationError::service_error(format!("Failed to source dense vector configuration from collection parameters: {err:?}")))?,
                sparse_vector_data: collection_params
                    .into_sparse_vector_data()
                    .map_err(|err| OperationError::service_error(format!("Failed to source sparse vector configuration from collection parameters: {err:?}")))?,
                payload_storage_type: if collection_params.on_disk_payload {
                    PayloadStorageType::OnDisk
                } else {
                    PayloadStorageType::InMemory
                },
            },
            // Base config on existing appendable or non-appendable segment
            None => {
                segments_lock
                    .random_appendable_segment()
                    .ok_or_else(|| OperationError::service_error("No existing segment to source temporary segment configuration from"))?
                    .get()
                    .read()
                    .config()
                    .clone()
            }
        };

        // Create temporary appendable segment to direct all proxy writes into
        let tmp_segment = LockedSegment::new(build_segment(collection_path, &config, false)?);

        // List all segments we want to snapshot
        let segment_ids = segments_lock
            .segments
            .keys()
            .copied()
            .collect::<Vec<SegmentId>>();

        // Create proxy for all segments
        let mut new_proxies = Vec::with_capacity(segment_ids.len());
        for segment_id in segment_ids {
            let segment = segments_lock.segments.get(&segment_id).unwrap();
            let mut proxy = ProxySegment::new(
                segment.clone(),
                tmp_segment.clone(),
                Default::default(),
                Default::default(),
                Default::default(),
            );

            // Write segment is fresh, so it has no operations
            // Operation with number 0 will be applied
            proxy.replicate_field_indexes(0)?;
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
            // Replicate field indexes a the second time, because optimized segments could have
            // been changed. The probability is small, though, so we can afford this operation
            // under the full collection write lock
            let op_num = 0;
            if let Err(err) = proxy.replicate_field_indexes(op_num) {
                log::error!("Failed to replicate proxy segment field indexes, ignoring: {err}");
            }

            let (segment_id, segments) = write_segments.swap(proxy, &[segment_id]);
            debug_assert_eq!(segments.len(), 1);
            // TODO: do not unwrap here?
            let locked_proxy_segment = write_segments.get(segment_id).cloned().unwrap();
            proxies.push((segment_id, locked_proxy_segment));
        }
        let segments_lock = RwLockWriteGuard::downgrade_to_upgradable(write_segments);

        Ok((proxies, tmp_segment, segments_lock))
    }

    /// Unproxy all shard segments for [`proxy_all_segments_and_apply`]
    fn unproxy_all_segments(
        segments_lock: RwLockUpgradableReadGuard<SegmentHolder>,
        proxies: Vec<(SegmentId, LockedSegment)>,
        tmp_segment: LockedSegment,
    ) -> OperationResult<()> {
        // We must propagate all changes in the proxy into their wrapped segments, as we'll put the
        // wrapped segments back into the segment holder. This can be an expensive step if we
        // collected a lot of changes in the proxy, so we do this in two batches. First we
        // propagate all changes with a read lock on the shard holder, to prevent blocking other
        // readers. Second we propagate any new changes again with a write lock on the segment
        // holder, blocking other operations. This second batch should be very fast, as we already
        // propagated all changes in the first, which is why we can hold a write lock. Once done,
        // we can swap out the proxy for the wrapped shard.

        // Batch 1: propagate changes to wrapped segment with segment holder read lock
        proxies
            .iter()
            .flat_map(|(proxy_id, proxy_segment)| match proxy_segment {
                LockedSegment::Proxy(proxy_segment) => Some((proxy_id, proxy_segment)),
                LockedSegment::Original(_) => None,
            }).for_each(|(proxy_id, proxy_segment)| {
                if let Err(err) = proxy_segment.read().propagate_to_wrapped() {
                    log::error!("Propagating proxy segment {proxy_id} changes to wrapped segment failed, ignoring: {err}");
                }
            });

        // Batch 2: propagate changes to wrapped segment with segment holder write lock
        // Swap out each proxy with wrapped segment once changes are propagated
        let mut write_segments = RwLockUpgradableReadGuard::upgrade(segments_lock);
        for (proxy_id, proxy_segment) in proxies {
            match proxy_segment {
                // Propagate proxied changes to wrapped segment, take it out and swap with proxy
                LockedSegment::Proxy(proxy_segment) => {
                    let wrapped_segment = {
                        let proxy_segment = proxy_segment.read();
                        if let Err(err) = proxy_segment.propagate_to_wrapped() {
                            log::error!("Propagating proxy segment {proxy_id} changes to wrapped segment failed, ignoring: {err}");
                        }
                        proxy_segment.wrapped_segment.clone()
                    };
                    let (_, segments) = write_segments.swap(wrapped_segment, &[proxy_id]);
                    debug_assert_eq!(segments.len(), 1);
                }
                // If already unproxied, do nothing
                LockedSegment::Original(_) => {}
            }
        }

        // Finalize temporary segment we proxied writes to
        // Append a temp segment to collection if it is not empty or there is no other appendable segment
        let has_appendable_segments = write_segments.random_appendable_segment().is_some();
        let available_points = tmp_segment.get().read().available_point_count();
        if available_points > 0 || !has_appendable_segments {
            log::trace!("Keeping temporary segment with {available_points} points");
            write_segments.add_locked(tmp_segment);
            drop(write_segments);
        } else {
            log::trace!("Dropping temporary segment with no changes");
            drop(write_segments);
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
    pub fn snapshot_all_segments(
        segments: LockedSegmentHolder,
        collection_path: &Path,
        collection_params: Option<&CollectionParams>,
        temp_dir: &Path,
        snapshot_dir_path: &Path,
    ) -> OperationResult<()> {
        // Snapshotting may take long-running read locks on segments blocking incoming writes, do
        // this through proxied segments to allow writes to continue.
        Self::proxy_all_segments_and_apply(
            segments,
            collection_path,
            collection_params,
            |segment| {
                let read_segment = segment.read();
                read_segment.take_snapshot(temp_dir, snapshot_dir_path)?;
                Ok(())
            },
        )
    }

    pub fn report_optimizer_error<E: Into<CollectionError>>(&mut self, error: E) {
        // Save only the first error
        // If is more likely to be the real cause of all further problems
        if self.optimizer_errors.is_none() {
            self.optimizer_errors = Some(error.into());
        }
    }

    /// Duplicated points can appear in case of interrupted optimization.
    /// LocalShard can still work with duplicated points, but it is better to remove them.
    /// Duplicated points should not affect the search results.
    ///
    /// Checks all segments and removes duplicated and outdated points.
    /// If two points have the same id, the point with the highest version is kept.
    /// If two points have the same id and version, one of them is kept.
    ///
    /// Deduplication works with plain segments only.
    pub fn deduplicate_points(&self) -> OperationResult<usize> {
        let points_to_remove = self.find_duplicated_points()?;

        let mut removed_points = 0;
        for (segment_id, points) in points_to_remove {
            let locked_segment = self.segments.get(&segment_id).unwrap();
            let segment_arc = locked_segment.get();
            let mut write_segment = segment_arc.write();
            for point_id in points {
                if let Some(point_version) = write_segment.point_version(point_id) {
                    removed_points += 1;
                    write_segment.delete_point(point_version, point_id)?;
                }
            }
        }
        Ok(removed_points)
    }

    fn find_duplicated_points(&self) -> OperationResult<HashMap<SegmentId, Vec<PointIdType>>> {
        let segments = self
            .segments
            .iter()
            .map(|(&segment_id, locked_segment)| (segment_id, locked_segment.get()))
            .collect_vec();
        let locked_segments = BTreeMap::from_iter(
            segments
                .iter()
                .map(|(segment_id, locked_segment)| (*segment_id, locked_segment.read())),
        );
        let mut iterators = BTreeMap::from_iter(
            locked_segments
                .iter()
                .map(|(segment_id, locked_segment)| (*segment_id, locked_segment.iter_points())),
        );

        // heap contains the current iterable point id from each segment
        let mut heap = iterators
            .iter_mut()
            .filter_map(|(&segment_id, iter)| {
                iter.next().map(|point_id| DedupPoint {
                    segment_id,
                    point_id,
                })
            })
            .collect::<BinaryHeap<_>>();

        let mut last_point_id_opt = None;
        let mut last_segment_id_opt = None;
        let mut last_point_version_opt = None;
        let mut points_to_remove: HashMap<SegmentId, Vec<PointIdType>> = Default::default();

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
                let last_point_id = last_point_id_opt.unwrap();
                let last_segment_id = last_segment_id_opt.unwrap();

                let point_version = locked_segments[&segment_id].point_version(point_id);
                let last_point_version = if let Some(last_point_version) = last_point_version_opt {
                    last_point_version
                } else {
                    let version = locked_segments[&last_segment_id].point_version(last_point_id);
                    last_point_version_opt = Some(version);
                    version
                };

                // choose newer version between point_id and last_point_id
                if point_version < last_point_version {
                    points_to_remove
                        .entry(segment_id)
                        .or_default()
                        .push(point_id);
                } else {
                    last_point_id_opt = Some(point_id);
                    last_segment_id_opt = Some(segment_id);
                    last_point_version_opt = Some(point_version);
                    points_to_remove
                        .entry(last_segment_id)
                        .or_default()
                        .push(last_point_id);
                }
            } else {
                last_point_id_opt = Some(point_id);
                last_segment_id_opt = Some(segment_id);
            }
        }

        Ok(points_to_remove)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::read_dir;

    use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
    use segment::types::Distance;
    use serde_json::json;
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::{build_segment_1, build_segment_2};

    #[test]
    fn test_add_and_swap() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let segment1 = build_segment_1(dir.path());
        let segment2 = build_segment_2(dir.path());

        let mut holder = SegmentHolder::default();

        let sid1 = holder.add(segment1);
        let sid2 = holder.add(segment2);

        assert_ne!(sid1, sid2);

        let segment3 = build_simple_segment(dir.path(), 4, Distance::Dot).unwrap();

        let (_sid3, replaced_segments) = holder.swap(segment3, &[sid1, sid2]);
        replaced_segments
            .into_iter()
            .for_each(|s| s.drop_data().unwrap());
    }

    #[rstest::rstest]
    #[case::do_update_nonappendable(true)]
    #[case::dont_update_nonappendable(false)]
    fn test_apply_to_appendable(#[case] update_nonappendable: bool) {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

        let segment1 = build_segment_1(dir.path());
        let mut segment2 = build_segment_2(dir.path());

        segment2.appendable_flag = false;

        let mut holder = SegmentHolder::default();

        let sid1 = holder.add(segment1);
        let sid2 = holder.add(segment2);

        let op_num = 100;
        let mut processed_points: Vec<PointIdType> = vec![];
        holder
            .apply_points_with_conditional_move(
                op_num,
                &[1.into(), 2.into(), 11.into(), 12.into()],
                |point_id, segment| {
                    processed_points.push(point_id);
                    assert!(segment.has_point(point_id));
                    Ok(true)
                },
                |_| update_nonappendable,
            )
            .unwrap();

        assert_eq!(4, processed_points.len());

        let locked_segment_1 = holder.get(sid1).unwrap().get();
        let read_segment_1 = locked_segment_1.read();
        let locked_segment_2 = holder.get(sid2).unwrap().get();
        let read_segment_2 = locked_segment_2.read();

        assert!(read_segment_1.has_point(1.into()));
        assert!(read_segment_1.has_point(2.into()));

        // Points moved or not moved on apply based on appendable flag
        assert_eq!(read_segment_1.has_point(11.into()), !update_nonappendable);
        assert_eq!(read_segment_1.has_point(12.into()), !update_nonappendable);
        assert_eq!(read_segment_2.has_point(11.into()), update_nonappendable);
        assert_eq!(read_segment_2.has_point(12.into()), update_nonappendable);
    }

    #[test]
    fn test_points_deduplication() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

        let mut segment1 = build_segment_1(dir.path());
        let mut segment2 = build_segment_1(dir.path());

        segment1
            .set_payload(100, 1.into(), &json!({}).into(), &None)
            .unwrap();
        segment1
            .set_payload(100, 2.into(), &json!({}).into(), &None)
            .unwrap();

        segment2
            .set_payload(200, 4.into(), &json!({}).into(), &None)
            .unwrap();
        segment2
            .set_payload(200, 5.into(), &json!({}).into(), &None)
            .unwrap();

        let mut holder = SegmentHolder::default();

        let sid1 = holder.add(segment1);
        let sid2 = holder.add(segment2);

        let res = holder.deduplicate_points().unwrap();

        assert_eq!(5, res);

        assert!(holder.get(sid1).unwrap().get().read().has_point(1.into()));
        assert!(holder.get(sid1).unwrap().get().read().has_point(2.into()));
        assert!(!holder.get(sid2).unwrap().get().read().has_point(1.into()));
        assert!(!holder.get(sid2).unwrap().get().read().has_point(2.into()));

        assert!(holder.get(sid2).unwrap().get().read().has_point(4.into()));
        assert!(holder.get(sid2).unwrap().get().read().has_point(5.into()));
        assert!(!holder.get(sid1).unwrap().get().read().has_point(4.into()));
        assert!(!holder.get(sid1).unwrap().get().read().has_point(5.into()));
    }

    #[test]
    fn test_snapshot_all() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let segment1 = build_segment_1(dir.path());
        let segment2 = build_segment_2(dir.path());

        let mut holder = SegmentHolder::default();

        let sid1 = holder.add(segment1);
        let sid2 = holder.add(segment2);
        assert_ne!(sid1, sid2);

        let holder = Arc::new(RwLock::new(holder));

        let collection_dir = Builder::new().prefix("collection_dir").tempdir().unwrap();
        let temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();
        let snapshot_dir = Builder::new().prefix("snapshot_dir").tempdir().unwrap();
        SegmentHolder::snapshot_all_segments(
            holder,
            collection_dir.path(),
            None,
            temp_dir.path(),
            snapshot_dir.path(),
        )
        .unwrap();

        let archive_count = read_dir(&snapshot_dir).unwrap().count();
        // one archive produced per concrete segment in the SegmentHolder
        assert_eq!(archive_count, 2);
    }
}
