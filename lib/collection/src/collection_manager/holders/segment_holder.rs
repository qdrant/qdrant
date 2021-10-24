use std::cmp::{max, min};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};

use segment::entry::entry_point::{OperationError, OperationResult, SegmentEntry};
use segment::segment::Segment;
use segment::types::{PointIdType, SeqNumberType};

use crate::collection_manager::holders::proxy_segment::ProxySegment;
use std::ops::Mul;
use std::time::Duration;

pub type SegmentId = usize;

pub enum LockedSegment {
    Original(Arc<RwLock<Segment>>),
    Proxy(Arc<RwLock<ProxySegment>>),
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

    pub fn drop_data(self) -> OperationResult<()> {
        self.get().write().drop_data()?;
        Ok(())
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

unsafe impl Sync for LockedSegment {}

unsafe impl Send for LockedSegment {}

#[derive(Default)]
pub struct SegmentHolder {
    segments: HashMap<SegmentId, LockedSegment>,
    /// Seq number of the first un-recovered operation.
    /// If there are no failed operation - None
    pub failed_operation: BTreeSet<SeqNumberType>,
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

    pub fn remove(&mut self, remove_ids: &[SegmentId], drop_data: bool) -> OperationResult<()> {
        for remove_id in remove_ids {
            let removed_segment = self.segments.remove(remove_id);

            if drop_data {
                match removed_segment {
                    None => {}
                    Some(segment) => segment.drop_data()?,
                }
            }
        }
        Ok(())
    }

    /// Replace old segments with a new one
    pub fn swap<T>(
        &mut self,
        segment: T,
        remove_ids: &[SegmentId],
        drop_data: bool,
    ) -> OperationResult<SegmentId>
    where
        T: Into<LockedSegment>,
    {
        let new_id = self.add(segment);
        self.remove(remove_ids, drop_data)?;
        Ok(new_id)
    }

    pub fn get(&self, id: SegmentId) -> Option<&LockedSegment> {
        self.segments.get(&id)
    }

    pub fn appendable_ids(&self) -> Vec<SegmentId> {
        self.segments
            .iter()
            .filter(|(_, x)| x.get().read().is_appendable())
            .map(|(id, _)| id)
            .cloned()
            .collect()
    }

    pub fn appendable_segments(&self) -> Vec<SegmentId> {
        self.segments
            .iter()
            .filter(|(_idx, seg)| seg.get().read().is_appendable())
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
    fn segment_points(&self, ids: &[PointIdType], segment: &LockedSegment) -> Vec<PointIdType> {
        let segment_arc = segment.get();
        let entry = segment_arc.read();
        ids.iter()
            .cloned()
            .filter(|id| entry.has_point(*id))
            .collect()
    }

    pub fn apply_segments<F>(&self, mut f: F) -> OperationResult<usize>
    where
        F: FnMut(&mut RwLockWriteGuard<dyn SegmentEntry + 'static>) -> OperationResult<bool>,
    {
        let mut processed_segments = 0;
        for (_idx, segment) in self.segments.iter() {
            let is_applied = f(&mut segment.get().write())?;
            processed_segments += is_applied as usize;
        }
        Ok(processed_segments)
    }

    pub fn apply_points<F>(&self, ids: &[PointIdType], mut f: F) -> OperationResult<usize>
    where
        F: FnMut(
            PointIdType,
            SegmentId,
            &mut RwLockWriteGuard<dyn SegmentEntry>,
        ) -> OperationResult<bool>,
    {
        let mut applied_points = 0;
        for (idx, segment) in self.segments.iter() {
            // Collect affected points first, we want to lock segment for writing as rare as possible
            let segment_points = self.segment_points(ids, segment);
            if !segment_points.is_empty() {
                let segment_arc = segment.get();
                let mut write_segment = segment_arc.write();
                for point_id in segment_points {
                    let is_applied = f(point_id, *idx, &mut write_segment)?;
                    applied_points += is_applied as usize;
                }
            }
        }
        Ok(applied_points)
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
            return Err(OperationError::ServiceError {
                description: "No appendable segments exists, expected at least one".to_string(),
            });
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
        let mut timeout = Duration::from_nanos(100);
        loop {
            let (segment_id, segment_lock) = entries.choose(&mut rng).unwrap();
            let opt_segment_guard = segment_lock.try_write_for(timeout);

            match opt_segment_guard {
                None => timeout = timeout.mul(2), // Wait longer next time
                Some(mut lock) => {
                    return apply(*segment_id, &mut lock);
                }
            }
        }
    }

    /// Update function wrapper, which ensures that updates are not applied written to un-appendable segment.
    /// In case of such attempt, this function will move data into a mutable segment and remove data from un-appendable.
    /// Returns: Set of point ids which were successfully(already) applied to segments
    pub fn apply_points_to_appendable<F>(
        &self,
        op_num: SeqNumberType,
        ids: &[PointIdType],
        mut f: F,
    ) -> OperationResult<HashSet<PointIdType>>
    where
        F: FnMut(PointIdType, &mut RwLockWriteGuard<dyn SegmentEntry>) -> OperationResult<bool>,
    {
        // Choose random appendable segment from this
        let appendable_segments = self.appendable_segments();

        let mut applied_points: HashSet<PointIdType> = Default::default();

        let _applied_points_count = self.apply_points(ids, |point_id, _idx, write_segment| {
            if let Some(point_version) = write_segment.point_version(point_id) {
                if point_version >= op_num {
                    applied_points.insert(point_id);
                    return Ok(false);
                }
            }

            let is_applied = if write_segment.is_appendable() {
                f(point_id, write_segment)?
            } else {
                self.aloha_random_write(
                    &appendable_segments,
                    |_appendable_idx, appendable_write_segment| {
                        let vector = write_segment.vector(point_id)?;
                        let payload = write_segment.payload(point_id)?;

                        appendable_write_segment.upsert_point(op_num, point_id, &vector)?;
                        appendable_write_segment.set_full_payload(op_num, point_id, payload)?;

                        write_segment.delete_point(op_num, point_id)?;

                        f(point_id, appendable_write_segment)
                    },
                )?
            };
            applied_points.insert(point_id);
            Ok(is_applied)
        })?;
        Ok(applied_points)
    }

    pub fn read_points<F>(&self, ids: &[PointIdType], mut f: F) -> OperationResult<usize>
    where
        F: FnMut(PointIdType, &RwLockReadGuard<dyn SegmentEntry>) -> OperationResult<bool>,
    {
        let mut read_points = 0;
        for (_idx, segment) in self.segments.iter() {
            let segment_arc = segment.get();
            let read_segment = segment_arc.read();
            for point in ids.iter().cloned().filter(|id| read_segment.has_point(*id)) {
                let is_ok = f(point, &read_segment)?;
                read_points += is_ok as usize;
            }
        }
        Ok(read_points)
    }

    /// Flushes all segments and returns maximum version to persist
    ///
    /// If there are unsaved changes after flush - detects lowest unsaved change version.
    /// If all changes are saved - returns max version.
    pub fn flush_all(&self) -> OperationResult<SeqNumberType> {
        let mut max_persisted_version: SeqNumberType = SeqNumberType::MIN;
        let mut min_unsaved_version: SeqNumberType = SeqNumberType::MAX;
        let mut has_unsaved = false;
        for (_idx, segment) in self.segments.iter() {
            let segment_lock = segment.get();
            let read_segment = segment_lock.read();
            let segment_version = read_segment.version();
            let segment_persisted_version = read_segment.flush()?;

            if segment_version > segment_persisted_version {
                has_unsaved = true;
                min_unsaved_version = min(min_unsaved_version, segment_persisted_version);
            }

            max_persisted_version = max(max_persisted_version, segment_persisted_version)
        }
        if has_unsaved {
            Ok(min_unsaved_version)
        } else {
            Ok(max_persisted_version)
        }
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
    use segment::types::Distance;

    use crate::collection_manager::fixtures::{build_segment_1, build_segment_2};

    use super::*;
    use std::{thread, time};

    #[test]
    fn test_add_and_swap() {
        let dir = TempDir::new("segment_dir").unwrap();
        let segment1 = build_segment_1(dir.path());
        let segment2 = build_segment_2(dir.path());

        let mut holder = SegmentHolder::default();

        let sid1 = holder.add(segment1);
        let sid2 = holder.add(segment2);

        assert_ne!(sid1, sid2);

        let segment3 = build_simple_segment(dir.path(), 4, Distance::Dot).unwrap();

        let _sid3 = holder.swap(segment3, &vec![sid1, sid2], true).unwrap();
    }

    #[test]
    fn test_aloha_locking() {
        let dir = TempDir::new("segment_dir").unwrap();

        let segment1 = build_segment_1(dir.path());
        let segment2 = build_segment_2(dir.path());

        let mut holder = SegmentHolder::default();

        let sid1 = holder.add(segment1);
        let sid2 = holder.add(segment2);

        let locked_segment1 = holder.get(sid1).unwrap().get();
        let locked_segment2 = holder.get(sid2).unwrap().clone();

        let _read_lock_1 = locked_segment1.read();

        let handler = thread::spawn(move || {
            let lc = locked_segment2.get();
            let _guard = lc.read();
            thread::sleep(time::Duration::from_millis(100));
        });

        thread::sleep(time::Duration::from_millis(10));

        holder
            .aloha_random_write(&vec![sid1, sid2], |idx, _seg| {
                assert_eq!(idx, sid2);
                Ok(true)
            })
            .unwrap();

        handler.join().unwrap();
    }

    #[test]
    fn test_apply_to_appendable() {
        let dir = TempDir::new("segment_dir").unwrap();

        let segment1 = build_segment_1(dir.path());
        let mut segment2 = build_segment_2(dir.path());

        segment2.appendable_flag = false;

        let mut holder = SegmentHolder::default();

        let sid1 = holder.add(segment1);
        let _sid2 = holder.add(segment2);

        let mut processed_points: Vec<PointIdType> = vec![];
        holder
            .apply_points_to_appendable(100, &vec![1, 2, 11, 12], |point_id, segment| {
                processed_points.push(point_id);
                assert!(segment.has_point(point_id));
                Ok(true)
            })
            .unwrap();

        assert_eq!(4, processed_points.len());

        let locked_segment_1 = holder.get(sid1).unwrap().get();
        let read_segment_1 = locked_segment_1.read();

        assert!(read_segment_1.has_point(1));
        assert!(read_segment_1.has_point(2));

        // Points moved on apply
        assert!(read_segment_1.has_point(11));
        assert!(read_segment_1.has_point(12));
    }
}
