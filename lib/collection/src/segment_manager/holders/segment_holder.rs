use std::cmp::min;
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};

use segment::entry::entry_point::{OperationError, OperationResult, SegmentEntry};
use segment::segment::Segment;
use segment::types::{PointIdType, SeqNumberType};

use crate::segment_manager::holders::proxy_segment::ProxySegment;

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
        return match self {
            LockedSegment::Original(segment) => segment.clone(),
            LockedSegment::Proxy(proxy) => proxy.clone(),
        };
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

pub struct SegmentHolder {
    segments: HashMap<SegmentId, LockedSegment>,
}

pub type LockedSegmentHolder = Arc<RwLock<SegmentHolder>>;

impl<'s> SegmentHolder {
    pub fn new() -> Self {
        SegmentHolder {
            segments: Default::default(),
        }
    }

    pub fn iter(&'s self) -> impl Iterator<Item = (&SegmentId, &LockedSegment)> + 's {
        self.segments.iter()
    }

    pub fn len(&self) -> usize {
        self.segments.len()
    }

    fn generate_new_key(&self) -> SegmentId {
        let key = thread_rng().gen::<SegmentId>();
        return if self.segments.contains_key(&key) {
            self.generate_new_key()
        } else {
            key
        };
    }

    /// Add new segment to storage
    pub fn add<T>(&mut self, segment: T) -> SegmentId
    where
        T: Into<LockedSegment>,
    {
        let key = self.generate_new_key();
        self.segments.insert(key, segment.into());
        return key;
    }

    /// Add new segment to storage which is already LockedSegment
    pub fn add_locked(&mut self, segment: LockedSegment) -> SegmentId {
        let key = self.generate_new_key();
        self.segments.insert(key, segment);
        return key;
    }

    pub fn remove(&mut self, remove_ids: &Vec<SegmentId>, drop_data: bool) -> OperationResult<()> {
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
        remove_ids: &Vec<SegmentId>,
        drop_data: bool,
    ) -> OperationResult<SegmentId>
    where
        T: Into<LockedSegment>,
    {
        let new_id = self.add(segment);
        self.remove(remove_ids, drop_data)?;
        return Ok(new_id);
    }

    pub fn get(&self, id: SegmentId) -> Option<&LockedSegment> {
        return self.segments.get(&id);
    }

    pub fn appendable_ids(&self) -> Vec<SegmentId> {
        self.segments
            .iter()
            .filter(|(_, x)| x.get().read().is_appendable())
            .map(|(id, _)| id)
            .cloned()
            .collect()
    }

    pub fn random_appendable_segment(&self) -> Option<LockedSegment> {
        let segments: Vec<_> = self
            .segments
            .values()
            .filter(|x| x.get().read().is_appendable())
            .collect();
        let segment = segments
            .choose(&mut rand::thread_rng())
            .cloned()
            .map(|x| x.clone());
        segment
    }

    /// Selects point ids, which is stored in this segment
    fn segment_points(&self, ids: &Vec<PointIdType>, segment: &LockedSegment) -> Vec<PointIdType> {
        let segment_arc = segment.get();
        let entry = segment_arc.read();
        ids.iter()
            .cloned()
            .filter(|id| entry.has_point(*id))
            .collect()
    }

    pub fn apply_segments<F>(&self, op_num: SeqNumberType, mut f: F) -> OperationResult<usize>
    where
        F: FnMut(&mut RwLockWriteGuard<dyn SegmentEntry + 'static>) -> OperationResult<bool>,
    {
        let mut processed_segments = 0;
        for (_idx, segment) in self.segments.iter() {
            // Skip this segment if it already have bigger version (WAL recovery related)
            if segment.get().read().version() > op_num {
                continue;
            }

            let is_applied = f(&mut segment.get().write())?;
            processed_segments += is_applied as usize;
        }
        Ok(processed_segments)
    }

    pub fn apply_points<F>(
        &self,
        op_num: SeqNumberType,
        ids: &Vec<PointIdType>,
        mut f: F,
    ) -> OperationResult<usize>
    where
        F: FnMut(PointIdType, &mut RwLockWriteGuard<dyn SegmentEntry>) -> OperationResult<bool>,
    {
        let mut applied_points = 0;
        for (_idx, segment) in self.segments.iter() {
            // Skip this segment if it already have bigger version (WAL recovery related)
            if segment.get().read().version() > op_num {
                continue;
            }
            // Collect affected points first, we want to lock segment for writing as rare as possible
            let segment_points = self.segment_points(ids, segment);
            if !segment_points.is_empty() {
                let segment_arc = segment.get();
                let mut write_segment = segment_arc.write();
                for point_id in segment_points {
                    let is_applied = f(point_id, &mut write_segment)?;
                    applied_points += is_applied as usize;
                }
            }
        }
        Ok(applied_points)
    }

    /// Update function wrapper, which ensures that updates are not applied written to un-appendable segment.
    /// In case of such attempt, this function will move data into a mutable segment and remove data from un-appendable.
    pub fn apply_points_to_appendable<F>(
        &self,
        op_num: SeqNumberType,
        ids: &Vec<PointIdType>,
        mut f: F,
    ) -> OperationResult<usize>
    where
        F: FnMut(PointIdType, &mut RwLockWriteGuard<dyn SegmentEntry>) -> OperationResult<bool>,
    {
        // Choose random appendable segment
        let default_write_segment =
            self.random_appendable_segment()
                .ok_or(OperationError::ServiceError {
                    description: "No appendable segments exists, expected at least one".to_string(),
                })?;

        let applied_points = self.apply_points(op_num, ids, |point_id, write_segment| {
            let is_applied = if write_segment.is_appendable() {
                f(point_id, write_segment)?
            } else {
                let default_segment_lock = default_write_segment.get();
                let mut default_segment_guard = default_segment_lock.write();
                let vector = write_segment.vector(point_id)?;
                let payload = write_segment.payload(point_id)?;

                default_segment_guard.upsert_point(op_num, point_id, &vector)?;
                default_segment_guard.set_full_payload(op_num, point_id, payload)?;

                write_segment.delete_point(op_num, point_id)?;

                f(point_id, &mut default_segment_guard)?
            };
            Ok(is_applied)
        })?;
        Ok(applied_points)
    }

    pub fn read_points<F>(&self, ids: &Vec<PointIdType>, mut f: F) -> OperationResult<usize>
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

    /// Flushes all segments and returns maximum persisted version
    pub fn flush_all(&self) -> OperationResult<SeqNumberType> {
        let mut persisted_version: SeqNumberType = SeqNumberType::MAX;
        for (_idx, segment) in self.segments.iter() {
            let segment_version = segment.get().read().flush()?;
            persisted_version = min(persisted_version, segment_version)
        }
        Ok(persisted_version)
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
    use segment::types::Distance;

    use crate::segment_manager::fixtures::{build_segment_1, build_segment_2};

    use super::*;

    #[test]
    fn test_add_and_swap() {
        let dir = TempDir::new("segment_dir").unwrap();
        let segment1 = build_segment_1(dir.path());
        let segment2 = build_segment_2(dir.path());

        let mut holder = SegmentHolder::new();

        let sid1 = holder.add(segment1);
        let sid2 = holder.add(segment2);

        assert_ne!(sid1, sid2);

        let segment3 = build_simple_segment(dir.path(), 4, Distance::Dot).unwrap();

        let _sid3 = holder.swap(segment3, &vec![sid1, sid2], true).unwrap();
    }
}
