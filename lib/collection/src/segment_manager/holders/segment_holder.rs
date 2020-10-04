use parking_lot::{RwLock, RwLockWriteGuard, RwLockReadGuard};

use std::collections::HashMap;
use segment::entry::entry_point::{SegmentEntry, Result};

use rand::{thread_rng, Rng};
use rand::seq::SliceRandom;
use segment::types::{PointIdType, SeqNumberType};
use crate::collection::OperationResult;
use std::sync::Arc;
use segment::segment::Segment;
use crate::segment_manager::holders::proxy_segment::ProxySegment;
use std::cmp::min;


pub type SegmentId = usize;

pub enum LockedSegment {
    Original(Arc<RwLock<Segment>>),
    Proxy(Arc<RwLock<ProxySegment>>),
}


impl LockedSegment {
    pub fn new<T>(segment: T) -> Self where T: Into<LockedSegment> {
        segment.into()
    }

    pub fn mk_copy(&self) -> Self {
        match self {
            LockedSegment::Original(x) => LockedSegment::Original(x.clone()),
            LockedSegment::Proxy(x) => LockedSegment::Proxy(x.clone()),
        }
    }

    pub fn get(&self) -> Arc<RwLock<dyn SegmentEntry>> {
        return match self {
            LockedSegment::Original(segment) => segment.clone(),
            LockedSegment::Proxy(proxy) => proxy.clone()
        };
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
            segments: Default::default()
        }
    }

    pub fn iter(&'s self) -> impl Iterator<Item=(&SegmentId, &LockedSegment)> + 's {
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
    pub fn add<T>(&mut self, segment: T) -> SegmentId where T: Into<LockedSegment> {
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

    /// Replace old segments with a new one
    pub fn swap<T>(&mut self, segment: T, remove_ids: &Vec<SegmentId>) -> SegmentId where T: Into<LockedSegment> {
        let new_id = self.add(segment);
        for remove_id in remove_ids {
            self.segments.remove(remove_id);
        }
        return new_id;
    }

    pub fn get(&self, id: SegmentId) -> Option<&LockedSegment> {
        return self.segments.get(&id);
    }

    pub fn random_segment(&self) -> Option<&LockedSegment> {
        let segments: Vec<_> = self.segments.values().collect();
        segments.choose(&mut rand::thread_rng()).cloned()
    }

    /// Selects point ids, which is stored in this segment
    fn segment_points(&self, ids: &Vec<PointIdType>, segment: &LockedSegment) -> Vec<PointIdType> {
        let segment_arc = segment.get();
        let entry = segment_arc.read();
        ids
            .iter()
            .cloned()
            .filter(|id| entry.has_point(*id))
            .collect()
    }


    pub fn apply_segments<F>(&self, op_num: SeqNumberType, mut f: F) -> OperationResult<usize>
        where F: FnMut(&mut RwLockWriteGuard<dyn SegmentEntry + 'static>) -> Result<bool>
    {
        let mut processed_segments = 0;
        for (_idx, segment) in self.segments.iter() {
            // Skip this segment if it already have bigger version (WAL recovery related)
            if segment.get().read().version() > op_num { continue; }

            let is_applied = f(&mut segment.get().write())?;
            processed_segments += is_applied as usize;
        }
        Ok(processed_segments)
    }


    pub fn apply_points<F>(&self, op_num: SeqNumberType, ids: &Vec<PointIdType>, mut f: F) -> OperationResult<usize>
        where F: FnMut(PointIdType, &mut RwLockWriteGuard<dyn SegmentEntry>) -> Result<bool>
    {
        let mut applied_points = 0;
        for (_idx, segment) in self.segments.iter() {
            // Skip this segment if it already have bigger version (WAL recovery related)
            if segment.get().read().version() > op_num { continue; }
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

    pub fn read_points<F>(&self, ids: &Vec<PointIdType>, mut f: F) -> OperationResult<usize>
        where F: FnMut(PointIdType, &RwLockReadGuard<dyn SegmentEntry>) -> Result<bool>
    {
        let mut read_points = 0;
        for (_idx, segment) in self.segments.iter() {
            let segment_arc = segment.get();
            let read_segment = segment_arc.read();
            for point in ids
                .iter()
                .cloned()
                .filter(|id| read_segment.has_point(*id))
            {
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
    use super::*;
    use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
    use segment::types::Distance;

    use crate::segment_manager::fixtures::{build_segment_1, build_segment_2};
    use tempdir::TempDir;


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

        let _sid3 = holder.swap(segment3, &vec![sid1, sid2]);
    }
}

