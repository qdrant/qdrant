use std::sync::{RwLock, Arc, RwLockReadGuard};
use std::collections::HashMap;
use segment::entry::entry_point::SegmentEntry;
use std::collections::hash_map::{Iter};
use rand::{thread_rng, Rng};
use rand::seq::SliceRandom;


pub type SegmentId = usize;

pub type LockedSegment = Arc<RwLock<dyn SegmentEntry>>;


pub struct SegmentHolder {
    segments: HashMap<SegmentId, LockedSegment>,
}

impl<'s> SegmentHolder {
    pub fn new() -> Self {
        SegmentHolder {
            segments: Default::default()
        }
    }

    pub fn iter(&'s self) -> impl Iterator<Item=(&SegmentId, &LockedSegment)> + 's {
        self.segments.iter()
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
    pub fn add<E: SegmentEntry + 'static>(&mut self, segment: E) -> SegmentId {
        let key = self.generate_new_key();
        self.segments.insert(key, Arc::new(RwLock::new(segment)));
        return key;
    }

    /// Replace old segments with a new one
    pub fn swap<E: SegmentEntry + 'static>(&mut self, segment: E, remove_ids: &Vec<SegmentId>) -> SegmentId {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
    use segment::types::Distance;
    use std::path::Path;
    use segment::segment::Segment;
    use crate::segment_manager::fixtures::{build_segment_1, build_segment_2};


    #[test]
    fn test_add_and_swap() {
        let segment1 = build_segment_1();
        let segment2 = build_segment_2();

        let mut holder = SegmentHolder::new();

        let sid1 = holder.add(segment1);
        let sid2 = holder.add(segment2);

        assert_ne!(sid1, sid2);

        let tmp_path = Path::new("/tmp/qdrant/segment");
        let mut segment3 = build_simple_segment(tmp_path, 4, Distance::Dot);

        let sid3 = holder.swap(segment3, &vec![sid1, sid2]);
    }
}

