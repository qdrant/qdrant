use shard::locked_segment::LockedSegment;

use crate::shards::local_shard::LocalShard;

impl LocalShard {
    // Testing helper: performs partial flush of the segments
    pub fn partial_flush(&self) {
        let segments = self.segments.read();

        for (_segment_id, segment) in segments.iter() {
            match segment {
                LockedSegment::Original(raw_segment_lock) => {
                    let raw_segment = raw_segment_lock.read();

                    raw_segment.id_tracker.borrow().mapping_flusher()().unwrap();
                }
                LockedSegment::Proxy(_) => {} // Skipping
            }
        }
    }

    pub fn full_flush(&self) {
        let segments = self.segments.read();
        segments.flush_all(true, true).unwrap();
    }
}
