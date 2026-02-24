use segment::id_tracker::IdTracker;

use crate::shards::local_shard::LocalShard;

impl LocalShard {
    // Testing helper: performs partial flush of the segments
    pub fn partial_flush(&self) {
        let segments = self.segments.read();

        for (_segment_id, segment) in segments.iter_original() {
            let segment = segment.read();
            segment.id_tracker.borrow().mapping_flusher()().unwrap();
        }
    }

    pub fn full_flush(&self) {
        let segments = self.segments.read();
        segments.flush_all(true, true).unwrap();
    }
}
