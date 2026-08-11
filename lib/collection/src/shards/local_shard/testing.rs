use segment::id_tracker::IdTracker;
use shard::segment_holder::FlushMode;
use shard::segment_holder::locked::LockedSegmentHolder;

use crate::shards::local_shard::LocalShard;

impl LocalShard {
    /// Testing helper: segment holder, for the model testing reload postmortem (see
    /// `model_testing::verify::describe_missing_points`).
    pub(crate) fn segments_for_testing(&self) -> LockedSegmentHolder {
        self.segments.clone()
    }

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
        segments.flush_all(FlushMode::Sync, true).unwrap();
    }
}
