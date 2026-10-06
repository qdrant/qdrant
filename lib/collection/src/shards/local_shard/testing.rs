use common::counter::hardware_counter::HardwareCounterCell;
use segment::id_tracker::IdTracker;
use shard::segment_holder::FlushMode;

use crate::shards::local_shard::LocalShard;

impl LocalShard {
    // Testing helper: performs partial flush of the segments
    pub fn partial_flush(&self, hw_counter: &HardwareCounterCell) {
        let segments = self.segments.read();

        for (_segment_id, segment) in segments.iter_original() {
            let segment = segment.read();
            segment.id_tracker.borrow().mapping_flusher(hw_counter)().unwrap();
        }
    }

    pub fn full_flush(&self, hw_counter: &HardwareCounterCell) {
        let segments = self.segments.read();
        segments
            .flush_all(FlushMode::Sync, true, hw_counter)
            .unwrap();
    }
}
