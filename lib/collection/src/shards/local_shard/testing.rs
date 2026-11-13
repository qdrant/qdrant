use std::sync::atomic::Ordering;

use segment::id_tracker::IdTracker;
use shard::segment_holder::FlushMode;

use crate::shards::local_shard::LocalShard;

impl LocalShard {
    /// Number of WAL tail entries the last `load_from_wal` call handed to the update queue.
    ///
    /// Captured synchronously at load time, so unlike `local_update_queue_info`, reading this
    /// afterwards can't race against the update worker draining the queue in the background.
    pub fn wal_tail_queued(&self) -> usize {
        self.wal_tail_queued.load(Ordering::Relaxed)
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
