use crate::segment_manager::segment_managers::SegmentOptimizer;
use std::sync::{Arc, RwLock, Mutex};
use crate::segment_manager::holders::segment_holder::SegmentHolder;
use std::time::Duration;
use segment::types::SeqNumberType;

pub struct SimpleSegmentOptimizer {
    segments: Arc<RwLock<SegmentHolder>>,
    optimize_delay_operations: usize,
    optimize_delay_time: Duration,

    last_updated: Mutex<SeqNumberType>,
    deleted_threshold: f64,
    min_vectors_number: usize
}

impl SegmentOptimizer for SimpleSegmentOptimizer {
    fn check_condition(&self, _op_num: SeqNumberType) -> bool {
        for (_idx, segment) in self.segments.read().unwrap().iter() {
            let segment_info = segment.0.read().unwrap().info();

            if segment_info.num_vectors > self.min_vectors_number &&
                (segment_info.ram_usage_bytes as f64 / segment_info.num_vectors as f64 > self.deleted_threshold) {
                return true
            }
        }
        return false;
    }

    fn optimize(&self) {
        unimplemented!()
    }
}