use crate::segment_manager::holders::segment_holder::{SegmentId, LockedSegment, LockerSegmentHolder};
use segment::types::{SegmentType};
use ordered_float::OrderedFloat;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use crate::operations::types::CollectionConfig;
use crate::operations::index_def::Indexes;
use crate::segment_manager::optimizers::segment_optimizer::SegmentOptimizer;
use segment::segment::Segment;

pub struct VacuumOptimizer {
    deleted_threshold: f64,
    min_vectors_number: usize,
    config: CollectionConfig,
}


impl VacuumOptimizer {
    fn worst_segment(&self, segments: LockerSegmentHolder) -> Option<(SegmentId, LockedSegment)> {
        segments.read().unwrap().iter()
            .map(|(idx, segment)| (*idx, segment.0.read().unwrap().info()))
            .filter(|(_, info)| info.segment_type != SegmentType::Special)
            .filter(|(_, info)| info.num_vectors < self.min_vectors_number)
            .filter(|(_, info)| info.num_vectors < self.min_vectors_number)
            .filter(|(_, info)| info.num_deleted_vectors as f64 / info.num_vectors as f64 > self.deleted_threshold)
            .max_by_key(|(_, info)| OrderedFloat(info.num_deleted_vectors as f64 / info.num_vectors as f64))
            .and_then(|(idx, _)| Some((idx, segments.read().unwrap().get(idx).unwrap().mk_copy())))
    }
}


impl SegmentOptimizer for VacuumOptimizer {
    fn check_condition(&self, segments: LockerSegmentHolder) -> Vec<SegmentId> {
        match self.worst_segment(segments) {
            None => vec![],
            Some((segment_id, _segment)) => vec![segment_id],
        }
    }

    fn temp_segment(&self) -> LockedSegment {
        LockedSegment::new(build_simple_segment(self.config.vector_size, self.config.distance))
    }

    fn optimized_segment(&self) -> Segment {
        match self.config.index {
            Indexes::Plain { .. } => {
                build_simple_segment(self.config.vector_size, self.config.distance)
            }
            Indexes::Hnsw { .. } => unimplemented!(),
        }
    }
}

