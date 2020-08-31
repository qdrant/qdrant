use crate::operations::types::CollectionConfig;
use crate::segment_manager::optimizers::segment_optimizer::SegmentOptimizer;
use crate::segment_manager::holders::segment_holder::{LockerSegmentHolder, SegmentId, LockedSegment};
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::segment::Segment;
use crate::operations::index_def::Indexes;
use segment::types::SegmentType;


pub struct MergeOptimizer {
    max_segments: usize,
    config: CollectionConfig,
}



impl SegmentOptimizer for MergeOptimizer {
    fn check_condition(&self, segments: LockerSegmentHolder) -> Vec<SegmentId> {
        let read_segments = segments.read().unwrap();

        if read_segments.len() <= self.max_segments {
            return vec![]
        }

        // Find top-3 smallest segments to join.
        // We need 3 segments because in this case we can guarantee that total segments number will be less

        read_segments.iter()
            .map(|(idx, segment)| (*idx, segment.0.read().unwrap().info()))
            .filter(|(_, info)| info.segment_type != SegmentType::Special)
            .map(|(idx, info)| (idx, info.num_vectors))
            .sorted_by_key(|(idx, size)| size)
            .take(3)
            .map(|x| x.0)
            .collect()
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



