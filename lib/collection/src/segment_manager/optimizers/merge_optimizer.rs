use crate::operations::types::CollectionConfig;
use crate::segment_manager::optimizers::segment_optimizer::SegmentOptimizer;
use crate::segment_manager::holders::segment_holder::{LockedSegmentHolder, SegmentId, LockedSegment};
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::segment::Segment;
use crate::operations::index_def::Indexes;
use segment::types::SegmentType;

use itertools::Itertools;


pub struct MergeOptimizer {
    max_segments: usize,
    config: CollectionConfig,
}

impl MergeOptimizer {
    pub fn new(max_segments: usize, config: CollectionConfig) -> Self {
        return MergeOptimizer {max_segments, config}
    }
}


impl SegmentOptimizer for MergeOptimizer {
    fn check_condition(&self, segments: LockedSegmentHolder) -> Vec<SegmentId> {
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
            .sorted_by_key(|(_, size)| *size)
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


#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment_manager::fixtures::{random_segment};
    use crate::segment_manager::holders::segment_holder::SegmentHolder;
    use segment::types::Distance;
    use std::sync::{Arc, RwLock};

    #[test]
    fn test_merge_optimizer() {
        let mut holder = SegmentHolder::new();


        let mut segments_to_merge = vec![];

        segments_to_merge.push(holder.add(random_segment(100, 3, 4)));
        segments_to_merge.push(holder.add(random_segment(100, 3, 4)));
        segments_to_merge.push(holder.add(random_segment(100, 3, 4)));


        let mut other_segment_ids: Vec<SegmentId> = vec![];

        other_segment_ids.push(holder.add(random_segment(100, 20, 4)));
        other_segment_ids.push(holder.add(random_segment(100, 20, 4)));
        other_segment_ids.push(holder.add(random_segment(100, 20, 4)));
        other_segment_ids.push(holder.add(random_segment(100, 20, 4)));


        let merge_optimizer = MergeOptimizer::new(5, CollectionConfig {
            vector_size: 4,
            index: Indexes::Plain {},
            distance: Distance::Dot
        });

        let locked_holder = Arc::new(RwLock::new(holder));

        let suggested_for_merge = merge_optimizer.check_condition(locked_holder.clone());

        assert_eq!(suggested_for_merge.len(),  3);

        for segment_in in suggested_for_merge.iter() {
            assert!(segments_to_merge.contains(&segment_in));
        }

        merge_optimizer.optimize(locked_holder.clone(), suggested_for_merge).unwrap();


        let after_optimization_segments = locked_holder
            .read().unwrap()
            .iter()
            .map(|(x, _)| *x)
            .collect_vec();

        assert_eq!(after_optimization_segments.len(), 5);

        for segment_id in other_segment_ids.iter() {
            assert!(after_optimization_segments.contains(&segment_id))
        }


        for segment_id in after_optimization_segments {
            if !other_segment_ids.contains(&segment_id) {
                let holder_guard = locked_holder.read().unwrap();
                let new_segment = holder_guard.get(segment_id).unwrap();
                assert_eq!(new_segment.0.read().unwrap().vectors_count(), 3 * 3);
            }
        }
    }
}