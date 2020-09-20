use crate::segment_manager::optimizers::segment_optimizer::SegmentOptimizer;
use crate::segment_manager::holders::segment_holder::{LockedSegmentHolder, SegmentId, LockedSegment};
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::segment::Segment;
use segment::types::{SegmentType, SegmentConfig};

use itertools::Itertools;
use segment::segment_constructor::segment_constructor::build_segment;
use std::path::PathBuf;
use crate::collection::OperationResult;


pub struct MergeOptimizer {
    max_segments: usize,
    segments_path: PathBuf,
    config: SegmentConfig,
}

impl MergeOptimizer {
    pub fn new(max_segments: usize, segments_path: PathBuf, config: SegmentConfig) -> Self {
        return MergeOptimizer { max_segments, segments_path, config };
    }
}


impl SegmentOptimizer for MergeOptimizer {
    fn check_condition(&self, segments: LockedSegmentHolder) -> Vec<SegmentId> {
        let read_segments = segments.read().unwrap();

        if read_segments.len() <= self.max_segments {
            return vec![];
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

    fn temp_segment(&self) -> OperationResult<LockedSegment> {
        Ok(LockedSegment::new(build_simple_segment(
            self.segments_path.as_path(),
            self.config.vector_size,
            self.config.distance,
        )?))
    }

    fn optimized_segment(&self) -> OperationResult<Segment> {
        Ok(build_segment(self.segments_path.as_path(), &self.config)?)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment_manager::fixtures::{random_segment};
    use crate::segment_manager::holders::segment_holder::SegmentHolder;
    use segment::types::{Distance, Indexes};
    use std::sync::{Arc, RwLock};
    use tempdir::TempDir;

    #[test]
    fn test_merge_optimizer() {
        let dir = TempDir::new("segment_dir").unwrap();

        let mut holder = SegmentHolder::new();


        let mut segments_to_merge = vec![];

        segments_to_merge.push(holder.add(random_segment(dir.path(), 100, 3, 4)));
        segments_to_merge.push(holder.add(random_segment(dir.path(), 100, 3, 4)));
        segments_to_merge.push(holder.add(random_segment(dir.path(), 100, 3, 4)));


        let mut other_segment_ids: Vec<SegmentId> = vec![];

        other_segment_ids.push(holder.add(random_segment(dir.path(), 100, 20, 4)));
        other_segment_ids.push(holder.add(random_segment(dir.path(), 100, 20, 4)));
        other_segment_ids.push(holder.add(random_segment(dir.path(), 100, 20, 4)));
        other_segment_ids.push(holder.add(random_segment(dir.path(), 100, 20, 4)));


        let merge_optimizer = MergeOptimizer::new(5, dir.path().to_owned(), SegmentConfig {
            vector_size: 4,
            index: Indexes::Plain {},
            distance: Distance::Dot,
        });

        let locked_holder = Arc::new(RwLock::new(holder));

        let suggested_for_merge = merge_optimizer.check_condition(locked_holder.clone());

        assert_eq!(suggested_for_merge.len(), 3);

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