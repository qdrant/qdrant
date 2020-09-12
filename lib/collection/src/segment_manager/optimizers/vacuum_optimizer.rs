use crate::segment_manager::holders::segment_holder::{SegmentId, LockedSegment, LockedSegmentHolder};
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
    pub fn new(deleted_threshold: f64,
               min_vectors_number: usize,
               config: CollectionConfig) -> Self {
        VacuumOptimizer {
            deleted_threshold,
            min_vectors_number,
            config,
        }
    }

    fn worst_segment(&self, segments: LockedSegmentHolder) -> Option<(SegmentId, LockedSegment)> {
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
    fn check_condition(&self, segments: LockedSegmentHolder) -> Vec<SegmentId> {
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


#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment_manager::holders::segment_holder::SegmentHolder;
    use crate::segment_manager::fixtures::random_segment;
    use itertools::Itertools;
    use rand::Rng;
    use std::sync::{RwLock, Arc};
    use segment::types::Distance;

    #[test]
    fn test_vacuum_conditions() {
        let mut holder = SegmentHolder::new();
        let segment_id = holder.add(random_segment(100, 200, 4));

        let segment = holder.get(segment_id).unwrap();

        let mut rnd = rand::thread_rng();

        let segment_points_to_delete = segment.0
            .read()
            .unwrap()
            .iter_points()
            .filter(|_| rnd.gen_bool(0.5)).
            collect_vec();

        for point_id in segment_points_to_delete.iter() {
            segment.0.write().unwrap().delete_point(101, *point_id).unwrap();
        }

        let locked_holder = Arc::new(RwLock::new(holder));

        let vacuum_optimizer = VacuumOptimizer::new(
            0.2,
            50,
            CollectionConfig {
                vector_size: 4,
                index: Indexes::Plain {},
                distance: Distance::Dot,
            },
        );

        let suggested_to_optimize = vacuum_optimizer.check_condition(locked_holder.clone());

        assert_eq!(suggested_to_optimize.len(), 1);

        vacuum_optimizer.optimize(locked_holder.clone(), suggested_to_optimize).unwrap();

        let after_optimization_segments = locked_holder
            .read().unwrap()
            .iter()
            .map(|(x, _)| *x)
            .collect_vec();

        assert_eq!(after_optimization_segments.len(), 1);

        let optimized_segment_id = *after_optimization_segments.get(0).unwrap();

        let holder_guard = locked_holder.read().unwrap();
        let optimized_segment = holder_guard.get(optimized_segment_id).unwrap();
        let segment_guard = optimized_segment.0.read().unwrap();

        assert_eq!(segment_guard.vectors_count(), 200 - segment_points_to_delete.len());

    }
}