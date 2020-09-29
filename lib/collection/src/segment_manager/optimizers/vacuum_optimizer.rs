use crate::segment_manager::holders::segment_holder::{SegmentId, LockedSegment, LockedSegmentHolder};
use segment::types::{SegmentType, SegmentConfig};
use ordered_float::OrderedFloat;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use crate::segment_manager::optimizers::segment_optimizer::SegmentOptimizer;
use segment::segment::Segment;
use segment::segment_constructor::segment_constructor::build_segment;
use std::path::PathBuf;
use crate::collection::OperationResult;

pub struct VacuumOptimizer {
    deleted_threshold: f64,
    min_vectors_number: usize,
    segments_path: PathBuf,
    config: SegmentConfig,
}


impl VacuumOptimizer {
    pub fn new(deleted_threshold: f64,
               min_vectors_number: usize,
               segments_path: PathBuf,
               config: SegmentConfig) -> Self {
        VacuumOptimizer {
            deleted_threshold,
            min_vectors_number,
            segments_path,
            config,
        }
    }

    fn worst_segment(&self, segments: LockedSegmentHolder) -> Option<(SegmentId, LockedSegment)> {
        segments.read().iter()
            .map(|(idx, segment)| (*idx, segment.get().read().info()))
            .filter(|(_, info)| info.segment_type != SegmentType::Special)
            .filter(|(_, info)| info.num_vectors > self.min_vectors_number)
            .filter(|(_, info)| info.num_deleted_vectors as f64 / info.num_vectors as f64 > self.deleted_threshold)
            .max_by_key(|(_, info)| OrderedFloat(info.num_deleted_vectors as f64 / info.num_vectors as f64))
            .and_then(|(idx, _)| Some((idx, segments.read().get(idx).unwrap().mk_copy())))
    }
}


impl SegmentOptimizer for VacuumOptimizer {
    fn check_condition(&self, segments: LockedSegmentHolder) -> Vec<SegmentId> {
        match self.worst_segment(segments) {
            None => vec![],
            Some((segment_id, _segment)) => vec![segment_id],
        }
    }

    fn temp_segment(&self) -> OperationResult<LockedSegment> {
        Ok(LockedSegment::new(build_simple_segment(
            self.segments_path.as_path(),
            self.config.vector_size,
            self.config.distance,
        )?))
    }

    fn optimized_segment(&self, optimizing_segments: &Vec<LockedSegment>) -> OperationResult<Segment> {
        let optimizing_segment = optimizing_segments.get(0).unwrap();
        let config = optimizing_segment.get().read().config();
        Ok(build_segment(self.segments_path.as_path(), &config)?)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment_manager::holders::segment_holder::SegmentHolder;
    use crate::segment_manager::fixtures::random_segment;
    use itertools::Itertools;
    use rand::Rng;
    use std::sync::Arc;
    use segment::types::{Distance, Indexes, PayloadType, StorageType};
    use tempdir::TempDir;
    use parking_lot::RwLock;

    #[test]
    fn test_vacuum_conditions() {
        let dir = TempDir::new("segment_dir").unwrap();
        let mut holder = SegmentHolder::new();
        let segment_id = holder.add(random_segment(dir.path(), 100, 200, 4));

        let segment = holder.get(segment_id).unwrap();

        let mut rnd = rand::thread_rng();

        let segment_points_to_delete = segment.get()
            .read()
            .iter_points()
            .filter(|_| rnd.gen_bool(0.5)).
            collect_vec();

        for point_id in segment_points_to_delete.iter() {
            segment.get().write().delete_point(101, *point_id).unwrap();
        }

        let segment_points_to_assign1 = segment.get()
            .read()
            .iter_points()
            .filter(|_| rnd.gen_bool(0.05)).
            collect_vec();

        let segment_points_to_assign2 = segment.get()
            .read()
            .iter_points()
            .filter(|_| rnd.gen_bool(0.05)).
            collect_vec();

        for point_id in segment_points_to_assign1.iter() {
            segment.get().write().set_payload(
                102,
                *point_id,
                &"color".to_string(),
                PayloadType::Keyword(vec!["red".to_string()]),
            ).unwrap();
        }

        for point_id in segment_points_to_assign2.iter() {
            segment.get().write().set_payload(
                102,
                *point_id,
                &"size".to_string(),
                PayloadType::Float(vec![0.42]),
            ).unwrap();
        }

        let locked_holder = Arc::new(RwLock::new(holder));

        let vacuum_optimizer = VacuumOptimizer::new(
            0.2,
            50,
            dir.path().to_owned(),
            SegmentConfig {
                vector_size: 4,
                index: Indexes::Plain {},
                distance: Distance::Dot,
                storage_type: StorageType::InMemory
            },
        );

        let suggested_to_optimize = vacuum_optimizer.check_condition(locked_holder.clone());

        assert_eq!(suggested_to_optimize.len(), 1);

        vacuum_optimizer.optimize(locked_holder.clone(), suggested_to_optimize).unwrap();

        let after_optimization_segments = locked_holder
            .read()
            .iter()
            .map(|(x, _)| *x)
            .collect_vec();

        assert_eq!(after_optimization_segments.len(), 1);

        let optimized_segment_id = *after_optimization_segments.get(0).unwrap();

        let holder_guard = locked_holder.read();
        let optimized_segment = holder_guard.get(optimized_segment_id).unwrap();
        let segment_arc = optimized_segment.get();
        let segment_guard = segment_arc.read();

        assert_eq!(segment_guard.vectors_count(), 200 - segment_points_to_delete.len());


        for point_id in segment_points_to_assign1.iter() {
            assert!(segment_guard.has_point(*point_id));
            let payload = segment_guard.payload(*point_id).unwrap().get(&"color".to_string()).unwrap().clone();

            match payload {
                PayloadType::Keyword(x) => assert_eq!(x.get(0).unwrap(), &"red".to_string()),
                PayloadType::Integer(_) => assert!(false),
                PayloadType::Float(_) => assert!(false),
                PayloadType::Geo(_) => assert!(false),
            }
        }
    }
}