use crate::collection_manager::holders::segment_holder::{
    LockedSegment, LockedSegmentHolder, SegmentId,
};
use crate::collection_manager::optimizers::segment_optimizer::{
    OptimizerThresholds, SegmentOptimizer,
};
use crate::config::CollectionParams;
use ordered_float::OrderedFloat;
use segment::types::{HnswConfig, SegmentType};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// Optimizer which looks for segments with hig amount of soft-deleted points.
/// Used to free up space.
pub struct VacuumOptimizer {
    deleted_threshold: f64,
    min_vectors_number: usize,
    thresholds_config: OptimizerThresholds,
    segments_path: PathBuf,
    collection_temp_dir: PathBuf,
    collection_params: CollectionParams,
    hnsw_config: HnswConfig,
}

impl VacuumOptimizer {
    pub fn new(
        deleted_threshold: f64,
        min_vectors_number: usize,
        thresholds_config: OptimizerThresholds,
        segments_path: PathBuf,
        collection_temp_dir: PathBuf,
        collection_params: CollectionParams,
        hnsw_config: HnswConfig,
    ) -> Self {
        VacuumOptimizer {
            deleted_threshold,
            min_vectors_number,
            thresholds_config,
            segments_path,
            collection_temp_dir,
            collection_params,
            hnsw_config,
        }
    }

    fn worst_segment(
        &self,
        segments: LockedSegmentHolder,
        excluded_ids: &HashSet<SegmentId>,
    ) -> Option<(SegmentId, LockedSegment)> {
        let segments_read_guard = segments.read();
        segments_read_guard
            .iter()
            // .map(|(idx, segment)| (*idx, segment.get().read().info()))
            .filter_map(|(idx, segment)| {
                if excluded_ids.contains(idx) {
                    // This segment is excluded externally. It might already be scheduled for optimization
                    return None;
                }

                let segment_entry = segment.get();
                let read_segment = segment_entry.read();
                let littered_ratio =
                    read_segment.deleted_count() as f64 / read_segment.vectors_count() as f64;

                let is_big = read_segment.vectors_count() >= self.min_vectors_number;
                let is_not_special = read_segment.segment_type() != SegmentType::Special;
                let is_littered = littered_ratio > self.deleted_threshold;

                match is_big && is_not_special && is_littered {
                    true => Some((*idx, littered_ratio)),
                    false => None,
                }
            })
            .max_by_key(|(_, ratio)| OrderedFloat(*ratio))
            .map(|(idx, _)| (idx, segments_read_guard.get(idx).unwrap().clone()))
    }
}

impl SegmentOptimizer for VacuumOptimizer {
    fn collection_path(&self) -> &Path {
        self.segments_path.as_path()
    }

    fn temp_path(&self) -> &Path {
        self.collection_temp_dir.as_path()
    }

    fn collection_params(&self) -> CollectionParams {
        self.collection_params.clone()
    }

    fn hnsw_config(&self) -> HnswConfig {
        self.hnsw_config
    }

    fn threshold_config(&self) -> &OptimizerThresholds {
        &self.thresholds_config
    }

    fn check_condition(
        &self,
        segments: LockedSegmentHolder,
        excluded_ids: &HashSet<SegmentId>,
    ) -> Vec<SegmentId> {
        match self.worst_segment(segments, excluded_ids) {
            None => vec![],
            Some((segment_id, _segment)) => vec![segment_id],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection_manager::fixtures::random_segment;
    use crate::collection_manager::holders::segment_holder::SegmentHolder;
    use itertools::Itertools;
    use parking_lot::RwLock;
    use rand::Rng;
    use segment::types::{Distance, PayloadType};
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use tempdir::TempDir;

    #[test]
    fn test_vacuum_conditions() {
        let temp_dir = TempDir::new("segment_temp_dir").unwrap();
        let dir = TempDir::new("segment_dir").unwrap();
        let mut holder = SegmentHolder::default();
        let segment_id = holder.add(random_segment(dir.path(), 100, 200, 4));

        let segment = holder.get(segment_id).unwrap();

        let original_segment_path = match segment {
            LockedSegment::Original(s) => s.read().current_path.clone(),
            LockedSegment::Proxy(_) => panic!("Not expected"),
        };

        let mut rnd = rand::thread_rng();

        let segment_points_to_delete = segment
            .get()
            .read()
            .iter_points()
            .filter(|_| rnd.gen_bool(0.5))
            .collect_vec();

        for &point_id in &segment_points_to_delete {
            segment.get().write().delete_point(101, point_id).unwrap();
        }

        let segment_points_to_assign1 = segment
            .get()
            .read()
            .iter_points()
            .filter(|_| rnd.gen_bool(0.05))
            .collect_vec();

        let segment_points_to_assign2 = segment
            .get()
            .read()
            .iter_points()
            .filter(|_| rnd.gen_bool(0.05))
            .collect_vec();

        for &point_id in &segment_points_to_assign1 {
            segment
                .get()
                .write()
                .set_payload(
                    102,
                    point_id,
                    &"color".to_string(),
                    PayloadType::Keyword(vec!["red".to_string()]),
                )
                .unwrap();
        }

        for &point_id in &segment_points_to_assign2 {
            segment
                .get()
                .write()
                .set_payload(
                    102,
                    point_id,
                    &"size".to_string(),
                    PayloadType::Float(vec![0.42]),
                )
                .unwrap();
        }

        let locked_holder = Arc::new(RwLock::new(holder));

        let vacuum_optimizer = VacuumOptimizer::new(
            0.2,
            50,
            OptimizerThresholds {
                memmap_threshold: 1000000,
                indexing_threshold: 1000000,
                payload_indexing_threshold: 1000000,
            },
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            CollectionParams {
                vector_size: 4,
                distance: Distance::Dot,
            },
            Default::default(),
        );

        let suggested_to_optimize =
            vacuum_optimizer.check_condition(locked_holder.clone(), &Default::default());

        // Check that only one segment is selected for optimization
        assert_eq!(suggested_to_optimize.len(), 1);

        vacuum_optimizer
            .optimize(
                locked_holder.clone(),
                suggested_to_optimize,
                &AtomicBool::new(false),
            )
            .unwrap();

        let after_optimization_segments =
            locked_holder.read().iter().map(|(x, _)| *x).collect_vec();

        // Check only one new segment
        assert_eq!(after_optimization_segments.len(), 1);

        let optimized_segment_id = *after_optimization_segments.get(0).unwrap();

        let holder_guard = locked_holder.read();
        let optimized_segment = holder_guard.get(optimized_segment_id).unwrap();
        let segment_arc = optimized_segment.get();
        let segment_guard = segment_arc.read();

        // Check new segment have proper amount of points
        assert_eq!(
            segment_guard.vectors_count(),
            200 - segment_points_to_delete.len()
        );

        // Check payload is preserved in optimized segment
        for &point_id in &segment_points_to_assign1 {
            assert!(segment_guard.has_point(point_id));
            let payload = segment_guard
                .payload(point_id)
                .unwrap()
                .get("color")
                .unwrap()
                .clone();

            match payload {
                PayloadType::Keyword(x) => assert_eq!(x[0], "red"),
                _ => panic!(),
            }
        }

        // Check old segment data is removed from disk
        assert!(!original_segment_path.exists());
    }
}
