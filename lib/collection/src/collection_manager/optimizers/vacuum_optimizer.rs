use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ordered_float::OrderedFloat;
use parking_lot::Mutex;
use segment::common::operation_time_statistics::{
    OperationDurationStatistics, OperationDurationsAggregator,
};
use segment::types::{HnswConfig, QuantizationConfig, SegmentType};

use crate::collection_manager::holders::segment_holder::{
    LockedSegment, LockedSegmentHolder, SegmentId,
};
use crate::collection_manager::optimizers::segment_optimizer::{
    OptimizerThresholds, SegmentOptimizer,
};
use crate::config::CollectionParams;

/// Optimizer which looks for segments with high amount of soft-deleted points.
/// Used to free up space.
pub struct VacuumOptimizer {
    deleted_threshold: f64,
    min_vectors_number: usize,
    thresholds_config: OptimizerThresholds,
    segments_path: PathBuf,
    collection_temp_dir: PathBuf,
    collection_params: CollectionParams,
    hnsw_config: HnswConfig,
    quantization_config: Option<QuantizationConfig>,
    telemetry_durations_aggregator: Arc<Mutex<OperationDurationsAggregator>>,
}

impl VacuumOptimizer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        deleted_threshold: f64,
        min_vectors_number: usize,
        thresholds_config: OptimizerThresholds,
        segments_path: PathBuf,
        collection_temp_dir: PathBuf,
        collection_params: CollectionParams,
        hnsw_config: HnswConfig,
        quantization_config: Option<QuantizationConfig>,
    ) -> Self {
        VacuumOptimizer {
            deleted_threshold,
            min_vectors_number,
            thresholds_config,
            segments_path,
            collection_temp_dir,
            collection_params,
            hnsw_config,
            quantization_config,
            telemetry_durations_aggregator: OperationDurationsAggregator::new(),
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
                    read_segment.deleted_point_count() as f64 / read_segment.points_count() as f64;

                let is_big = read_segment.points_count() >= self.min_vectors_number;
                let is_not_special = read_segment.segment_type() != SegmentType::Special;
                let is_littered = littered_ratio > self.deleted_threshold;

                (is_big && is_not_special && is_littered).then_some((*idx, littered_ratio))
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

    fn quantization_config(&self) -> Option<QuantizationConfig> {
        self.quantization_config.clone()
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

    fn get_telemetry_data(&self) -> OperationDurationStatistics {
        self.get_telemetry_counter().lock().get_statistics()
    }

    fn get_telemetry_counter(&self) -> Arc<Mutex<OperationDurationsAggregator>> {
        self.telemetry_durations_aggregator.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use itertools::Itertools;
    use parking_lot::RwLock;
    use rand::Rng;
    use segment::types::Distance;
    use serde_json::{json, Value};
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::random_segment;
    use crate::collection_manager::holders::segment_holder::SegmentHolder;
    use crate::operations::types::{VectorParams, VectorsConfig};

    #[test]
    fn test_vacuum_conditions() {
        let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
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
                .set_payload(102, point_id, &json!({ "color": "red" }).into())
                .unwrap();
        }

        for &point_id in &segment_points_to_assign2 {
            segment
                .get()
                .write()
                .set_payload(102, point_id, &json!({"size": 0.42}).into())
                .unwrap();
        }

        let locked_holder: Arc<RwLock<_>> = Arc::new(RwLock::new(holder));

        let vacuum_optimizer = VacuumOptimizer::new(
            0.2,
            50,
            OptimizerThresholds {
                max_segment_size: 1000000,
                memmap_threshold: 1000000,
                indexing_threshold: 1000000,
            },
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            CollectionParams {
                vectors: VectorsConfig::Single(VectorParams {
                    size: NonZeroU64::new(4).unwrap(),
                    distance: Distance::Dot,
                    hnsw_config: None,
                    quantization_config: None,
                }),
                shard_number: NonZeroU32::new(1).unwrap(),
                on_disk_payload: false,
                replication_factor: NonZeroU32::new(1).unwrap(),
                write_consistency_factor: NonZeroU32::new(1).unwrap(),
            },
            Default::default(),
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

        let optimized_segment_id = *after_optimization_segments.first().unwrap();

        let holder_guard = locked_holder.read();
        let optimized_segment = holder_guard.get(optimized_segment_id).unwrap();
        let segment_arc = optimized_segment.get();
        let segment_guard = segment_arc.read();

        // Check new segment have proper amount of points
        assert_eq!(
            segment_guard.points_count(),
            200 - segment_points_to_delete.len()
        );

        // Check payload is preserved in optimized segment
        for &point_id in &segment_points_to_assign1 {
            assert!(segment_guard.has_point(point_id));
            let payload = segment_guard.payload(point_id).unwrap();
            let payload_color = &(*payload.get_value("color").next().unwrap()).clone();

            match payload_color {
                Value::String(x) => assert_eq!(x, "red"),
                _ => panic!(),
            }
        }

        // Check old segment data is removed from disk
        assert!(!original_segment_path.exists());
    }
}
