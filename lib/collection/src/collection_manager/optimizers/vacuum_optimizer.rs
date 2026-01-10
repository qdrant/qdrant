use std::path::{Path, PathBuf};
use std::sync::Arc;

use itertools::Itertools;
use ordered_float::OrderedFloat;
use parking_lot::Mutex;
use segment::common::operation_time_statistics::OperationDurationsAggregator;
use segment::entry::entry_point::SegmentEntry;
use segment::index::VectorIndex;
use segment::segment::Segment;
use segment::types::{HnswConfig, HnswGlobalConfig, QuantizationConfig};
use segment::vector_storage::VectorStorage;

use crate::collection_manager::optimizers::segment_optimizer::{
    OptimizationPlanner, OptimizerThresholds, SegmentOptimizer,
};
use crate::config::CollectionParams;

/// Optimizer which looks for segments with high amount of soft-deleted points or vectors
///
/// Since the creation of a segment, a lot of points or vectors may have been soft-deleted. This
/// results in the index slowly breaking apart, and unnecessary storage usage.
///
/// This optimizer will look for the worst segment to rebuilt the index and minimize storage usage.
pub struct VacuumOptimizer {
    deleted_threshold: f64,
    min_vectors_number: usize,
    thresholds_config: OptimizerThresholds,
    segments_path: PathBuf,
    collection_temp_dir: PathBuf,
    collection_params: CollectionParams,
    hnsw_config: HnswConfig,
    hnsw_global_config: HnswGlobalConfig,
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
        hnsw_global_config: HnswGlobalConfig,
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
            hnsw_global_config,
            telemetry_durations_aggregator: OperationDurationsAggregator::new(),
        }
    }

    /// Calculate littered ratio for segment on point level
    ///
    /// Returns `None` if littered ratio did not reach vacuum thresholds.
    fn littered_ratio_segment(&self, segment: &Segment) -> Option<f64> {
        let littered_ratio =
            segment.deleted_point_count() as f64 / segment.total_point_count() as f64;
        let is_big = segment.total_point_count() >= self.min_vectors_number;
        let is_littered = littered_ratio > self.deleted_threshold;

        (is_big && is_littered).then_some(littered_ratio)
    }

    /// Calculate littered ratio for segment on vector index level
    ///
    /// If a segment has multiple named vectors, it checks each one.
    /// We are only interested in indexed vectors, as they are the ones affected by soft-deletes.
    ///
    /// This finds the maximum deletion ratio for a named vector. The ratio is based on the number
    /// of deleted vectors versus the number of indexed vector.s
    ///
    /// Returns `None` if littered ratio did not reach vacuum thresholds for no named vectors.
    fn littered_vectors_index_ratio(&self, segment: &Segment) -> Option<f64> {
        // Segment must have any index
        let segment_config = segment.config();
        if !segment_config.is_any_vector_indexed() {
            return None;
        }

        // In this segment, check the index of each named vector for a high deletion ratio.
        // Return the worst ratio.
        segment
            .vector_data
            .values()
            .filter(|vector_data| vector_data.vector_index.borrow().is_index())
            .filter_map(|vector_data| {
                // We use the number of now available vectors against the number of indexed vectors
                // to determine how many are soft-deleted from the index.
                let vector_index = vector_data.vector_index.borrow();
                let vector_storage = vector_data.vector_storage.borrow();
                let indexed_vector_count = vector_index.indexed_vector_count();
                let deleted_from_index =
                    indexed_vector_count.saturating_sub(vector_storage.available_vector_count());
                let deleted_ratio = if indexed_vector_count != 0 {
                    deleted_from_index as f64 / indexed_vector_count as f64
                } else {
                    0.0
                };

                let reached_minimum = deleted_from_index >= self.min_vectors_number;
                let reached_ratio = deleted_ratio > self.deleted_threshold;
                (reached_minimum && reached_ratio).then_some(deleted_ratio)
            })
            .max_by_key(|ratio| OrderedFloat(*ratio))
    }
}

impl SegmentOptimizer for VacuumOptimizer {
    fn name(&self) -> &str {
        "vacuum"
    }

    fn segments_path(&self) -> &Path {
        self.segments_path.as_path()
    }

    fn temp_path(&self) -> &Path {
        self.collection_temp_dir.as_path()
    }

    fn collection_params(&self) -> CollectionParams {
        self.collection_params.clone()
    }

    fn hnsw_config(&self) -> &HnswConfig {
        &self.hnsw_config
    }

    fn hnsw_global_config(&self) -> &HnswGlobalConfig {
        &self.hnsw_global_config
    }

    fn quantization_config(&self) -> Option<QuantizationConfig> {
        self.quantization_config.clone()
    }

    fn threshold_config(&self) -> &OptimizerThresholds {
        &self.thresholds_config
    }

    fn plan_optimizations(&self, planner: &mut OptimizationPlanner) {
        let to_optimize = planner
            .remaining()
            .iter()
            .filter_map(|(&segment_id, segment)| {
                let segment = segment.read();
                let littered_ratio_segment = self.littered_ratio_segment(&segment);
                let littered_ratio_vectors = self.littered_vectors_index_ratio(&segment);
                let worst_ratio = std::iter::chain(littered_ratio_segment, littered_ratio_vectors)
                    .max_by_key(|ratio| OrderedFloat(*ratio));
                worst_ratio.map(|ratio| (segment_id, ratio))
            })
            .sorted_by_key(|(_, ratio)| OrderedFloat(-ratio))
            .collect_vec();
        for (segment_id, _) in to_optimize {
            planner.plan(vec![segment_id]);
        }
    }

    fn get_telemetry_counter(&self) -> &Mutex<OperationDurationsAggregator> {
        &self.telemetry_durations_aggregator
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use common::budget::ResourceBudget;
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::progress_tracker::ProgressTracker;
    use itertools::Itertools;
    use parking_lot::RwLock;
    use segment::entry::entry_point::SegmentEntry;
    use segment::index::hnsw_index::num_rayon_threads;
    use segment::payload_json;
    use segment::types::{Distance, PayloadContainer, PayloadSchemaType, VectorName};
    use serde_json::Value;
    use shard::locked_segment::LockedSegment;
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::{random_multi_vec_segment, random_segment};
    use crate::collection_manager::holders::segment_holder::SegmentHolder;
    use crate::collection_manager::optimizers::indexing_optimizer::IndexingOptimizer;
    use crate::operations::types::VectorsConfig;
    use crate::operations::vector_params_builder::VectorParamsBuilder;

    const VECTOR1_NAME: &VectorName = "vector1";
    const VECTOR2_NAME: &VectorName = "vector2";

    #[test]
    fn test_vacuum_conditions() {
        let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let mut holder = SegmentHolder::default();
        let segment_id = holder.add_new(random_segment(dir.path(), 100, 200, 4));

        let segment = holder.get(segment_id).unwrap();

        let hw_counter = HardwareCounterCell::new();

        let original_segment_path = match segment {
            LockedSegment::Original(s) => s.read().segment_path.clone(),
            LockedSegment::Proxy(_) => panic!("Not expected"),
        };

        let segment_points_to_delete = segment
            .get()
            .read()
            .iter_points()
            .enumerate()
            .filter_map(|(i, point_id)| (i % 2 == 0).then_some(point_id))
            .collect_vec();

        for &point_id in &segment_points_to_delete {
            segment
                .get()
                .write()
                .delete_point(101, point_id, &hw_counter)
                .unwrap();
        }

        let segment_points_to_assign1 = segment
            .get()
            .read()
            .iter_points()
            .enumerate()
            .filter_map(|(i, point_id)| (i % 20 == 0).then_some(point_id))
            .collect_vec();

        let segment_points_to_assign2 = segment
            .get()
            .read()
            .iter_points()
            .enumerate()
            .filter_map(|(i, point_id)| (i % 20 == 0).then_some(point_id))
            .collect_vec();

        for &point_id in &segment_points_to_assign1 {
            segment
                .get()
                .write()
                .set_payload(
                    102,
                    point_id,
                    &payload_json! {"color": "red"},
                    &None,
                    &hw_counter,
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
                    &payload_json! {"size": 0.42},
                    &None,
                    &hw_counter,
                )
                .unwrap();
        }

        let locked_holder: Arc<RwLock<_>> = Arc::new(RwLock::new(holder));

        let vacuum_optimizer = VacuumOptimizer::new(
            0.2,
            50,
            OptimizerThresholds {
                max_segment_size_kb: 1000000,
                memmap_threshold_kb: 1000000,
                indexing_threshold_kb: 1000000,
            },
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            CollectionParams {
                vectors: VectorsConfig::Single(VectorParamsBuilder::new(4, Distance::Dot).build()),
                ..CollectionParams::empty()
            },
            Default::default(),
            HnswGlobalConfig::default(),
            Default::default(),
        );

        let suggested_to_optimize = vacuum_optimizer.plan_optimizations_for_test(&locked_holder);
        let suggested_to_optimize = suggested_to_optimize.into_iter().exactly_one().unwrap();

        // Check that only one segment is selected for optimization
        assert_eq!(suggested_to_optimize.len(), 1);

        let permit_cpu_count = num_rayon_threads(0);
        let budget = ResourceBudget::new(permit_cpu_count, permit_cpu_count);
        let permit = budget.try_acquire(0, permit_cpu_count).unwrap();

        vacuum_optimizer
            .optimize(
                locked_holder.clone(),
                suggested_to_optimize,
                permit,
                budget.clone(),
                &AtomicBool::new(false),
                ProgressTracker::new_for_test(),
                Box::new(|| ()),
            )
            .unwrap();

        let after_optimization_segments = locked_holder.read().iter().map(|(x, _)| x).collect_vec();

        // Check only one new segment
        assert_eq!(after_optimization_segments.len(), 1);

        let optimized_segment_id = *after_optimization_segments.first().unwrap();

        let holder_guard = locked_holder.read();
        let optimized_segment = holder_guard.get(optimized_segment_id).unwrap();
        let segment_arc = optimized_segment.get();
        let segment_guard = segment_arc.read();

        // Check new segment have proper amount of points
        assert_eq!(
            segment_guard.available_point_count(),
            200 - segment_points_to_delete.len(),
        );

        // Check payload is preserved in optimized segment
        for &point_id in &segment_points_to_assign1 {
            assert!(segment_guard.has_point(point_id));
            let payload = segment_guard.payload(point_id, &hw_counter).unwrap();
            let payload_color = payload
                .get_value(&"color".parse().unwrap())
                .into_iter()
                .next()
                .unwrap();

            match payload_color {
                Value::String(x) => assert_eq!(x, "red"),
                _ => panic!(),
            }
        }

        // Check old segment data is removed from disk
        assert!(!original_segment_path.exists());
    }

    /// This tests the vacuum optimizer when many vectors get deleted.
    ///
    /// It tests whether:
    /// - the condition check for deleted vectors work
    /// - optimized segments (and vector storages) are properly rebuilt
    ///
    /// In short, this is what happens in this test:
    /// - create randomized multi vector segment as base
    /// - use indexing optimizer to build index for our segment
    /// - test vacuum deleted vectors condition: should not trigger yet
    /// - delete many points and vectors
    /// - assert deletions are stored properly
    /// - test vacuum deleted vectors condition: should trigger due to deletions
    /// - optimize segment with vacuum optimizer
    /// - assert segment is properly optimized
    #[test]
    fn test_vacuum_deleted_vectors() {
        // Collection configuration
        let (point_count, vector1_dim, vector2_dim) = (1000, 10, 20);
        let thresholds_config = OptimizerThresholds {
            max_segment_size_kb: usize::MAX,
            memmap_threshold_kb: usize::MAX,
            indexing_threshold_kb: 10,
        };
        let collection_params = CollectionParams {
            vectors: VectorsConfig::Multi(BTreeMap::from([
                (
                    VECTOR1_NAME.to_owned(),
                    VectorParamsBuilder::new(vector1_dim, Distance::Dot).build(),
                ),
                (
                    VECTOR2_NAME.to_owned(),
                    VectorParamsBuilder::new(vector2_dim, Distance::Dot).build(),
                ),
            ])),
            ..CollectionParams::empty()
        };

        // Base segment
        let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let mut holder = SegmentHolder::default();

        let mut segment = random_multi_vec_segment(
            dir.path(),
            100,
            point_count,
            vector1_dim as usize,
            vector2_dim as usize,
        );

        let hw_counter = HardwareCounterCell::new();

        segment
            .create_field_index(
                101,
                &"keyword".parse().unwrap(),
                Some(&PayloadSchemaType::Keyword.into()),
                &hw_counter,
            )
            .unwrap();

        let mut segment_id = holder.add_new(segment);
        let locked_holder: Arc<RwLock<_>> = Arc::new(RwLock::new(holder));

        let hnsw_config = HnswConfig {
            m: 16,
            ef_construct: 100,
            full_scan_threshold: 10, // Force to build HNSW links for payload
            max_indexing_threads: 0,
            on_disk: None,
            payload_m: None,
            inline_storage: None,
        };

        let permit_cpu_count = num_rayon_threads(hnsw_config.max_indexing_threads);
        let budget = ResourceBudget::new(permit_cpu_count, permit_cpu_count);
        let permit = budget.try_acquire(0, permit_cpu_count).unwrap();

        // Optimizers used in test
        let index_optimizer = IndexingOptimizer::new(
            2,
            thresholds_config,
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            collection_params.clone(),
            hnsw_config,
            HnswGlobalConfig::default(),
            Default::default(),
        );
        let vacuum_optimizer = VacuumOptimizer::new(
            0.2,
            5,
            thresholds_config,
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            collection_params,
            hnsw_config,
            HnswGlobalConfig::default(),
            Default::default(),
        );

        // Use indexing optimizer to build index for vacuum index test
        let changed = index_optimizer
            .optimize(
                locked_holder.clone(),
                vec![segment_id],
                permit,
                budget.clone(),
                &false.into(),
                ProgressTracker::new_for_test(),
                Box::new(|| ()),
            )
            .unwrap();
        assert!(changed > 0, "optimizer should have rebuilt this segment");
        assert!(
            locked_holder.read().get(segment_id).is_none(),
            "optimized segment should be gone",
        );
        assert_eq!(locked_holder.read().len(), 2, "index must be built");

        // Update working segment ID
        segment_id = locked_holder
            .read()
            .iter_original()
            .find(|(_, segment)| segment.read().total_point_count() > 0)
            .unwrap()
            .0;

        // Vacuum optimizer should not optimize yet, no points/vectors have been deleted
        let suggested_to_optimize = vacuum_optimizer.plan_optimizations_for_test(&locked_holder);
        assert_eq!(suggested_to_optimize.len(), 0);

        // Delete some points and vectors
        {
            let holder = locked_holder.write();
            let segment = holder.get(segment_id).unwrap();
            let mut segment = match segment {
                LockedSegment::Original(s) => s.write(),
                LockedSegment::Proxy(_) => unreachable!(),
            };

            // Delete 10% of points
            let segment_points_to_delete = segment
                .iter_points()
                .enumerate()
                .filter_map(|(i, point_id)| (i % 10 == 3).then_some(point_id))
                .collect_vec();
            for &point_id in &segment_points_to_delete {
                segment.delete_point(201, point_id, &hw_counter).unwrap();
            }

            // Delete 25% of vectors named vector1
            {
                let id_tracker = segment.id_tracker.clone();
                let vector1_data = segment.vector_data.get_mut(VECTOR1_NAME).unwrap();
                let mut vector1_storage = vector1_data.vector_storage.borrow_mut();

                let vector1_vecs_to_delete = id_tracker
                    .borrow()
                    .iter_external()
                    .enumerate()
                    .filter_map(|(i, point_id)| (i % 4 == 0).then_some(point_id))
                    .collect_vec();
                for &point_id in &vector1_vecs_to_delete {
                    let id = id_tracker.borrow().internal_id(point_id).unwrap();
                    vector1_storage.delete_vector(id).unwrap();
                }
            }

            // Delete 10% of vectors named vector2
            {
                let id_tracker = segment.id_tracker.clone();
                let vector2_data = segment.vector_data.get_mut(VECTOR2_NAME).unwrap();
                let mut vector2_storage = vector2_data.vector_storage.borrow_mut();

                let vector2_vecs_to_delete = id_tracker
                    .borrow()
                    .iter_external()
                    .enumerate()
                    .filter_map(|(i, point_id)| (i % 10 == 7).then_some(point_id))
                    .collect_vec();
                for &point_id in &vector2_vecs_to_delete {
                    let id = id_tracker.borrow().internal_id(point_id).unwrap();
                    vector2_storage.delete_vector(id).unwrap();
                }
            }
        }

        // Ensure deleted points and vectors are stored properly before optimizing
        locked_holder
            .read()
            .iter_original()
            .map(|(_, segment)| segment.read())
            .filter(|segment| segment.total_point_count() > 0)
            .for_each(|segment| {
                // We should still have all points
                assert_eq!(segment.total_point_count(), point_count as usize);

                // Named vector storages should have deletions, but not at creation
                segment.vector_data.values().for_each(|vector_data| {
                    let vector_storage = vector_data.vector_storage.borrow();
                    assert!(vector_storage.deleted_vector_count() > 0);
                });
            });

        // Run vacuum index optimizer, make sure it optimizes properly
        let permit = budget.try_acquire(0, permit_cpu_count).unwrap();
        let suggested_to_optimize = vacuum_optimizer.plan_optimizations_for_test(&locked_holder);
        let suggested_to_optimize = suggested_to_optimize.into_iter().exactly_one().unwrap();
        assert_eq!(suggested_to_optimize.len(), 1);
        let changed = vacuum_optimizer
            .optimize(
                locked_holder.clone(),
                suggested_to_optimize,
                permit,
                budget.clone(),
                &false.into(),
                ProgressTracker::new_for_test(),
                Box::new(|| ()),
            )
            .unwrap();
        assert!(changed > 0, "optimizer should have rebuilt this segment");

        // Ensure deleted points and vectors are optimized
        locked_holder
            .read()
            .iter_original()
            .map(|(_, segment)| segment.read())
            .filter(|segment| segment.total_point_count() > 0)
            .for_each(|segment| {
                // We should have deleted some points
                assert!(segment.total_point_count() < point_count as usize);

                // Named vector storages should have:
                // - deleted vectors
                // - indexed vectors (more than 0)
                // - indexed less vectors than the total available
                segment.vector_data.values().for_each(|vector_data| {
                    let vector_index = vector_data.vector_index.borrow();
                    let vector_storage = vector_data.vector_storage.borrow();

                    assert!(vector_storage.available_vector_count() > 0);
                    assert!(vector_storage.deleted_vector_count() > 0);
                    assert_eq!(
                        vector_index.indexed_vector_count(),
                        vector_storage.available_vector_count(),
                    );
                });
            });
    }
}
