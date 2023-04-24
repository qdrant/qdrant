use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ordered_float::OrderedFloat;
use parking_lot::Mutex;
use segment::common::operation_time_statistics::{
    OperationDurationStatistics, OperationDurationsAggregator,
};
use segment::types::{HnswConfig, QuantizationConfig, SegmentType};
use segment::vector_storage::VectorStorage;

use crate::collection_manager::holders::segment_holder::{
    LockedSegment, LockedSegmentHolder, SegmentId,
};
use crate::collection_manager::optimizers::segment_optimizer::{
    OptimizerThresholds, SegmentOptimizer,
};
use crate::config::CollectionParams;

/// Optimizer which looks for segments with high amount of soft-deleted points or vectors
///
/// Since the creation of a segment, a lot of points or vectors may have been soft-deleted. This
/// results in the index slowly breaking appart, and unnecessary storage usage.
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
            .filter(|(idx, _segment)| !excluded_ids.contains(idx))
            .flat_map(|(idx, segment)| {
                // Calcualte littered ratio for segment and named vectors
                let littered_ratio_segment = self.littered_ratio_segment(segment);
                let littered_ratio_vectors = self.littered_ratio_vectors(segment);
                [littered_ratio_segment, littered_ratio_vectors]
                    .into_iter()
                    .flatten()
                    .map(|ratio| (*idx, ratio))
            })
            .max_by_key(|(_, ratio)| OrderedFloat(*ratio))
            .map(|(idx, _)| (idx, segments_read_guard.get(idx).unwrap().clone()))
    }

    /// Calculate littered ratio for segment on point level
    ///
    /// Returns `None` if littered ratio did not reach vacuum thresholds.
    fn littered_ratio_segment(&self, segment: &LockedSegment) -> Option<f64> {
        let segment_entry = segment.get();
        let read_segment = segment_entry.read();

        // Never optimize special segments
        if read_segment.segment_type() == SegmentType::Special {
            return None;
        }

        let littered_ratio =
            read_segment.deleted_point_count() as f64 / read_segment.total_point_count() as f64;
        let is_big = read_segment.total_point_count() >= self.min_vectors_number;
        let is_littered = littered_ratio > self.deleted_threshold;

        (is_big && is_littered).then_some(littered_ratio)
    }

    /// Calculate littered ratio for segment on vector level
    ///
    /// If a segment has multiple named vectors, it checks each one.
    ///
    /// The ratio is based on the number of deleted vectors in each named vector in which the
    /// vacuum threshold is reached.
    ///
    /// Returns `None` if littered ratio did not reach vacuum thresholds for no named vectors.
    fn littered_ratio_vectors(&self, segment: &LockedSegment) -> Option<f64> {
        let segment_entry = segment.get();
        let read_segment = segment_entry.read();

        // Never optimize special segments
        if read_segment.segment_type() == SegmentType::Special {
            return None;
        }

        // Segment must have an index
        let segment_config = read_segment.config();
        if !segment_config.is_vector_indexed() {
            return None;
        }

        // We can only work with original segments
        let real_segment = match segment {
            LockedSegment::Original(segment) => segment.read(),
            LockedSegment::Proxy(_) => return None,
        };
        let total_vector_count = real_segment
            .vector_data
            .values()
            .map(|vector_data| vector_data.vector_storage.borrow().total_vector_count())
            .sum::<usize>();
        if total_vector_count == 0 {
            return None;
        }

        // In this segment, check the index of each named vector for a high deletion count.
        // Count all deleted vectors of those that reach a threshold.
        let deleted_vector_count = real_segment
            .vector_data
            .values()
            .filter(|vector_data| vector_data.vector_index.borrow().is_index())
            .filter_map(|vector_data| {
                // We asume the storage and its index are created at the same time. We
                // therefore know the number of deleted vectors then and now. Based on that we
                // can determine what ratio of points from the index is deleted.
                let vector_storage = vector_data.vector_storage.borrow();
                let create_deleted_vec_count = vector_storage.create_deleted_vector_count();
                let full_index_count =
                    vector_storage.total_vector_count() - create_deleted_vec_count;
                let deleted_index_count =
                    vector_storage.deleted_vector_count() - create_deleted_vec_count;
                let deleted_ratio = if full_index_count != 0 {
                    deleted_index_count as f64 / full_index_count as f64
                } else {
                    0.0
                };

                let reached_minimum = deleted_index_count >= self.min_vectors_number;
                let reached_ratio = deleted_ratio > self.deleted_threshold;
                (reached_minimum && reached_ratio).then_some(deleted_index_count)
            })
            .sum::<usize>();

        (deleted_vector_count > 0)
            .then_some(deleted_vector_count as f64 / total_vector_count as f64)
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
    use std::collections::BTreeMap;
    use std::num::{NonZeroU32, NonZeroU64};
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use itertools::Itertools;
    use parking_lot::RwLock;
    use rand::Rng;
    use segment::entry::entry_point::SegmentEntry;
    use segment::types::Distance;
    use serde_json::{json, Value};
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::{random_multi_vec_segment, random_segment};
    use crate::collection_manager::holders::segment_holder::SegmentHolder;
    use crate::collection_manager::optimizers::indexing_optimizer::IndexingOptimizer;
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
            segment_guard.total_point_count(),
            200 - segment_points_to_delete.len(),
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
        let (point_count, vector1_dim, vector2_dim) = (300, 4, 6);
        let thresholds_config = OptimizerThresholds {
            max_segment_size: std::usize::MAX,
            memmap_threshold: std::usize::MAX,
            indexing_threshold: 10,
        };
        let collection_params = CollectionParams {
            vectors: VectorsConfig::Multi(BTreeMap::from([
                (
                    "vector1".into(),
                    VectorParams {
                        size: vector1_dim.try_into().unwrap(),
                        distance: Distance::Dot,
                        hnsw_config: None,
                        quantization_config: None,
                    },
                ),
                (
                    "vector2".into(),
                    VectorParams {
                        size: vector2_dim.try_into().unwrap(),
                        distance: Distance::Dot,
                        hnsw_config: None,
                        quantization_config: None,
                    },
                ),
            ])),
            shard_number: 1.try_into().unwrap(),
            on_disk_payload: false,
            replication_factor: 1.try_into().unwrap(),
            write_consistency_factor: 1.try_into().unwrap(),
        };

        // Base segment
        let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let mut holder = SegmentHolder::default();
        let mut segment_id = holder.add(random_multi_vec_segment(
            dir.path(),
            100,
            point_count,
            vector1_dim as usize,
            vector2_dim as usize,
        ));
        let locked_holder: Arc<RwLock<_>> = Arc::new(RwLock::new(holder));

        // Optimizers used in test
        let index_optimizer = IndexingOptimizer::new(
            thresholds_config.clone(),
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            collection_params.clone(),
            Default::default(),
            Default::default(),
        );
        let vacuum_optimizer = VacuumOptimizer::new(
            0.5,
            5,
            thresholds_config,
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            collection_params,
            Default::default(),
            Default::default(),
        );

        // Use indexing optimizer to build index for vacuum index test
        let changed = index_optimizer
            .optimize(locked_holder.clone(), vec![segment_id], &false.into())
            .unwrap();
        assert!(changed, "optimizer should have rebuilt this segment");
        assert!(
            locked_holder.read().get(segment_id).is_none(),
            "optimized segment should be gone",
        );
        assert_eq!(locked_holder.read().len(), 2, "index must be built");

        // Update working segment ID
        segment_id = *locked_holder
            .read()
            .iter()
            .find(|(_, segment)| {
                let segment = match segment {
                    LockedSegment::Original(s) => s.read(),
                    LockedSegment::Proxy(_) => unreachable!(),
                };
                segment.total_point_count() > 0
            })
            .unwrap()
            .0;

        // Vacuum optimizer should not optimize yet, no points/vectors have been deleted
        let suggested_to_optimize =
            vacuum_optimizer.check_condition(locked_holder.clone(), &Default::default());
        assert_eq!(suggested_to_optimize.len(), 0);

        // Delete some points and vectors
        {
            let holder = locked_holder.write();
            let segment = holder.get(segment_id).unwrap();
            let mut rnd = rand::thread_rng();
            let mut segment = match segment {
                LockedSegment::Original(s) => s.write(),
                LockedSegment::Proxy(_) => unreachable!(),
            };

            // Delete 15% of points
            let segment_points_to_delete = segment
                .iter_points()
                .filter(|_| rnd.gen_bool(0.15))
                .collect_vec();
            for &point_id in &segment_points_to_delete {
                segment.delete_point(101, point_id).unwrap();
            }

            // Delete 80% of vectors named vector1
            {
                let id_tracker = segment.id_tracker.clone();
                let vector1_data = segment.vector_data.get_mut("vector1").unwrap();
                let mut vector1_storage = vector1_data.vector_storage.borrow_mut();

                let vector1_vecs_to_delete = id_tracker
                    .borrow()
                    .iter_external()
                    .filter(|_| rnd.gen_bool(0.8))
                    .collect_vec();
                for &point_id in &vector1_vecs_to_delete {
                    let id = id_tracker.borrow().internal_id(point_id).unwrap();
                    vector1_storage.delete_vector(id).unwrap();
                }
            }

            // Delete 10% of vectors named vector2
            {
                let id_tracker = segment.id_tracker.clone();
                let vector2_data = segment.vector_data.get_mut("vector2").unwrap();
                let mut vector2_storage = vector2_data.vector_storage.borrow_mut();

                let vector2_vecs_to_delete = id_tracker
                    .borrow()
                    .iter_external()
                    .filter(|_| rnd.gen_bool(0.1))
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
            .iter()
            .map(|(_, segment)| match segment {
                LockedSegment::Original(s) => s.read(),
                LockedSegment::Proxy(_) => unreachable!(),
            })
            .filter(|segment| segment.total_point_count() > 0)
            .for_each(|segment| {
                // We should still have all points
                assert_eq!(segment.total_point_count(), point_count as usize);

                // Named vector storages should have deletions, but not at creation
                segment.vector_data.values().for_each(|vector_data| {
                    let vector_storage = vector_data.vector_storage.borrow();
                    assert_eq!(vector_storage.create_deleted_vector_count(), 0);
                    assert!(vector_storage.deleted_vector_count() > 0);
                });
            });

        // Run vacuum index optimizer, make sure it optimizes properly
        let suggested_to_optimize =
            vacuum_optimizer.check_condition(locked_holder.clone(), &Default::default());
        assert_eq!(suggested_to_optimize.len(), 1);
        let changed = vacuum_optimizer
            .optimize(locked_holder.clone(), suggested_to_optimize, &false.into())
            .unwrap();
        assert!(changed, "optimizer should have rebuilt this segment");

        // Ensure deleted points and vectors are optimized
        locked_holder
            .read()
            .iter()
            .map(|(_, segment)| match segment {
                LockedSegment::Original(s) => s.read(),
                LockedSegment::Proxy(_) => unreachable!(),
            })
            .filter(|segment| segment.total_point_count() > 0)
            .for_each(|segment| {
                // We should have deleted some points
                assert!(segment.total_point_count() < point_count as usize);

                // Named vector storages should have deletions at creation
                segment.vector_data.values().for_each(|vector_data| {
                    let vector_storage = vector_data.vector_storage.borrow();
                    assert!(vector_storage.create_deleted_vector_count() > 0);
                    assert_eq!(
                        vector_storage.create_deleted_vector_count(),
                        vector_storage.deleted_vector_count()
                    );
                });
            });
    }
}
