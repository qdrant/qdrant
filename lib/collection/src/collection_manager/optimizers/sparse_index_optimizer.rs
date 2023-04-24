use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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

/// Looks for segments with vector storage that has a high disbalance of index and available
/// vectors
///
/// Since the index for a vector storage has been created, a lot of vectors in it may have been
/// deleted. That results in the index slowly breaking apart. This will find indexes with a high
/// ratio of deletions in it, and will schedule the segment to re-create to rebuild the index more
/// efficiently.
///
/// The process of index creation is slow and CPU-bounded, so it is convenient to perform
/// index building in a same way as segment re-creation.
pub struct SparseIndexOptimizer {
    /// Required minimum ratio of deleted / total vectors in a vector storage index to start reindexing.
    deleted_threshold: f64,
    /// Required minimum number of deleted vectors in a vector storage index to start reindexing.
    min_vectors_number: usize,
    thresholds_config: OptimizerThresholds,
    segments_path: PathBuf,
    collection_temp_dir: PathBuf,
    collection_params: CollectionParams,
    hnsw_config: HnswConfig,
    quantization_config: Option<QuantizationConfig>,
    telemetry_durations_aggregator: Arc<Mutex<OperationDurationsAggregator>>,
}

impl SparseIndexOptimizer {
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
        SparseIndexOptimizer {
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
    ) -> Vec<SegmentId> {
        let segments_read_guard = segments.read();
        let candidate = segments_read_guard
            .iter()
            .filter(|(idx, _segment)| !excluded_ids.contains(idx))
            .filter_map(|(idx, segment)| {
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
                    LockedSegment::Original(segment) => segment,
                    LockedSegment::Proxy(_) => return None,
                };

                // In this segment, check the index of each vector storage for a high deletion
                // count (sparse index). Count all deleted vectors of those that reach a threshold.
                let read_real_segment = real_segment.read();
                let deleted_vector_count = read_real_segment
                    .vector_data
                    .values()
                    .filter(|vector_data| vector_data.vector_index.borrow().is_index())
                    .filter_map(|vector_data| {
                        // We asume the storage and its index are created at the same time. We
                        // therefore know the number of deleted vectors then and now. Based on that we
                        // can determine what ratio of points from the index is deleted.
                        // TODO: problem: point/vector deletes overlap
                        let vector_storage = vector_data.vector_storage.borrow();
                        let create_deleted_vector_count =
                            vector_storage.create_deleted_vector_count();
                        let full_index_count =
                            vector_storage.total_vector_count() - create_deleted_vector_count;
                        let deleted_index_count =
                            vector_storage.deleted_vector_count() - create_deleted_vector_count;
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

                (deleted_vector_count > 0).then_some((*idx, deleted_vector_count))
            })
            .max_by_key(|(_, vector_size)| *vector_size)
            .map(|(idx, _)| idx);
        Vec::from_iter(candidate)
    }
}

impl SegmentOptimizer for SparseIndexOptimizer {
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
        self.worst_segment(segments, excluded_ids)
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
    use std::sync::Arc;

    use itertools::Itertools;
    use parking_lot::RwLock;
    use rand::Rng;
    use segment::entry::entry_point::SegmentEntry;
    use segment::types::Distance;
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::random_multi_vec_segment;
    use crate::collection_manager::holders::segment_holder::SegmentHolder;
    use crate::collection_manager::optimizers::indexing_optimizer::IndexingOptimizer;
    use crate::operations::types::{VectorParams, VectorsConfig};

    /// This tests the sparse index optimizer.
    ///
    /// It tests whether:
    /// - the condition check for the sparde index optimizer works
    /// - optimized segments (and vector storages) are properly rebuilt
    ///
    /// In short, this is what happens in this test:
    /// - create randomized multi vector segment as base
    /// - use indexing optimizer to build index for our segment
    /// - test sparse index condition: should not trigger yet
    /// - delete many points and vectors
    /// - assert deletions are stored properly
    /// - test sparse index condition: should trigger due to deletions
    /// - optimize segment with sparse index optimizer
    /// - assert segment is properly optimized
    #[test]
    fn test_sparse_index_optimizer() {
        // Collection configuration
        let (point_count, vector1_dim, vector2_dim) = (500, 10, 20);
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
        let sparse_index_optimizer = SparseIndexOptimizer::new(
            0.5,
            5,
            thresholds_config,
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            collection_params,
            Default::default(),
            Default::default(),
        );

        // Use indexing optimizer to build index for sparse index test
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

        // Sparse index optimizer should not optimize yet, no points/vectors have been deleted
        let suggested_to_optimize =
            sparse_index_optimizer.check_condition(locked_holder.clone(), &Default::default());
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

        // Run sparse index optimizer, make sure it optimizes properly
        let suggested_to_optimize =
            sparse_index_optimizer.check_condition(locked_holder.clone(), &Default::default());
        assert_eq!(suggested_to_optimize.len(), 1);
        let changed = sparse_index_optimizer
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
