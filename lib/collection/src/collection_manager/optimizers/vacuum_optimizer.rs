/// Optimizer which looks for segments with high amount of soft-deleted points or vectors
///
/// Since the creation of a segment, a lot of points or vectors may have been soft-deleted. This
/// results in the index slowly breaking apart, and unnecessary storage usage.
///
/// This optimizer will look for the worst segment to rebuilt the index and minimize storage usage.
pub use shard::optimizers::vacuum_optimizer::VacuumOptimizer;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use common::counter::hardware_counter::HardwareCounterCell;
    use itertools::Itertools;
    use segment::entry::entry_point::NonAppendableSegmentEntry as _;
    use segment::id_tracker::IdTracker;
    use segment::index::VectorIndex;
    use segment::payload_json;
    use segment::types::{
        Distance, HnswConfig, HnswGlobalConfig, PayloadContainer, PayloadSchemaType,
        QuantizationConfig, VectorName,
    };
    use segment::vector_storage::VectorStorage;
    use serde_json::Value;
    use shard::locked_segment::LockedSegment;
    use shard::operations::optimization::OptimizerThresholds;
    use shard::optimizers::segment_optimizer::SegmentOptimizer;
    use shard::segment_holder::locked::LockedSegmentHolder;
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::{random_multi_vec_segment, random_segment};
    use crate::collection_manager::holders::segment_holder::SegmentHolder;
    use crate::collection_manager::optimizers::indexing_optimizer::IndexingOptimizer;
    use crate::config::CollectionParams;
    use crate::operations::types::VectorsConfig;
    use crate::operations::vector_params_builder::VectorParamsBuilder;
    use crate::optimizers_builder::build_segment_optimizer_config;

    const VECTOR1_NAME: &VectorName = "vector1";
    const VECTOR2_NAME: &VectorName = "vector2";

    #[allow(clippy::too_many_arguments)]
    fn new_indexing_optimizer(
        default_segments_number: usize,
        thresholds_config: OptimizerThresholds,
        segments_path: PathBuf,
        collection_temp_dir: PathBuf,
        collection_params: CollectionParams,
        hnsw_config: HnswConfig,
        hnsw_global_config: HnswGlobalConfig,
        quantization_config: Option<QuantizationConfig>,
    ) -> IndexingOptimizer {
        let segment_config =
            build_segment_optimizer_config(&collection_params, &hnsw_config, &quantization_config);
        shard::optimizers::indexing_optimizer::IndexingOptimizer::new(
            default_segments_number,
            thresholds_config,
            segments_path,
            collection_temp_dir,
            segment_config,
            hnsw_config,
            hnsw_global_config,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_vacuum_optimizer(
        deleted_threshold: f64,
        min_vectors_number: usize,
        thresholds_config: OptimizerThresholds,
        segments_path: PathBuf,
        collection_temp_dir: PathBuf,
        collection_params: CollectionParams,
        hnsw_config: HnswConfig,
        hnsw_global_config: HnswGlobalConfig,
        quantization_config: Option<QuantizationConfig>,
    ) -> VacuumOptimizer {
        let segment_config =
            build_segment_optimizer_config(&collection_params, &hnsw_config, &quantization_config);
        shard::optimizers::vacuum_optimizer::VacuumOptimizer::new(
            deleted_threshold,
            min_vectors_number,
            thresholds_config,
            segments_path,
            collection_temp_dir,
            segment_config,
            hnsw_config,
            hnsw_global_config,
        )
    }

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

        let locked_holder = LockedSegmentHolder::new(holder);

        let vacuum_optimizer = new_vacuum_optimizer(
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

        vacuum_optimizer.optimize_for_test(locked_holder.clone(), suggested_to_optimize);

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
        let locked_holder = LockedSegmentHolder::new(holder);

        let hnsw_config = HnswConfig {
            m: 16,
            ef_construct: 100,
            full_scan_threshold: 10, // Force to build HNSW links for payload
            max_indexing_threads: 0,
            on_disk: None,
            payload_m: None,
            inline_storage: None,
        };

        // Optimizers used in test
        let index_optimizer = new_indexing_optimizer(
            2,
            thresholds_config,
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            collection_params.clone(),
            hnsw_config,
            HnswGlobalConfig::default(),
            Default::default(),
        );
        let vacuum_optimizer = new_vacuum_optimizer(
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
        let changed = index_optimizer.optimize_for_test(locked_holder.clone(), vec![segment_id]);
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
        let suggested_to_optimize = vacuum_optimizer.plan_optimizations_for_test(&locked_holder);
        let suggested_to_optimize = suggested_to_optimize.into_iter().exactly_one().unwrap();
        assert_eq!(suggested_to_optimize.len(), 1);
        let changed =
            vacuum_optimizer.optimize_for_test(locked_holder.clone(), suggested_to_optimize);
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
