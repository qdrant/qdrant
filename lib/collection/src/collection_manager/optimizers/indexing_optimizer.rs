// Deprecated storage placement params (`on_disk`, `always_ram`, `on_disk_payload`) are still
// handled here for backward compatibility with the new `memory` parameter
#![allow(deprecated)]

/// Looks for the segments, which require to be indexed.
///
/// If segment is too large, but still does not have indexes - it is time to create some indexes.
/// The process of index creation is slow and CPU-bounded, so it is convenient to perform
/// index building in a same way as segment re-creation.
pub use shard::optimizers::indexing_optimizer::IndexingOptimizer;

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::path::PathBuf;

    use common::counter::hardware_counter::HardwareCounterCell;
    use fs_err as fs;
    use itertools::Itertools;
    use rand::rng;
    use segment::data_types::named_vectors::NamedVectors;
    use segment::data_types::vectors::{
        DEFAULT_VECTOR_NAME, MultiDenseVectorInternal, VectorInternal,
    };
    use segment::entry::ReadSegmentEntry;
    use segment::entry::entry_point::SegmentEntry;
    use segment::fixtures::index_fixtures::random_vector;
    use segment::fixtures::payload_fixtures::random_multi_vector;
    use segment::json_path::JsonPath;
    use segment::payload_json;
    use segment::segment_constructor::build_segment;
    use segment::segment_constructor::simple_segment_constructor::{VECTOR1_NAME, VECTOR2_NAME};
    use segment::types::{
        Distance, HnswConfig, HnswGlobalConfig, Indexes, MultiVectorComparator, MultiVectorConfig,
        PayloadSchemaType, QuantizationConfig, SegmentConfig, SegmentType, VectorDataConfig,
        VectorNameBuf, VectorStorageType,
    };
    use shard::operations::optimization::OptimizerThresholds;
    use shard::optimizers::segment_optimizer::SegmentOptimizer;
    use shard::segment_holder::locked::LockedSegmentHolder;
    use shard::segment_holder::{FlushMode, SegmentId};
    use shard::update::{
        process_field_index_operation, process_payload_operation, process_point_operation,
    };
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::{random_multi_vec_segment, random_segment};
    use crate::collection_manager::holders::segment_holder::SegmentHolder;
    use crate::collection_manager::optimizers::config_mismatch_optimizer::ConfigMismatchOptimizer;
    use crate::config::CollectionParams;
    use crate::operations::point_ops::{
        BatchPersisted, BatchVectorStructPersisted, PointInsertOperationsInternal, PointOperations,
    };
    use crate::operations::types::{VectorParams, VectorsConfig};
    use crate::operations::vector_params_builder::VectorParamsBuilder;
    use crate::operations::{CreateIndex, FieldIndexOperations};
    use crate::optimizers_builder::build_segment_optimizer_config;

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
            hnsw_global_config,
        )
    }

    fn new_config_mismatch_optimizer(
        thresholds_config: OptimizerThresholds,
        segments_path: PathBuf,
        collection_temp_dir: PathBuf,
        collection_params: CollectionParams,
        hnsw_config: HnswConfig,
        hnsw_global_config: HnswGlobalConfig,
        quantization_config: Option<QuantizationConfig>,
    ) -> ConfigMismatchOptimizer {
        let segment_config =
            build_segment_optimizer_config(&collection_params, &hnsw_config, &quantization_config);
        shard::optimizers::config_mismatch_optimizer::ConfigMismatchOptimizer::new(
            thresholds_config,
            segments_path,
            collection_temp_dir,
            segment_config,
            hnsw_config,
            hnsw_global_config,
        )
    }

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    fn normalize(segments: Vec<Vec<SegmentId>>) -> Vec<Vec<SegmentId>> {
        segments
            .into_iter()
            .map(|group| group.into_iter().sorted().collect_vec())
            .collect()
    }

    #[test]
    fn test_multi_vector_optimization() {
        init();
        let mut holder = SegmentHolder::default();

        let dim1 = 128;
        let dim2 = 256;

        let segments_dir = Builder::new().prefix("segments_dir").tempdir().unwrap();
        let segments_temp_dir = Builder::new()
            .prefix("segments_temp_dir")
            .tempdir()
            .unwrap();
        let mut opnum = 101..1000000;

        let large_segment =
            random_multi_vec_segment(segments_dir.path(), opnum.next().unwrap(), 200, dim1, dim2);

        let segment_config = large_segment.segment_config.clone();

        let large_segment_id = holder.add_new(large_segment);

        let vectors_config: BTreeMap<VectorNameBuf, VectorParams> = segment_config
            .vector_data
            .iter()
            .map(|(name, params)| {
                (
                    name.to_owned(),
                    VectorParamsBuilder::new(params.size as u64, params.distance).build(),
                )
            })
            .collect();

        let mut index_optimizer = new_indexing_optimizer(
            2,
            OptimizerThresholds {
                max_segment_size_kb: 300,
                memmap_threshold_kb: 1000,
                indexing_threshold_kb: 1000,
                deferred_internal_id: None,
            },
            segments_dir.path().to_owned(),
            segments_temp_dir.path().to_owned(),
            CollectionParams {
                vectors: VectorsConfig::Multi(vectors_config),
                ..CollectionParams::empty()
            },
            Default::default(),
            HnswGlobalConfig::default(),
            Default::default(),
        );
        let locked_holder = LockedSegmentHolder::new(holder);

        let suggested_to_optimize = index_optimizer.plan_optimizations_for_test(&locked_holder);
        assert!(suggested_to_optimize.is_empty());

        index_optimizer
            .threshold_config_mut_for_test()
            .memmap_threshold_kb = 1000;
        index_optimizer
            .threshold_config_mut_for_test()
            .indexing_threshold_kb = 50;

        let suggested_to_optimize = index_optimizer.plan_optimizations_for_test(&locked_holder);
        let suggested_to_optimize = suggested_to_optimize.into_iter().exactly_one().unwrap();
        assert!(suggested_to_optimize.contains(&large_segment_id));

        index_optimizer.optimize_for_test(locked_holder.clone(), suggested_to_optimize);

        let infos = locked_holder
            .read()
            .iter()
            .map(|(_sid, segment)| segment.get().read().info().unwrap())
            .collect_vec();
        let configs = locked_holder
            .read()
            .iter()
            .map(|(_sid, segment)| segment.get().read().config().clone())
            .collect_vec();

        assert_eq!(infos.len(), 2);
        assert_eq!(configs.len(), 2);

        let total_points: usize = infos.iter().map(|info| info.num_points).sum();
        let total_vectors: usize = infos.iter().map(|info| info.num_vectors).sum();
        assert_eq!(total_points, 200);
        assert_eq!(total_vectors, 400);

        for config in configs {
            assert_eq!(config.vector_data.len(), 2);
            assert_eq!(config.vector_data.get(VECTOR1_NAME).unwrap().size, dim1);
            assert_eq!(config.vector_data.get(VECTOR2_NAME).unwrap().size, dim2);
        }
    }

    /// A multivector's deferred-point threshold is computed assuming a fixed inner-vector
    /// count (`MULTIVECTOR_SIZE = 16`, see `CollectionParams::get_deferred_point_id`). This
    /// makes points become "deferred" at a far smaller storage size than the actual data
    /// occupies, so a segment can hold deferred points while staying well below the indexing
    /// threshold.
    ///
    /// Before the fix, the indexing optimizer rebuilt such a segment as a plain (non-HNSW)
    /// segment because it was below the indexing threshold. Plain segments don't promote
    /// deferred points, so `has_deferred_points()` stayed `true` and the optimizer kept
    /// re-selecting the segment forever (infinite loop).
    ///
    /// This test proves the fix: a below-threshold segment with deferred points is optimized
    /// into an HNSW-indexed segment (which promotes the deferred points) and is no longer
    /// selected for optimization afterwards.
    #[test]
    fn test_deferred_points_multivector_optimization() {
        init();

        // Multivector named vector. With float32 elements the per-point size used to derive
        // the deferred-point threshold is `ELEMENT_BYTES * DIM * MULTIVECTOR_SIZE`.
        const VECTOR_NAME: &str = "vector";
        const DIM: usize = 16;
        const MULTIVECTOR_SIZE: usize = 16; // mirrors CollectionParams::get_deferred_point_id
        const ELEMENT_BYTES: usize = 4; // float32

        // Deferred-point byte threshold, sized so points start deferring at internal offset 100.
        let deferred_threshold_bytes =
            NonZeroUsize::new(ELEMENT_BYTES * DIM * MULTIVECTOR_SIZE * 100).unwrap(); // 102_400

        let collection_params = CollectionParams {
            vectors: VectorsConfig::Multi(BTreeMap::from([(
                VECTOR_NAME.to_owned(),
                VectorParams {
                    memory: None,
                    size: NonZeroU64::new(DIM as u64).unwrap(),
                    distance: Distance::Dot,
                    hnsw_config: None,
                    quantization_config: None,
                    on_disk: None,
                    datatype: None,
                    multivector_config: Some(MultiVectorConfig::default()),
                },
            )])),
            ..CollectionParams::empty()
        };

        let hnsw_config = HnswConfig::default();

        // Deferred-point offset, derived exactly as production does it. Because the multivector
        // assumes 16 inner vectors per point, the threshold of 102_400 bytes maps to only 100
        // points (102_400 / (4 * 16 * 16)), even though each point actually stores far less.
        let deferred_internal_id =
            collection_params.get_deferred_point_id(&hnsw_config, Some(deferred_threshold_bytes));
        assert_eq!(
            deferred_internal_id,
            Some(100),
            "points should start deferring at internal offset 100",
        );

        // Build a multivector segment holding deferred points: insert more points than the
        // deferred offset, each with a single inner vector so the actual storage size stays
        // tiny (DIM * ELEMENT_BYTES bytes/point) - far below the indexing threshold.
        let segments_dir = Builder::new().prefix("segments_dir").tempdir().unwrap();
        let segments_temp_dir = Builder::new()
            .prefix("segments_temp_dir")
            .tempdir()
            .unwrap();

        const NUM_POINTS: u64 = 120;
        let segment_config = SegmentConfig {
            vector_data: HashMap::from([(
                VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: DIM,
                    distance: Distance::Dot,
                    storage_type: VectorStorageType::default(),
                    index: Indexes::Plain {},
                    quantization_config: None,
                    multivector_config: Some(MultiVectorConfig::default()),
                    datatype: None,
                },
            )]),
            sparse_vector_data: Default::default(),
            payload_storage_type: Default::default(),
        };

        let (mut segment, _) = build_segment(
            segments_dir.path(),
            &segment_config,
            deferred_internal_id,
            true,
        )
        .unwrap();

        let mut rnd = rng();
        let hw_counter = HardwareCounterCell::new();
        for n in 0..NUM_POINTS {
            let multi_vec = random_multi_vector(&mut rnd, DIM, 1);
            let mut named = NamedVectors::default();
            named.insert(
                VECTOR_NAME.to_owned(),
                VectorInternal::MultiDense(multi_vec),
            );
            segment
                .upsert_point(n, n.into(), named, &hw_counter)
                .unwrap();
        }

        // The segment holds deferred points, yet its vectors stay below the indexing threshold.
        assert!(
            segment.has_deferred_points(),
            "segment should hold deferred points",
        );
        let vectors_size_bytes = segment
            .available_vectors_size_in_bytes(VECTOR_NAME)
            .unwrap();

        let mut holder = SegmentHolder::default();
        let segment_id = holder.add_new(segment);
        let locked_holder = LockedSegmentHolder::new(holder);

        // Indexing threshold set BELOW the deferred byte threshold but ABOVE the actual segment
        // size, so the segment does NOT exceed the indexing threshold by size.
        let indexing_threshold_kb = 50; // 51_200 bytes
        assert!(
            vectors_size_bytes < indexing_threshold_kb * 1024,
            "segment ({vectors_size_bytes} bytes) must stay below the indexing threshold",
        );
        assert!(
            indexing_threshold_kb * 1024 < deferred_threshold_bytes.get(),
            "indexing threshold must be under the deferred-point threshold",
        );

        let index_optimizer = new_indexing_optimizer(
            1,
            OptimizerThresholds {
                max_segment_size_kb: 1000,
                memmap_threshold_kb: 1000,
                indexing_threshold_kb,
                deferred_internal_id,
            },
            segments_dir.path().to_owned(),
            segments_temp_dir.path().to_owned(),
            collection_params,
            hnsw_config,
            HnswGlobalConfig::default(),
            None,
        );

        // The segment is selected for optimization solely because it has deferred points.
        let suggested_to_optimize = index_optimizer.plan_optimizations_for_test(&locked_holder);
        let suggested_to_optimize = suggested_to_optimize.into_iter().exactly_one().unwrap();
        assert!(suggested_to_optimize.contains(&segment_id));

        index_optimizer.optimize_for_test(locked_holder.clone(), suggested_to_optimize);

        // The fix: the optimized segment is HNSW-indexed even though it was below the indexing
        // threshold, which promotes the deferred points.
        let infos = locked_holder
            .read()
            .iter()
            .map(|(_sid, segment)| segment.get().read().info().unwrap())
            .collect_vec();
        assert!(
            infos
                .iter()
                .any(|info| info.segment_type == SegmentType::Indexed),
            "optimized segment must be HNSW-indexed to promote deferred points",
        );

        // No segment holds deferred points anymore, so the optimizer no longer loops on it.
        let still_has_deferred = locked_holder
            .read()
            .iter()
            .any(|(_sid, segment)| segment.get().read().has_deferred_points());
        assert!(
            !still_has_deferred,
            "deferred points must be promoted after optimization",
        );

        let suggested_after = index_optimizer.plan_optimizations_for_test(&locked_holder);
        assert!(
            suggested_after.is_empty(),
            "no further optimization should be required (no infinite loop)",
        );
    }

    #[test]
    fn test_indexing_optimizer() {
        init();

        let mut rng = rng();
        let mut holder = SegmentHolder::default();

        let payload_field: JsonPath = "number".parse().unwrap();

        let dim = 256;

        let segments_dir = Builder::new().prefix("segments_dir").tempdir().unwrap();
        let segments_temp_dir = Builder::new()
            .prefix("segments_temp_dir")
            .tempdir()
            .unwrap();
        let mut opnum = 101..1000000;

        let small_segment = random_segment(segments_dir.path(), opnum.next().unwrap(), 25, dim);
        let middle_low_segment =
            random_segment(segments_dir.path(), opnum.next().unwrap(), 90, dim);
        let middle_segment = random_segment(segments_dir.path(), opnum.next().unwrap(), 100, dim);
        let large_segment = random_segment(segments_dir.path(), opnum.next().unwrap(), 200, dim);

        let segment_config = small_segment.segment_config.clone();

        let small_segment_id = holder.add_new(small_segment);
        let middle_low_segment_id = holder.add_new(middle_low_segment);
        let middle_segment_id = holder.add_new(middle_segment);
        let large_segment_id = holder.add_new(large_segment);

        let mut index_optimizer = new_indexing_optimizer(
            2,
            OptimizerThresholds {
                max_segment_size_kb: 300,
                memmap_threshold_kb: 1000,
                indexing_threshold_kb: 1000,
                deferred_internal_id: None,
            },
            segments_dir.path().to_owned(),
            segments_temp_dir.path().to_owned(),
            CollectionParams {
                vectors: VectorsConfig::Single(
                    VectorParamsBuilder::new(
                        segment_config.vector_data[DEFAULT_VECTOR_NAME].size as u64,
                        segment_config.vector_data[DEFAULT_VECTOR_NAME].distance,
                    )
                    .build(),
                ),
                ..CollectionParams::empty()
            },
            Default::default(),
            HnswGlobalConfig::default(),
            Default::default(),
        );

        let locked_holder = LockedSegmentHolder::new(holder);

        // ---- check condition for MMap optimization
        let suggested_to_optimize = index_optimizer.plan_optimizations_for_test(&locked_holder);
        assert!(suggested_to_optimize.is_empty());

        index_optimizer
            .threshold_config_mut_for_test()
            .memmap_threshold_kb = 1000;
        index_optimizer
            .threshold_config_mut_for_test()
            .indexing_threshold_kb = 50;

        let suggested_to_optimize = index_optimizer.plan_optimizations_for_test(&locked_holder);
        assert_eq!(
            normalize(suggested_to_optimize),
            normalize(vec![
                vec![large_segment_id, middle_low_segment_id],
                vec![middle_segment_id],
            ]),
        );

        index_optimizer
            .threshold_config_mut_for_test()
            .memmap_threshold_kb = 1000;
        index_optimizer
            .threshold_config_mut_for_test()
            .indexing_threshold_kb = 1000;

        let suggested_to_optimize = index_optimizer.plan_optimizations_for_test(&locked_holder);
        assert!(suggested_to_optimize.is_empty());

        index_optimizer
            .threshold_config_mut_for_test()
            .memmap_threshold_kb = 50;
        index_optimizer
            .threshold_config_mut_for_test()
            .indexing_threshold_kb = 1000;

        let suggested_to_optimize = index_optimizer.plan_optimizations_for_test(&locked_holder);
        assert_eq!(
            normalize(suggested_to_optimize),
            normalize(vec![
                vec![large_segment_id, middle_low_segment_id],
                vec![middle_segment_id],
            ]),
        );

        index_optimizer
            .threshold_config_mut_for_test()
            .memmap_threshold_kb = 150;
        index_optimizer
            .threshold_config_mut_for_test()
            .indexing_threshold_kb = 50;

        // ----- CREATE AN INDEXED FIELD ------
        let hw_counter = HardwareCounterCell::new();

        process_field_index_operation(
            &locked_holder.read(),
            opnum.next().unwrap(),
            &FieldIndexOperations::CreateIndex(CreateIndex {
                field_name: payload_field.clone(),
                field_schema: Some(PayloadSchemaType::Integer.into()),
            }),
            &hw_counter,
        )
        .unwrap();

        // ------ Plain -> Mmap & Indexed payload
        let suggested_to_optimize = index_optimizer.plan_optimizations_for_test(&locked_holder);
        assert_eq!(
            normalize(suggested_to_optimize.clone()),
            normalize(vec![
                vec![large_segment_id, middle_low_segment_id],
                vec![middle_segment_id],
            ]),
        );
        index_optimizer.optimize_for_test(locked_holder.clone(), suggested_to_optimize[0].clone());

        // ------ Plain -> Indexed payload
        let suggested_to_optimize = index_optimizer.plan_optimizations_for_test(&locked_holder);
        assert_eq!(suggested_to_optimize.clone(), vec![vec![middle_segment_id]]);
        index_optimizer.optimize_for_test(locked_holder.clone(), suggested_to_optimize[0].clone());

        // ------- Keep smallest segment without changes
        let suggested_to_optimize = index_optimizer.plan_optimizations_for_test(&locked_holder);
        assert!(suggested_to_optimize.is_empty());

        assert_eq!(
            locked_holder.read().len(),
            3,
            "Testing no new segments were created"
        );

        let infos = locked_holder
            .read()
            .iter()
            .map(|(_sid, segment)| segment.get().read().info().unwrap())
            .collect_vec();
        let configs = locked_holder
            .read()
            .iter()
            .map(|(_sid, segment)| segment.get().read().config().clone())
            .collect_vec();

        let indexed_count = infos
            .iter()
            .filter(|info| info.segment_type == SegmentType::Indexed)
            .count();
        assert_eq!(
            indexed_count, 2,
            "Testing that 2 segments are actually indexed"
        );

        let on_disk_count = configs
            .iter()
            .filter(|config| config.is_any_on_disk())
            .count();
        assert_eq!(
            on_disk_count, 1,
            "Testing that only largest segment is not Mmap"
        );

        // Optimizations defer destroying their source segments to a post-flush action; the files
        // are removed once a flush confirms the optimized data is durable (see
        // `SegmentHolder::register_post_flush_action`). Flush to run the action before counting dirs.
        locked_holder
            .read()
            .flush_all(FlushMode::Sync, true)
            .expect("failed to flush segment holder");

        let segment_dirs = fs::read_dir(segments_dir.path()).unwrap().collect_vec();
        assert_eq!(
            segment_dirs.len(),
            locked_holder.read().len(),
            "Testing that new segments are persisted and old data is removed"
        );

        for info in &infos {
            assert!(
                info.index_schema.contains_key(&payload_field),
                "Testing that payload is not lost"
            );
            assert_eq!(
                info.index_schema[&payload_field].data_type,
                PayloadSchemaType::Integer,
                "Testing that payload type is not lost"
            );
        }

        let point_payload = payload_json! {"number": 10000i64};

        let batch = BatchPersisted {
            ids: vec![501.into(), 502.into(), 503.into()],
            vectors: BatchVectorStructPersisted::Single(vec![
                random_vector(&mut rng, dim),
                random_vector(&mut rng, dim),
                random_vector(&mut rng, dim),
            ]),
            payloads: Some(vec![
                Some(point_payload.clone()),
                Some(point_payload.clone()),
                Some(point_payload),
            ]),
        };

        let insert_point_ops =
            PointOperations::UpsertPoints(PointInsertOperationsInternal::from(batch));

        let smallest_size = infos
            .iter()
            .min_by_key(|info| info.num_vectors)
            .unwrap()
            .num_vectors;

        let hw_counter = HardwareCounterCell::new();

        process_point_operation(
            &locked_holder.read(),
            opnum.next().unwrap(),
            insert_point_ops,
            &hw_counter,
        )
        .unwrap();

        let new_infos = locked_holder
            .read()
            .iter()
            .map(|(_sid, segment)| segment.get().read().info().unwrap())
            .collect_vec();
        let new_smallest_size = new_infos
            .iter()
            .min_by_key(|info| info.num_vectors)
            .unwrap()
            .num_vectors;

        assert_eq!(
            new_smallest_size,
            smallest_size + 3,
            "Testing that new data is added to an appendable segment only"
        );

        // ---- New appendable segment should be created if none left

        // Index even the smallest segment
        index_optimizer
            .threshold_config_mut_for_test()
            .indexing_threshold_kb = 20;
        let suggested_to_optimize = index_optimizer.plan_optimizations_for_test(&locked_holder);
        let suggested_to_optimize = suggested_to_optimize.into_iter().exactly_one().unwrap();
        assert!(suggested_to_optimize.contains(&small_segment_id));
        index_optimizer.optimize_for_test(locked_holder.clone(), suggested_to_optimize);

        let new_infos2 = locked_holder
            .read()
            .iter()
            .map(|(_sid, segment)| segment.get().read().info().unwrap())
            .collect_vec();

        let mut has_empty = false;
        for info in new_infos2 {
            has_empty |= info.num_vectors == 0;
        }

        assert!(
            has_empty,
            "Testing that new segment is created if none left"
        );

        let batch = BatchPersisted {
            ids: vec![601.into(), 602.into(), 603.into()],
            vectors: BatchVectorStructPersisted::Single(vec![
                random_vector(&mut rng, dim),
                random_vector(&mut rng, dim),
                random_vector(&mut rng, dim),
            ]),
            payloads: None,
        };

        let insert_point_ops =
            PointOperations::UpsertPoints(PointInsertOperationsInternal::from(batch));

        process_point_operation(
            &locked_holder.read(),
            opnum.next().unwrap(),
            insert_point_ops,
            &hw_counter,
        )
        .unwrap();
    }

    /// Test that indexing optimizer maintain expected number of during the optimization duty
    #[test]
    fn test_indexing_optimizer_with_number_of_segments() {
        init();

        let mut holder = SegmentHolder::default();

        let dim = 256;

        let segments_dir = Builder::new().prefix("segments_dir").tempdir().unwrap();
        let segments_temp_dir = Builder::new()
            .prefix("segments_temp_dir")
            .tempdir()
            .unwrap();
        let mut opnum = 101..1000000;

        let segments = vec![
            random_segment(segments_dir.path(), opnum.next().unwrap(), 100, dim),
            random_segment(segments_dir.path(), opnum.next().unwrap(), 100, dim),
            random_segment(segments_dir.path(), opnum.next().unwrap(), 100, dim),
            random_segment(segments_dir.path(), opnum.next().unwrap(), 100, dim),
        ];

        let number_of_segments = segments.len();
        let segment_config = segments[0].segment_config.clone();

        let _segment_ids: Vec<SegmentId> = segments
            .into_iter()
            .map(|segment| holder.add_new(segment))
            .collect();

        let locked_holder = LockedSegmentHolder::new(holder);

        let index_optimizer = new_indexing_optimizer(
            number_of_segments, // Keep the same number of segments
            OptimizerThresholds {
                max_segment_size_kb: 1000,
                memmap_threshold_kb: 1000,
                indexing_threshold_kb: 10, // Always optimize
                deferred_internal_id: None,
            },
            segments_dir.path().to_owned(),
            segments_temp_dir.path().to_owned(),
            CollectionParams {
                vectors: VectorsConfig::Single(
                    VectorParamsBuilder::new(
                        segment_config.vector_data[DEFAULT_VECTOR_NAME].size as u64,
                        segment_config.vector_data[DEFAULT_VECTOR_NAME].distance,
                    )
                    .build(),
                ),
                ..CollectionParams::empty()
            },
            Default::default(),
            HnswGlobalConfig::default(),
            Default::default(),
        );

        // Index until all segments are indexed
        let mut numer_of_optimizations = 0;
        loop {
            let suggested_to_optimize = index_optimizer.plan_optimizations_for_test(&locked_holder);
            if suggested_to_optimize.is_empty() {
                break;
            }
            log::debug!("suggested_to_optimize = {suggested_to_optimize:#?}");
            let suggested_to_optimize = suggested_to_optimize.into_iter().next().unwrap();

            index_optimizer.optimize_for_test(locked_holder.clone(), suggested_to_optimize);
            numer_of_optimizations += 1;
            assert!(numer_of_optimizations <= number_of_segments);
            let number_of_segments = locked_holder.read().len();
            log::debug!(
                "numer_of_optimizations = {numer_of_optimizations}, number_of_segments = {number_of_segments}"
            );
        }

        // Ensure that the total number of segments did not change
        assert_eq!(locked_holder.read().len(), number_of_segments);
    }

    /// This tests things are as we expect when we define both `on_disk: false` and `memmap_threshold`
    ///
    /// Before this PR (<https://github.com/qdrant/qdrant/pull/3167>) such configuration would create an infinite optimization loop.
    ///
    /// It tests whether:
    /// - the on_disk flag is preferred over memmap_threshold
    /// - the index optimizer and config mismatch optimizer don't conflict with this preference
    /// - there is no infinite optiization loop with the above configuration
    ///
    /// In short, this is what happens in this test:
    /// - create randomized segment as base with `on_disk: false` and `memmap_threshold`
    /// - test that indexing optimizer and config mismatch optimizer dont trigger
    /// - test that current storage is in memory
    /// - change `on_disk: None`
    /// - test that indexing optimizer now wants to optimize for `memmap_threshold`
    /// - optimize with indexing optimizer to put storage on disk
    /// - test that config mismatch optimizer doesn't try to revert on disk storage
    #[test]
    fn test_on_disk_memmap_threshold_conflict() {
        // Collection configuration
        let (point_count, dim) = (1000, 10);
        let thresholds_config = OptimizerThresholds {
            max_segment_size_kb: usize::MAX,
            memmap_threshold_kb: 10,
            indexing_threshold_kb: usize::MAX,
            deferred_internal_id: None,
        };
        let mut collection_params = CollectionParams {
            vectors: VectorsConfig::Single(
                VectorParamsBuilder::new(dim as u64, Distance::Dot)
                    .with_on_disk(false)
                    .build(),
            ),
            ..CollectionParams::empty()
        };

        // Base segment
        let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let mut holder = SegmentHolder::default();

        let segment = random_segment(dir.path(), 100, point_count, dim as usize);

        let segment_id = holder.add_new(segment);
        let locked_holder = LockedSegmentHolder::new(holder);

        let hnsw_config = HnswConfig {
            memory: None,
            m: 16,
            ef_construct: 100,
            full_scan_threshold: 10,
            max_indexing_threads: 0,
            on_disk: None,
            payload_m: None,
            inline_storage: None,
        };

        {
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
            let config_mismatch_optimizer = new_config_mismatch_optimizer(
                thresholds_config,
                dir.path().to_owned(),
                temp_dir.path().to_owned(),
                collection_params.clone(),
                hnsw_config,
                HnswGlobalConfig::default(),
                Default::default(),
            );

            // Index optimizer should not optimize and put storage back in memory, nothing changed
            let suggested_to_optimize = index_optimizer.plan_optimizations_for_test(&locked_holder);
            assert_eq!(
                suggested_to_optimize.len(),
                0,
                "index optimizer should not run for index nor mmap"
            );

            // Config mismatch optimizer should not try to change the current state
            let suggested_to_optimize =
                config_mismatch_optimizer.plan_optimizations_for_test(&locked_holder);
            assert_eq!(
                suggested_to_optimize.len(),
                0,
                "config mismatch optimizer should not change anything"
            );

            // Ensure segment is not on disk
            locked_holder
                .read()
                .iter_original()
                .map(|(_, segment)| segment.read())
                .filter(|segment| segment.total_point_count() > 0)
                .for_each(|segment| {
                    assert!(
                        !segment.config().vector_data[DEFAULT_VECTOR_NAME]
                            .storage_type
                            .is_on_disk(),
                        "segment must not be on disk with mmap",
                    );
                });
        }

        // Remove explicit on_disk flag and go back to default
        collection_params
            .vectors
            .get_params_mut(DEFAULT_VECTOR_NAME)
            .unwrap()
            .on_disk
            .take();

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
        let config_mismatch_optimizer = new_config_mismatch_optimizer(
            thresholds_config,
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            collection_params,
            hnsw_config,
            HnswGlobalConfig::default(),
            Default::default(),
        );

        // Use indexing optimizer to build mmap
        let changed = index_optimizer.optimize_for_test(locked_holder.clone(), vec![segment_id]);
        assert!(
            changed > 0,
            "optimizer should have rebuilt this segment for mmap"
        );
        assert!(
            locked_holder.read().get(segment_id).is_none(),
            "optimized segment should be gone",
        );
        assert_eq!(locked_holder.read().len(), 2, "mmap must be built");

        // Mismatch optimizer should not optimize yet, HNSW config is not changed yet
        let suggested_to_optimize =
            config_mismatch_optimizer.plan_optimizations_for_test(&locked_holder);
        assert_eq!(suggested_to_optimize.len(), 0);

        // Ensure new segment is on disk now
        locked_holder
            .read()
            .iter_original()
            .map(|(_, segment)| segment.read())
            .filter(|segment| segment.total_point_count() > 0)
            .for_each(|segment| {
                assert!(
                    segment.config().vector_data[DEFAULT_VECTOR_NAME]
                        .storage_type
                        .is_on_disk(),
                    "segment must be on disk with mmap",
                );
            });
    }

    /// Multi vectors with deferred points below the indexing threshold must not cause an
    /// infinite optimization loop.
    ///
    /// Deferred points are tracked with a static internal-id offset (`deferred_internal_id`).
    /// For multi vectors this offset is computed assuming a fixed number of sub vectors per
    /// point (see [`CollectionParams::get_deferred_point_id`] and its `MULTIVECTOR_SIZE`
    /// constant). When a user uploads small multi vectors (e.g. a single sub vector per
    /// point), the real storage size stays well below the indexing threshold, yet the segment
    /// still ends up with deferred points because the offset was sized for much larger points.
    ///
    /// The indexing optimizer always triggers for segments that have deferred points, so it
    /// can promote them quickly by building an HNSW index. Before commit "Always optimize
    /// deferred points", the optimizer skipped building the HNSW index while the segment was
    /// still below the indexing threshold. Because building the index is what promotes
    /// deferred points, they stayed deferred, the optimizer kept being triggered, and the
    /// same segment was re-optimized forever -- an infinite loop.
    ///
    /// This test reproduces that scenario. Before the fix it loops forever (and hits the
    /// iteration limit, failing the test); after the fix the optimizer builds an HNSW index,
    /// promotes the deferred points, and terminates.
    #[test]
    fn test_deferred_multivector_below_indexing_threshold_no_infinite_loop() {
        init();

        let dim = 4;
        let indexing_threshold_kb = 10;
        let indexing_threshold_bytes = indexing_threshold_kb * 1024;

        let segments_dir = Builder::new().prefix("segments_dir").tempdir().unwrap();
        let segments_temp_dir = Builder::new()
            .prefix("segments_temp_dir")
            .tempdir()
            .unwrap();

        // A single multi vector (one vector configured as a multivector).
        let mut multi_params = VectorParamsBuilder::new(dim as u64, Distance::Dot).build();
        multi_params.multivector_config = Some(MultiVectorConfig {
            comparator: MultiVectorComparator::MaxSim,
        });
        let collection_params = CollectionParams {
            vectors: VectorsConfig::Single(multi_params),
            ..CollectionParams::empty()
        };

        let hnsw_config = HnswConfig::default();

        // The deferred offset, computed exactly like production does. For multi vectors this
        // assumes a fixed (large) number of sub vectors per point, so the offset is small
        // relative to the number of points even when each point holds a single sub vector.
        let deferred_internal_id = collection_params
            .get_deferred_point_id(&hnsw_config, NonZeroUsize::new(indexing_threshold_bytes))
            .expect("a multi vector with HNSW indexing should produce a deferred offset");

        // Upload more points than the deferred offset (so we get deferred points), while each
        // point stores only a single sub vector so the real storage size stays well below the
        // indexing threshold.
        let num_points = u64::from(deferred_internal_id) * 2;
        assert!(
            num_points > u64::from(deferred_internal_id),
            "test must upload more points than the deferred offset to create deferred points",
        );

        // Build a plain, appendable segment for this collection with the deferred offset set,
        // mimicking a live appendable segment that has accumulated deferred points.
        let segment_optimizer_config =
            build_segment_optimizer_config(&collection_params, &hnsw_config, &None);
        let segment_config = segment_optimizer_config.plain_segment_config();

        let hw_counter = HardwareCounterCell::new();
        let (mut segment, _) = build_segment(
            segments_dir.path(),
            &segment_config,
            Some(deferred_internal_id),
            true,
        )
        .unwrap();

        for i in 0..num_points {
            let mut vectors = NamedVectors::default();
            vectors.insert(
                DEFAULT_VECTOR_NAME.into(),
                VectorInternal::MultiDense(
                    MultiDenseVectorInternal::try_from_matrix(vec![vec![0.5; dim]]).unwrap(),
                ),
            );
            segment
                .upsert_point(100, (i + 1).into(), vectors, &hw_counter)
                .unwrap();
        }

        // Sanity: the segment has deferred points but stays below the indexing threshold.
        assert!(
            segment.has_deferred_points(),
            "segment should have deferred points",
        );
        let stored_bytes = segment
            .available_vectors_size_in_bytes(DEFAULT_VECTOR_NAME)
            .unwrap();
        assert!(
            stored_bytes < indexing_threshold_bytes,
            "segment must stay below the indexing threshold \
             ({stored_bytes} >= {indexing_threshold_bytes})",
        );

        let mut holder = SegmentHolder::default();
        holder.add_new(segment);
        let locked_holder = LockedSegmentHolder::new(holder);

        let index_optimizer = new_indexing_optimizer(
            2,
            OptimizerThresholds {
                max_segment_size_kb: 1000,
                memmap_threshold_kb: 1_000_000, // never put on disk
                indexing_threshold_kb,          // real size stays below this
                deferred_internal_id: Some(deferred_internal_id),
            },
            segments_dir.path().to_owned(),
            segments_temp_dir.path().to_owned(),
            collection_params.clone(),
            hnsw_config,
            HnswGlobalConfig::default(),
            Default::default(),
        );

        // Drive the optimizer until there is nothing left to do. Before the fix the optimizer
        // re-optimizes the same segment forever because the deferred points are never
        // promoted; the iteration limit turns that infinite loop into a test failure.
        const MAX_ITERATIONS: usize = 16;
        let mut iterations = 0;
        loop {
            let suggested_to_optimize = index_optimizer.plan_optimizations_for_test(&locked_holder);
            if suggested_to_optimize.is_empty() {
                break;
            }
            assert!(
                iterations < MAX_ITERATIONS,
                "indexing optimizer is stuck in an infinite loop: deferred points below the \
                 indexing threshold are never promoted",
            );
            let batch = suggested_to_optimize.into_iter().next().unwrap();
            index_optimizer.optimize_for_test(locked_holder.clone(), batch);
            iterations += 1;
        }

        // The deferred points must have been promoted: no segment has deferred points left,
        // and an HNSW index was built to make them searchable.
        let holder = locked_holder.read();
        let any_deferred = holder
            .iter()
            .any(|(_, segment)| segment.get().read().has_deferred_points());
        assert!(!any_deferred, "deferred points should have been promoted");

        let any_hnsw = holder.iter().any(|(_, segment)| {
            segment
                .get()
                .read()
                .config()
                .vector_data
                .values()
                .any(|vector| matches!(vector.index, Indexes::Hnsw(_)))
        });
        assert!(
            any_hnsw,
            "an HNSW index should have been created to promote the deferred points",
        );
    }

    /// Reproduction for segment overgrow on `set_payload_by_filter`.
    ///
    /// Hypothesis (under test):
    ///   When all points of a collection live in immutable (indexed) segments and
    ///   `set_payload` is executed via a filter that matches all points, the
    ///   `apply_points_with_conditional_move` mechanism CoW-moves every matched
    ///   point into a single (random) appendable segment.
    ///
    ///   This appendable segment is not size-checked during the move, so its
    ///   resulting size can exceed the configured `max_segment_size`.
    ///
    /// This test currently FAILS by design: it is a reproduction for the
    /// unfixed segment overgrow bug. It will pass once the underlying issue
    /// is addressed.
    #[test]
    fn test_set_payload_by_filter_does_not_overgrow_segment() {
        init();

        let dim = 256;
        // Each random_segment with 200 points and 256-dim f32 vectors has roughly
        // ~200 KB of vector data on its own, well above the threshold below.
        let points_per_segment = 200u64;
        // Use a small max segment size so the combined indexed size exceeds it,
        // but each individual indexed segment fits below it.
        let max_segment_size_kb = 300;

        let segments_dir = Builder::new().prefix("segments_dir").tempdir().unwrap();
        let segments_temp_dir = Builder::new()
            .prefix("segments_temp_dir")
            .tempdir()
            .unwrap();
        let mut opnum = 101..1_000_000;

        // --- 1. Build two sizeable random segments (initially appendable). ---
        let segment_a = random_segment(
            segments_dir.path(),
            opnum.next().unwrap(),
            points_per_segment,
            dim,
        );
        let segment_b = random_segment(
            segments_dir.path(),
            opnum.next().unwrap(),
            points_per_segment,
            dim,
        );

        let segment_config = segment_a.segment_config.clone();

        let mut holder = SegmentHolder::default();
        let segment_a_id = holder.add_new(segment_a);
        let segment_b_id = holder.add_new(segment_b);

        // Add a small empty appendable segment so that after indexing the other
        // two, there is still an appendable target for upserts. We don't really
        // need it (the holder creates one on demand), but it makes assertions
        // about which segment overgrows clearer.
        let locked_holder = LockedSegmentHolder::new(holder);

        // --- 2. Run indexing optimizer to convert both segments to indexed
        // (non-appendable) segments. ---
        let index_optimizer = new_indexing_optimizer(
            2,
            OptimizerThresholds {
                max_segment_size_kb,
                memmap_threshold_kb: 1_000_000,
                indexing_threshold_kb: 10, // Always optimize / index
                deferred_internal_id: None,
            },
            segments_dir.path().to_owned(),
            segments_temp_dir.path().to_owned(),
            CollectionParams {
                vectors: VectorsConfig::Single(
                    VectorParamsBuilder::new(
                        segment_config.vector_data[DEFAULT_VECTOR_NAME].size as u64,
                        segment_config.vector_data[DEFAULT_VECTOR_NAME].distance,
                    )
                    .build(),
                ),
                ..CollectionParams::empty()
            },
            Default::default(),
            HnswGlobalConfig::default(),
            Default::default(),
        );

        // Index both raw segments. Each gets converted into an indexed
        // non-appendable segment and a fresh empty appendable segment is
        // created by the optimizer.
        index_optimizer.optimize_for_test(locked_holder.clone(), vec![segment_a_id]);
        index_optimizer.optimize_for_test(locked_holder.clone(), vec![segment_b_id]);

        // Sanity check: we now have indexed segments larger combined than the
        // configured max segment size.
        let indexed_total: usize = locked_holder
            .read()
            .iter()
            .map(|(_sid, segment)| {
                let segment = segment.get().read();
                if segment.is_appendable() {
                    0
                } else {
                    segment
                        .max_available_vectors_size_in_bytes()
                        .unwrap_or_default()
                }
            })
            .sum();
        let max_segment_size_bytes = max_segment_size_kb * 1024;
        assert!(
            indexed_total > max_segment_size_bytes,
            "Expected combined indexed size ({indexed_total}) to exceed max_segment_size ({max_segment_size_bytes})",
        );

        // Sanity: at least 2 indexed (non-appendable) segments exist.
        let indexed_count = locked_holder
            .read()
            .iter()
            .filter(|(_sid, segment)| !segment.get().read().is_appendable())
            .count();
        assert!(
            indexed_count >= 2,
            "Expected at least 2 indexed segments after optimization, got {indexed_count}",
        );

        // --- 3. Run set_payload by filter that matches ALL points. ---
        // An empty filter matches every point in the collection.
        let payload: segment::types::Payload = payload_json! {"new_field": "value"};

        let hw_counter = HardwareCounterCell::new();

        let payload_op = crate::operations::payload_ops::PayloadOps::SetPayload(
            crate::operations::payload_ops::SetPayloadOp {
                payload,
                points: None,
                filter: Some(segment::types::Filter::default()),
                key: None,
            },
        );

        let result = process_payload_operation(
            &locked_holder.read(),
            opnum.next().unwrap(),
            payload_op,
            &hw_counter,
        );

        assert!(
            result.is_ok(),
            "set_payload_by_filter should succeed: {result:?}",
        );

        // --- 4. Verify no segment exceeds max_segment_size. ---
        let largest_segment_bytes = locked_holder
            .read()
            .iter()
            .map(|(sid, segment)| {
                let s = segment.get().read();
                let size = s.max_available_vectors_size_in_bytes().unwrap_or_default();
                let info = s.info().unwrap();
                log::info!(
                    "segment {sid:?}: appendable={} num_points={} num_vectors={} size_bytes={size}",
                    s.is_appendable(),
                    info.num_points,
                    info.num_vectors,
                );
                size
            })
            .max()
            .unwrap_or_default();

        assert!(
            largest_segment_bytes <= max_segment_size_bytes,
            "A segment overgrew the configured max_segment_size: largest={largest_segment_bytes} bytes, max={max_segment_size_bytes} bytes",
        );
    }
}
