/// Looks for segments having a mismatch between configured and actual parameters
///
/// For example, a user may change the HNSW parameters for a collection. A segment that was already
/// indexed with different parameters now has a mismatch. This segment should be optimized (and
/// indexed) again in order to update the effective configuration.
pub use shard::optimizers::config_mismatch_optimizer::ConfigMismatchOptimizer;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use itertools::Itertools;
    use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
    use segment::entry::NonAppendableSegmentEntry;
    use segment::segment_constructor::simple_segment_constructor::{VECTOR1_NAME, VECTOR2_NAME};
    use segment::types::{
        CompressionRatio, HnswConfig, HnswGlobalConfig, Indexes, ProductQuantization,
        ProductQuantizationConfig, QuantizationConfig, ScalarQuantizationConfig, ScalarType,
        SegmentConfig, VectorNameBuf, VectorStorageType,
    };
    use shard::operations::optimization::OptimizerThresholds;
    use shard::optimizers::config::{
        DenseVectorOptimizerConfig, SegmentOptimizerConfig, SparseVectorOptimizerConfig,
    };
    use shard::optimizers::segment_optimizer::SegmentOptimizer;
    use shard::segment_holder::locked::LockedSegmentHolder;
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::{random_multi_vec_segment, random_segment};
    use crate::collection_manager::holders::segment_holder::SegmentHolder;
    use crate::collection_manager::optimizers::indexing_optimizer::IndexingOptimizer;

    fn dense_map_from_segment(
        segment_config: &SegmentConfig,
        overrides: &HashMap<VectorNameBuf, DenseVectorOptimizerConfig>,
    ) -> HashMap<VectorNameBuf, DenseVectorOptimizerConfig> {
        segment_config
            .vector_data
            .keys()
            .map(|name| {
                let cfg = overrides
                    .get(name)
                    .cloned()
                    .unwrap_or(DenseVectorOptimizerConfig {
                        on_disk: None,
                        hnsw_config: HnswConfig::default(),
                        quantization_config: None,
                    });
                (name.clone(), cfg)
            })
            .collect()
    }

    fn segment_optimizer_config(
        segment_config: &SegmentConfig,
        dense_overrides: &HashMap<VectorNameBuf, DenseVectorOptimizerConfig>,
    ) -> SegmentOptimizerConfig {
        let dense_vector = dense_map_from_segment(segment_config, dense_overrides);
        let mut base_vector_data = segment_config.vector_data.clone();
        for (name, cfg) in &dense_vector {
            if let Some(vector_data) = base_vector_data.get_mut(name)
                && let Some(on_disk) = cfg.on_disk
            {
                vector_data.storage_type = if on_disk {
                    VectorStorageType::Mmap
                } else {
                    VectorStorageType::Memory
                };
            }
        }

        SegmentOptimizerConfig {
            payload_storage_type: segment_config.payload_storage_type,
            base_vector_data,
            base_sparse_vector_data: segment_config.sparse_vector_data.clone(),
            dense_vector,
            sparse_vector: segment_config
                .sparse_vector_data
                .keys()
                .map(|name| (name.clone(), SparseVectorOptimizerConfig { on_disk: None }))
                .collect(),
        }
    }

    /// This test the config mismatch optimizer for a changed HNSW config
    ///
    /// It tests whether:
    /// - the condition check for HNSW mismatches works
    /// - optimized segments (and vector storages) use the updated configuration
    ///
    /// In short, this is what happens in this test:
    /// - create randomized segment as base
    /// - use indexing optimizer to build index for our segment
    /// - test config mismatch condition: should not trigger yet
    /// - change collection HNSW config
    /// - test config mismatch condition: should trigger due to HNSW change
    /// - optimize segment with config mismatch optimizer
    /// - assert segment uses changed configuration
    #[test]
    fn test_hnsw_config_mismatch() {
        // Collection configuration
        let (point_count, dim) = (1000, 10);
        let thresholds_config = OptimizerThresholds {
            max_segment_size_kb: usize::MAX,
            memmap_threshold_kb: usize::MAX,
            indexing_threshold_kb: 10,
        };

        // Base segment
        let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let mut holder = SegmentHolder::default();

        let segment = random_segment(dir.path(), 100, point_count, dim as usize);
        let base_segment_config = segment.segment_config.clone();

        let segment_id = holder.add_new(segment);
        let locked_holder = LockedSegmentHolder::new(holder);

        let hnsw_config = HnswConfig {
            m: 16,
            ef_construct: 100,
            full_scan_threshold: 10,
            max_indexing_threads: 0,
            on_disk: None,
            payload_m: None,
            inline_storage: None,
        };

        let mut dense_overrides = HashMap::new();
        dense_overrides.insert(
            DEFAULT_VECTOR_NAME.into(),
            DenseVectorOptimizerConfig {
                on_disk: None,
                hnsw_config,
                quantization_config: None,
            },
        );
        let optimizer_config = segment_optimizer_config(&base_segment_config, &dense_overrides);

        // Optimizers used in test
        let index_optimizer = IndexingOptimizer::new(
            2,
            thresholds_config,
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            optimizer_config.clone(),
            hnsw_config,
            HnswGlobalConfig::default(),
        );
        let config_mismatch_optimizer = ConfigMismatchOptimizer::new(
            thresholds_config,
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            optimizer_config,
            hnsw_config,
            HnswGlobalConfig::default(),
        );

        // Use indexing optimizer to build index for HNSW mismatch test
        let changed = index_optimizer.optimize_for_test(locked_holder.clone(), vec![segment_id]);
        assert!(changed > 0, "optimizer should have rebuilt this segment");
        assert!(
            locked_holder.read().get(segment_id).is_none(),
            "optimized segment should be gone",
        );
        assert_eq!(locked_holder.read().len(), 2, "index must be built");

        // Mismatch optimizer should not optimize yet, HNSW config is not changed yet
        let suggested_to_optimize =
            config_mismatch_optimizer.plan_optimizations_for_test(&locked_holder);
        assert_eq!(suggested_to_optimize.len(), 0);

        // Create changed HNSW config with other m/ef_construct value, update it in the optimizer
        let mut changed_hnsw_config = hnsw_config;
        changed_hnsw_config.m /= 2;
        changed_hnsw_config.ef_construct /= 5;

        dense_overrides.insert(
            DEFAULT_VECTOR_NAME.into(),
            DenseVectorOptimizerConfig {
                on_disk: None,
                hnsw_config: changed_hnsw_config,
                quantization_config: None,
            },
        );
        let changed_optimizer_config =
            segment_optimizer_config(&base_segment_config, &dense_overrides);
        let config_mismatch_optimizer = ConfigMismatchOptimizer::new(
            thresholds_config,
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            changed_optimizer_config,
            changed_hnsw_config,
            HnswGlobalConfig::default(),
        );

        // Run mismatch optimizer again, make sure it optimizes now
        let suggested_to_optimize =
            config_mismatch_optimizer.plan_optimizations_for_test(&locked_holder);
        let suggested_to_optimize = suggested_to_optimize.into_iter().exactly_one().unwrap();
        assert_eq!(suggested_to_optimize.len(), 1);
        let changed = config_mismatch_optimizer
            .optimize_for_test(locked_holder.clone(), suggested_to_optimize);
        assert!(changed > 0, "optimizer should have rebuilt this segment");

        // Ensure new segment has changed HNSW config
        locked_holder
            .read()
            .iter_original()
            .map(|(_, segment)| segment.read())
            .filter(|segment| segment.total_point_count() > 0)
            .for_each(|segment| {
                assert_eq!(
                    segment.config().vector_data[DEFAULT_VECTOR_NAME].index,
                    Indexes::Hnsw(changed_hnsw_config),
                    "segment must be optimized with changed HNSW config",
                );
            });
    }

    /// This test the config mismatch optimizer for a changed vector specific HNSW config
    ///
    /// Similar to `test_hnsw_config_mismatch` but for multi vector segment with a vector specific
    /// change.
    ///
    /// It tests whether:
    /// - the condition check for HNSW mismatches works for a vector specific change
    /// - optimized segments (and vector storages) use the updated configuration
    ///
    /// In short, this is what happens in this test:
    /// - create randomized multi segment as base
    /// - use indexing optimizer to build index for our segment
    /// - test config mismatch condition: should not trigger yet
    /// - change HNSW config for vector2
    /// - test config mismatch condition: should trigger due to HNSW change
    /// - optimize segment with config mismatch optimizer
    /// - assert segment uses changed configuration
    #[test]
    fn test_hnsw_config_mismatch_vector_specific() {
        // Collection configuration
        let (point_count, vector1_dim, vector2_dim) = (1000, 10, 20);
        let thresholds_config = OptimizerThresholds {
            max_segment_size_kb: usize::MAX,
            memmap_threshold_kb: usize::MAX,
            indexing_threshold_kb: 10,
        };

        // Base segment
        let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let mut holder = SegmentHolder::default();

        let segment = random_multi_vec_segment(
            dir.path(),
            100,
            point_count,
            vector1_dim as usize,
            vector2_dim as usize,
        );
        let base_segment_config = segment.segment_config.clone();

        let segment_id = holder.add_new(segment);
        let locked_holder = LockedSegmentHolder::new(holder);

        let hnsw_config_collection = HnswConfig {
            m: 16,
            ef_construct: 100,
            full_scan_threshold: 10,
            max_indexing_threads: 0,
            on_disk: None,
            payload_m: None,
            inline_storage: None,
        };

        let mut hnsw_config_vector1 = hnsw_config_collection;
        hnsw_config_vector1.m = 10;
        hnsw_config_vector1.ef_construct = 40;
        hnsw_config_vector1.on_disk = Some(true);

        let hnsw_config_vector2 = hnsw_config_collection;

        let mut dense_overrides = HashMap::new();
        dense_overrides.insert(
            VECTOR1_NAME.into(),
            DenseVectorOptimizerConfig {
                on_disk: Some(true),
                hnsw_config: hnsw_config_vector1,
                quantization_config: None,
            },
        );
        dense_overrides.insert(
            VECTOR2_NAME.into(),
            DenseVectorOptimizerConfig {
                on_disk: None,
                hnsw_config: hnsw_config_vector2,
                quantization_config: None,
            },
        );
        let optimizer_config = segment_optimizer_config(&base_segment_config, &dense_overrides);

        // Optimizers used in test
        let index_optimizer = IndexingOptimizer::new(
            2,
            thresholds_config,
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            optimizer_config.clone(),
            hnsw_config_collection,
            HnswGlobalConfig::default(),
        );
        let config_mismatch_optimizer = ConfigMismatchOptimizer::new(
            thresholds_config,
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            optimizer_config,
            hnsw_config_collection,
            HnswGlobalConfig::default(),
        );

        // Use indexing optimizer to build index for HNSW mismatch test
        let changed = index_optimizer.optimize_for_test(locked_holder.clone(), vec![segment_id]);
        assert!(changed > 0, "optimizer should have rebuilt this segment");
        assert!(
            locked_holder.read().get(segment_id).is_none(),
            "optimized segment should be gone",
        );
        assert_eq!(locked_holder.read().len(), 2, "index must be built");

        // Mismatch optimizer should not optimize yet, HNSW config is not changed yet
        let suggested_to_optimize =
            config_mismatch_optimizer.plan_optimizations_for_test(&locked_holder);
        assert_eq!(suggested_to_optimize.len(), 0);

        let mut hnsw_config_vector2_changed = hnsw_config_collection;
        hnsw_config_vector2_changed.m = hnsw_config_vector1.m / 2;
        hnsw_config_vector2_changed.on_disk = Some(true);

        dense_overrides.insert(
            VECTOR2_NAME.into(),
            DenseVectorOptimizerConfig {
                on_disk: None,
                hnsw_config: hnsw_config_vector2_changed,
                quantization_config: None,
            },
        );
        let changed_optimizer_config =
            segment_optimizer_config(&base_segment_config, &dense_overrides);
        let config_mismatch_optimizer = ConfigMismatchOptimizer::new(
            thresholds_config,
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            changed_optimizer_config,
            hnsw_config_collection,
            HnswGlobalConfig::default(),
        );

        // Run mismatch optimizer again, make sure it optimizes now
        let suggested_to_optimize =
            config_mismatch_optimizer.plan_optimizations_for_test(&locked_holder);
        let suggested_to_optimize = suggested_to_optimize.into_iter().exactly_one().unwrap();
        assert_eq!(suggested_to_optimize.len(), 1);
        let changed = config_mismatch_optimizer
            .optimize_for_test(locked_holder.clone(), suggested_to_optimize);
        assert!(changed > 0, "optimizer should have rebuilt this segment");

        // Ensure new segment has changed HNSW config
        locked_holder
            .read()
            .iter_original()
            .map(|(_, segment)| segment.read())
            .filter(|segment| segment.total_point_count() > 0)
            .for_each(|segment| {
                assert_eq!(
                    segment.config().vector_data[VECTOR1_NAME].index,
                    Indexes::Hnsw(hnsw_config_vector1),
                    "HNSW config of vector1 is not what we expect",
                );
                assert_eq!(
                    segment.config().vector_data[VECTOR2_NAME].index,
                    Indexes::Hnsw(hnsw_config_vector2_changed),
                    "HNSW config of vector2 is not what we expect",
                );
            });
    }

    /// This test the config mismatch optimizer for a changed vector specific HNSW config
    ///
    /// Similar to `test_hnsw_config_mismatch` but for multi vector segment with a vector specific
    /// change.
    ///
    /// It tests whether:
    /// - the condition check for HNSW mismatches works for a vector specific change
    /// - optimized segments (and vector storages) use the updated configuration
    ///
    /// In short, this is what happens in this test:
    /// - create randomized multi segment as base
    /// - use indexing optimizer to build index for our segment
    /// - test config mismatch condition: should not trigger yet
    /// - change HNSW config for vector2
    /// - test config mismatch condition: should trigger due to HNSW change
    /// - optimize segment with config mismatch optimizer
    /// - assert segment uses changed configuration
    #[test]
    fn test_quantization_config_mismatch_vector_specific() {
        // Collection configuration
        let (point_count, vector1_dim, vector2_dim) = (1000, 10, 20);
        let thresholds_config = OptimizerThresholds {
            max_segment_size_kb: usize::MAX,
            memmap_threshold_kb: usize::MAX,
            indexing_threshold_kb: 10,
        };
        let quantization_config_vector1 =
            QuantizationConfig::Scalar(segment::types::ScalarQuantization {
                scalar: ScalarQuantizationConfig {
                    r#type: ScalarType::Int8,
                    quantile: Some(0.99),
                    always_ram: Some(true),
                },
            });

        // Base segment
        let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let mut holder = SegmentHolder::default();

        let segment = random_multi_vec_segment(
            dir.path(),
            100,
            point_count,
            vector1_dim as usize,
            vector2_dim as usize,
        );
        let base_segment_config = segment.segment_config.clone();

        let segment_id = holder.add_new(segment);
        let locked_holder = LockedSegmentHolder::new(holder);

        let quantization_config_collection =
            QuantizationConfig::Scalar(segment::types::ScalarQuantization {
                scalar: ScalarQuantizationConfig {
                    r#type: ScalarType::Int8,
                    quantile: Some(0.91),
                    always_ram: None,
                },
            });

        let mut dense_overrides = HashMap::new();
        dense_overrides.insert(
            VECTOR1_NAME.into(),
            DenseVectorOptimizerConfig {
                on_disk: None,
                hnsw_config: HnswConfig::default(),
                quantization_config: Some(quantization_config_vector1.clone()),
            },
        );
        dense_overrides.insert(
            VECTOR2_NAME.into(),
            DenseVectorOptimizerConfig {
                on_disk: None,
                hnsw_config: HnswConfig::default(),
                quantization_config: Some(quantization_config_collection.clone()),
            },
        );
        let optimizer_config = segment_optimizer_config(&base_segment_config, &dense_overrides);

        // Optimizers used in test
        let index_optimizer = IndexingOptimizer::new(
            2,
            thresholds_config,
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            optimizer_config.clone(),
            Default::default(),
            HnswGlobalConfig::default(),
        );
        let config_mismatch_optimizer = ConfigMismatchOptimizer::new(
            thresholds_config,
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            optimizer_config,
            Default::default(),
            HnswGlobalConfig::default(),
        );

        // Use indexing optimizer to build index for quantization mismatch test
        let changed = index_optimizer.optimize_for_test(locked_holder.clone(), vec![segment_id]);
        assert!(changed > 0, "optimizer should have rebuilt this segment");
        assert!(
            locked_holder.read().get(segment_id).is_none(),
            "optimized segment should be gone",
        );
        assert_eq!(locked_holder.read().len(), 2, "index must be built");

        // Mismatch optimizer should not optimize yet, quantization config is not changed yet
        let suggested_to_optimize =
            config_mismatch_optimizer.plan_optimizations_for_test(&locked_holder);
        assert_eq!(suggested_to_optimize.len(), 0);

        // Create changed quantization config for vector2, update it in the optimizer
        let quantization_config_vector2 = QuantizationConfig::Product(ProductQuantization {
            product: ProductQuantizationConfig {
                compression: CompressionRatio::X32,
                always_ram: Some(true),
            },
        });
        dense_overrides.insert(
            VECTOR2_NAME.into(),
            DenseVectorOptimizerConfig {
                on_disk: None,
                hnsw_config: HnswConfig::default(),
                quantization_config: Some(quantization_config_vector2.clone()),
            },
        );
        let changed_optimizer_config =
            segment_optimizer_config(&base_segment_config, &dense_overrides);
        let config_mismatch_optimizer = ConfigMismatchOptimizer::new(
            thresholds_config,
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            changed_optimizer_config,
            Default::default(),
            HnswGlobalConfig::default(),
        );

        // Run mismatch optimizer again, make sure it optimizes now
        let suggested_to_optimize =
            config_mismatch_optimizer.plan_optimizations_for_test(&locked_holder);
        let suggested_to_optimize = suggested_to_optimize.into_iter().exactly_one().unwrap();
        assert_eq!(suggested_to_optimize.len(), 1);
        let changed = config_mismatch_optimizer
            .optimize_for_test(locked_holder.clone(), suggested_to_optimize);
        assert!(changed > 0, "optimizer should have rebuilt this segment");

        // Ensure new segment has changed quantization config
        locked_holder
            .read()
            .iter_original()
            .map(|(_, segment)| segment.read())
            .filter(|segment| segment.total_point_count() > 0)
            .for_each(|segment| {
                assert_eq!(
                    segment.config().vector_data[VECTOR1_NAME].quantization_config,
                    Some(quantization_config_vector1.clone()),
                    "Quantization config of vector1 is not what we expect",
                );
                assert_eq!(
                    segment.config().vector_data[VECTOR2_NAME].quantization_config,
                    Some(quantization_config_vector2.clone()),
                    "Quantization config of vector2 is not what we expect",
                );
            });
    }
}
