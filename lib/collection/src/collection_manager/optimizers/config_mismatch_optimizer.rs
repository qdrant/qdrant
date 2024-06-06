use std::borrow::Cow;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use segment::common::operation_time_statistics::OperationDurationsAggregator;
use segment::index::sparse_index::sparse_index_config::SparseIndexType;
use segment::types::{HnswConfig, Indexes, QuantizationConfig, SegmentType};

use crate::collection_manager::holders::segment_holder::{LockedSegmentHolder, SegmentId};
use crate::collection_manager::optimizers::segment_optimizer::{
    OptimizerThresholds, SegmentOptimizer,
};
use crate::config::CollectionParams;
use crate::operations::config_diff::DiffConfig;

/// Looks for segments having a mismatch between configured and actual parameters
///
/// For example, a user may change the HNSW parameters for a collection. A segment that was already
/// indexed with different parameters now has a mismatch. This segment should be optimized (and
/// indexed) again in order to update the effective configuration.
pub struct ConfigMismatchOptimizer {
    thresholds_config: OptimizerThresholds,
    segments_path: PathBuf,
    collection_temp_dir: PathBuf,
    collection_params: CollectionParams,
    hnsw_config: HnswConfig,
    quantization_config: Option<QuantizationConfig>,
    telemetry_durations_aggregator: Arc<Mutex<OperationDurationsAggregator>>,
}

impl ConfigMismatchOptimizer {
    pub fn new(
        thresholds_config: OptimizerThresholds,
        segments_path: PathBuf,
        collection_temp_dir: PathBuf,
        collection_params: CollectionParams,
        hnsw_config: HnswConfig,
        quantization_config: Option<QuantizationConfig>,
    ) -> Self {
        ConfigMismatchOptimizer {
            thresholds_config,
            segments_path,
            collection_temp_dir,
            collection_params,
            hnsw_config,
            quantization_config,
            telemetry_durations_aggregator: OperationDurationsAggregator::new(),
        }
    }

    /// Check if current configuration requires vectors to be stored on disk
    fn check_if_vectors_on_disk(&self, vector_name: &str) -> Option<bool> {
        self.collection_params
            .vectors
            .get_params(vector_name)
            .and_then(|vector_params| vector_params.on_disk)
    }

    /// Check if current configuration requires sparse vectors index to be stored on disk
    #[allow(dead_code)]
    fn check_if_sparse_vectors_index_on_disk(&self, vector_name: &str) -> Option<bool> {
        self.collection_params
            .sparse_vectors
            .as_ref()
            .and_then(|vector_params| vector_params.get(vector_name))
            .and_then(|params| params.index)
            .and_then(|index| index.on_disk)
    }

    /// Calculates and HNSW config that should be used for a given vector
    /// with current configuration.
    ///
    /// Takes vector-specific HNSW config (if any) and merges it with the collection-wide config.
    fn get_required_hnsw_config(&self, vector_name: &str) -> Cow<HnswConfig> {
        let target_hnsw_collection = &self.hnsw_config;
        // Select vector specific target HNSW config
        let target_hnsw_vector = self
            .collection_params
            .vectors
            .get_params(vector_name)
            .and_then(|vector_params| vector_params.hnsw_config)
            .map(|vector_hnsw| vector_hnsw.update(target_hnsw_collection))
            .and_then(|hnsw| match hnsw {
                Ok(hnsw) => Some(hnsw),
                Err(err) => {
                    log::warn!(
                        "Failed to merge collection and vector HNSW config, ignoring: {err}"
                    );
                    None
                }
            });
        match target_hnsw_vector {
            Some(target_hnsw) => Cow::Owned(target_hnsw),
            None => Cow::Borrowed(target_hnsw_collection),
        }
    }

    fn worst_segment(
        &self,
        segments: LockedSegmentHolder,
        excluded_ids: &HashSet<SegmentId>,
    ) -> Vec<SegmentId> {
        let segments_read_guard = segments.read();
        let candidates: Vec<_> = segments_read_guard
            .iter()
            // Excluded externally, might already be scheduled for optimization
            .filter(|(idx, _)| !excluded_ids.contains(idx))
            .filter_map(|(idx, segment)| {
                let segment_entry = segment.get();
                let read_segment = segment_entry.read();
                let vector_size = read_segment
                    .max_available_vectors_size_in_bytes()
                    .unwrap_or_default();

                let segment_config = read_segment.config();

                if read_segment.segment_type() == SegmentType::Special {
                    return None; // Never optimize already optimized segment
                }

                if self.collection_params.on_disk_payload
                    != segment_config.payload_storage_type.is_on_disk()
                {
                    return Some((*idx, vector_size)); // Skip segments with payload mismatch
                }

                // Determine whether dense data in segment has mismatch
                let dense_has_mismatch =
                    segment_config
                        .vector_data
                        .iter()
                        .any(|(vector_name, vector_data)| {
                            // Check HNSW mismatch
                            match &vector_data.index {
                                Indexes::Plain {} => {}
                                Indexes::Hnsw(effective_hnsw) => {
                                    // Select segment if we have an HNSW mismatch that requires rebuild
                                    let target_hnsw = self.get_required_hnsw_config(vector_name);
                                    if effective_hnsw.mismatch_requires_rebuild(&target_hnsw) {
                                        return true;
                                    }
                                }
                            }

                            if let Some(is_required_on_disk) =
                                self.check_if_vectors_on_disk(vector_name)
                            {
                                if is_required_on_disk != vector_data.storage_type.is_on_disk() {
                                    return true;
                                }
                            }

                            // Check quantization mismatch
                            let target_quantization_collection = self.quantization_config.as_ref();
                            let target_quantization_vector = self
                                .collection_params
                                .vectors
                                .get_params(vector_name)
                                .and_then(|vector_params| {
                                    vector_params.quantization_config.clone()
                                });
                            let target_quantization = target_quantization_vector
                                .as_ref()
                                .or(target_quantization_collection);
                            let quantization_mismatch = vector_data
                                .quantization_config
                                .as_ref()
                                .zip(target_quantization)
                                // Rebuild if current parameters differ from target parameters
                                .map(|(current, target)| current.mismatch_requires_rebuild(target))
                                // Or rebuild if we now change the enabled state on an indexed segment
                                .unwrap_or_else(|| {
                                    vector_data.index.is_indexed()
                                        && (vector_data.quantization_config.is_some()
                                            != target_quantization.is_some())
                                });

                            quantization_mismatch
                        });

                // Determine whether dense data in segment has mismatch
                let sparse_has_mismatch =
                    segment_config
                        .sparse_vector_data
                        .iter()
                        .any(|(vector_name, vector_data)| {
                            let Some(is_required_on_disk) =
                                self.check_if_sparse_vectors_index_on_disk(vector_name)
                            else {
                                return false; // Do nothing if not specified
                            };

                            match vector_data.index.index_type {
                                SparseIndexType::MutableRam => false, // Do nothing for mutable RAM
                                SparseIndexType::ImmutableRam => is_required_on_disk, // Rebuild if we require on disk
                                SparseIndexType::Mmap => !is_required_on_disk, // Rebuild if we require in RAM
                            }
                        });
                (sparse_has_mismatch || dense_has_mismatch).then_some((*idx, vector_size))
            })
            .collect();

        // Select segment with largest vector size
        candidates
            .into_iter()
            .max_by_key(|(_, vector_size)| *vector_size)
            .map(|(segment_id, _)| segment_id)
            .into_iter()
            .collect()
    }
}

impl SegmentOptimizer for ConfigMismatchOptimizer {
    fn name(&self) -> &str {
        "config mismatch"
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

    fn get_telemetry_counter(&self) -> &Mutex<OperationDurationsAggregator> {
        &self.telemetry_durations_aggregator
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use common::cpu::CpuPermit;
    use parking_lot::RwLock;
    use segment::entry::entry_point::SegmentEntry;
    use segment::index::hnsw_index::num_rayon_threads;
    use segment::types::{
        CompressionRatio, Distance, ProductQuantization, ProductQuantizationConfig,
        ScalarQuantizationConfig, ScalarType,
    };
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::{random_multi_vec_segment, random_segment};
    use crate::collection_manager::holders::segment_holder::{LockedSegment, SegmentHolder};
    use crate::collection_manager::optimizers::indexing_optimizer::IndexingOptimizer;
    use crate::operations::config_diff::HnswConfigDiff;
    use crate::operations::types::VectorsConfig;
    use crate::operations::vector_params_builder::VectorParamsBuilder;

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
            max_segment_size: usize::MAX,
            memmap_threshold: usize::MAX,
            indexing_threshold: 10,
        };
        let collection_params = CollectionParams {
            vectors: VectorsConfig::Single(
                VectorParamsBuilder::new(dim as u64, Distance::Dot).build(),
            ),
            ..CollectionParams::empty()
        };

        // Base segment
        let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let mut holder = SegmentHolder::default();

        let segment = random_segment(dir.path(), 100, point_count, dim as usize);

        let segment_id = holder.add_new(segment);
        let locked_holder: Arc<RwLock<_>> = Arc::new(RwLock::new(holder));

        let hnsw_config = HnswConfig {
            m: 16,
            ef_construct: 100,
            full_scan_threshold: 10,
            max_indexing_threads: 0,
            on_disk: None,
            payload_m: None,
        };

        // Optimizers used in test
        let index_optimizer = IndexingOptimizer::new(
            2,
            thresholds_config.clone(),
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            collection_params.clone(),
            hnsw_config.clone(),
            Default::default(),
        );
        let mut config_mismatch_optimizer = ConfigMismatchOptimizer::new(
            thresholds_config,
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            collection_params,
            hnsw_config.clone(),
            Default::default(),
        );

        let permit_cpu_count = num_rayon_threads(hnsw_config.max_indexing_threads);
        let permit = CpuPermit::dummy(permit_cpu_count as u32);

        // Use indexing optimizer to build index for HNSW mismatch test
        let changed = index_optimizer
            .optimize(
                locked_holder.clone(),
                vec![segment_id],
                permit,
                &false.into(),
            )
            .unwrap();
        assert!(changed, "optimizer should have rebuilt this segment");
        assert!(
            locked_holder.read().get(segment_id).is_none(),
            "optimized segment should be gone",
        );
        assert_eq!(locked_holder.read().len(), 2, "index must be built");

        // Mismatch optimizer should not optimize yet, HNSW config is not changed yet
        let suggested_to_optimize =
            config_mismatch_optimizer.check_condition(locked_holder.clone(), &Default::default());
        assert_eq!(suggested_to_optimize.len(), 0);

        // Create changed HNSW config with other m/ef_construct value, update it in the optimizer
        let mut changed_hnsw_config = hnsw_config;
        changed_hnsw_config.m /= 2;
        changed_hnsw_config.ef_construct /= 5;
        config_mismatch_optimizer.hnsw_config = changed_hnsw_config.clone();

        // Run mismatch optimizer again, make sure it optimizes now
        let permit = CpuPermit::dummy(permit_cpu_count as u32);
        let suggested_to_optimize =
            config_mismatch_optimizer.check_condition(locked_holder.clone(), &Default::default());
        assert_eq!(suggested_to_optimize.len(), 1);
        let changed = config_mismatch_optimizer
            .optimize(
                locked_holder.clone(),
                suggested_to_optimize,
                permit,
                &false.into(),
            )
            .unwrap();
        assert!(changed, "optimizer should have rebuilt this segment");

        // Ensure new segment has changed HNSW config
        locked_holder
            .read()
            .iter()
            .map(|(_, segment)| match segment {
                LockedSegment::Original(s) => s.read(),
                LockedSegment::Proxy(_) => unreachable!(),
            })
            .filter(|segment| segment.total_point_count() > 0)
            .for_each(|segment| {
                assert_eq!(
                    segment.config().vector_data[""].index,
                    Indexes::Hnsw(changed_hnsw_config.clone()),
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
            max_segment_size: usize::MAX,
            memmap_threshold: usize::MAX,
            indexing_threshold: 10,
        };
        let hnsw_config_vector1 = HnswConfigDiff {
            m: Some(10),
            ef_construct: Some(40),
            on_disk: Some(true),
            ..Default::default()
        };
        let collection_params = CollectionParams {
            vectors: VectorsConfig::Multi(BTreeMap::from([
                (
                    "vector1".into(),
                    VectorParamsBuilder::new(vector1_dim as u64, Distance::Dot)
                        .with_hnsw_config(hnsw_config_vector1)
                        .build(),
                ),
                (
                    "vector2".into(),
                    VectorParamsBuilder::new(vector2_dim as u64, Distance::Dot).build(),
                ),
            ])),
            ..CollectionParams::empty()
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

        let segment_id = holder.add_new(segment);
        let locked_holder: Arc<RwLock<_>> = Arc::new(RwLock::new(holder));

        let hnsw_config_collection = HnswConfig {
            m: 16,
            ef_construct: 100,
            full_scan_threshold: 10,
            max_indexing_threads: 0,
            on_disk: None,
            payload_m: None,
        };

        let permit_cpu_count = num_rayon_threads(hnsw_config_collection.max_indexing_threads);
        let permit = CpuPermit::dummy(permit_cpu_count as u32);

        // Optimizers used in test
        let index_optimizer = IndexingOptimizer::new(
            2,
            thresholds_config.clone(),
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            collection_params.clone(),
            hnsw_config_collection.clone(),
            Default::default(),
        );
        let mut config_mismatch_optimizer = ConfigMismatchOptimizer::new(
            thresholds_config,
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            collection_params,
            hnsw_config_collection.clone(),
            Default::default(),
        );

        // Use indexing optimizer to build index for HNSW mismatch test
        let changed = index_optimizer
            .optimize(
                locked_holder.clone(),
                vec![segment_id],
                permit,
                &false.into(),
            )
            .unwrap();
        assert!(changed, "optimizer should have rebuilt this segment");
        assert!(
            locked_holder.read().get(segment_id).is_none(),
            "optimized segment should be gone",
        );
        assert_eq!(locked_holder.read().len(), 2, "index must be built");

        // Mismatch optimizer should not optimize yet, HNSW config is not changed yet
        let suggested_to_optimize =
            config_mismatch_optimizer.check_condition(locked_holder.clone(), &Default::default());
        assert_eq!(suggested_to_optimize.len(), 0);

        // Create changed HNSW config for vector2, update it in the optimizer
        let mut hnsw_config_vector2 = hnsw_config_vector1;
        hnsw_config_vector2.m = hnsw_config_vector1.m.map(|m| m / 2);
        hnsw_config_vector2.ef_construct = None;
        match config_mismatch_optimizer.collection_params.vectors {
            VectorsConfig::Single(_) => unreachable!(),
            VectorsConfig::Multi(ref mut map) => {
                map.get_mut("vector2")
                    .unwrap()
                    .hnsw_config
                    .replace(hnsw_config_vector2);
            }
        }

        // Run mismatch optimizer again, make sure it optimizes now
        let permit = CpuPermit::dummy(permit_cpu_count as u32);
        let suggested_to_optimize =
            config_mismatch_optimizer.check_condition(locked_holder.clone(), &Default::default());
        assert_eq!(suggested_to_optimize.len(), 1);
        let changed = config_mismatch_optimizer
            .optimize(
                locked_holder.clone(),
                suggested_to_optimize,
                permit,
                &false.into(),
            )
            .unwrap();
        assert!(changed, "optimizer should have rebuilt this segment");

        // Ensure new segment has changed HNSW config
        locked_holder
            .read()
            .iter()
            .map(|(_, segment)| match segment {
                LockedSegment::Original(s) => s.read(),
                LockedSegment::Proxy(_) => unreachable!(),
            })
            .filter(|segment| segment.total_point_count() > 0)
            .for_each(|segment| {
                assert_eq!(
                    segment.config().vector_data["vector1"].index,
                    Indexes::Hnsw(hnsw_config_vector1.update(&hnsw_config_collection).unwrap()),
                    "HNSW config of vector1 is not what we expect",
                );
                assert_eq!(
                    segment.config().vector_data["vector2"].index,
                    Indexes::Hnsw(hnsw_config_vector2.update(&hnsw_config_collection).unwrap()),
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
            max_segment_size: usize::MAX,
            memmap_threshold: usize::MAX,
            indexing_threshold: 10,
        };
        let quantization_config_vector1 =
            QuantizationConfig::Scalar(segment::types::ScalarQuantization {
                scalar: ScalarQuantizationConfig {
                    r#type: ScalarType::Int8,
                    quantile: Some(0.99),
                    always_ram: Some(true),
                },
            });
        let collection_params = CollectionParams {
            vectors: VectorsConfig::Multi(BTreeMap::from([
                (
                    "vector1".into(),
                    VectorParamsBuilder::new(vector1_dim as u64, Distance::Dot)
                        .with_quantization_config(quantization_config_vector1.clone())
                        .build(),
                ),
                (
                    "vector2".into(),
                    VectorParamsBuilder::new(vector2_dim as u64, Distance::Dot).build(),
                ),
            ])),
            ..CollectionParams::empty()
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

        let segment_id = holder.add_new(segment);
        let locked_holder: Arc<RwLock<_>> = Arc::new(RwLock::new(holder));

        let quantization_config_collection =
            QuantizationConfig::Scalar(segment::types::ScalarQuantization {
                scalar: ScalarQuantizationConfig {
                    r#type: ScalarType::Int8,
                    quantile: Some(0.91),
                    always_ram: None,
                },
            });

        // Optimizers used in test
        let index_optimizer = IndexingOptimizer::new(
            2,
            thresholds_config.clone(),
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            collection_params.clone(),
            Default::default(),
            Some(quantization_config_collection.clone()),
        );
        let mut config_mismatch_optimizer = ConfigMismatchOptimizer::new(
            thresholds_config,
            dir.path().to_owned(),
            temp_dir.path().to_owned(),
            collection_params,
            Default::default(),
            Some(quantization_config_collection),
        );

        let permit_cpu_count = num_rayon_threads(0);
        let permit = CpuPermit::dummy(permit_cpu_count as u32);

        // Use indexing optimizer to build index for quantization mismatch test
        let changed = index_optimizer
            .optimize(
                locked_holder.clone(),
                vec![segment_id],
                permit,
                &false.into(),
            )
            .unwrap();
        assert!(changed, "optimizer should have rebuilt this segment");
        assert!(
            locked_holder.read().get(segment_id).is_none(),
            "optimized segment should be gone",
        );
        assert_eq!(locked_holder.read().len(), 2, "index must be built");

        // Mismatch optimizer should not optimize yet, quantization config is not changed yet
        let suggested_to_optimize =
            config_mismatch_optimizer.check_condition(locked_holder.clone(), &Default::default());
        assert_eq!(suggested_to_optimize.len(), 0);

        // Create changed quantization config for vector2, update it in the optimizer
        let quantization_config_vector2 = QuantizationConfig::Product(ProductQuantization {
            product: ProductQuantizationConfig {
                compression: CompressionRatio::X32,
                always_ram: Some(true),
            },
        });
        match config_mismatch_optimizer.collection_params.vectors {
            VectorsConfig::Single(_) => unreachable!(),
            VectorsConfig::Multi(ref mut map) => {
                map.get_mut("vector2")
                    .unwrap()
                    .quantization_config
                    .replace(quantization_config_vector2.clone());
            }
        }

        // Run mismatch optimizer again, make sure it optimizes now
        let permit = CpuPermit::dummy(permit_cpu_count as u32);
        let suggested_to_optimize =
            config_mismatch_optimizer.check_condition(locked_holder.clone(), &Default::default());
        assert_eq!(suggested_to_optimize.len(), 1);
        let changed = config_mismatch_optimizer
            .optimize(
                locked_holder.clone(),
                suggested_to_optimize,
                permit,
                &false.into(),
            )
            .unwrap();
        assert!(changed, "optimizer should have rebuilt this segment");

        // Ensure new segment has changed quantization config
        locked_holder
            .read()
            .iter()
            .map(|(_, segment)| match segment {
                LockedSegment::Original(s) => s.read(),
                LockedSegment::Proxy(_) => unreachable!(),
            })
            .filter(|segment| segment.total_point_count() > 0)
            .for_each(|segment| {
                assert_eq!(
                    segment.config().vector_data["vector1"].quantization_config,
                    Some(quantization_config_vector1.clone()),
                    "Quantization config of vector1 is not what we expect",
                );
                assert_eq!(
                    segment.config().vector_data["vector2"].quantization_config,
                    Some(quantization_config_vector2.clone()),
                    "Quantization config of vector2 is not what we expect",
                );
            });
    }
}
