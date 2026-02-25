use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::budget::ResourceBudget;
use common::progress_tracker::new_progress_tracker;
use fs_err as fs;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::index::hnsw_index::num_rayon_threads;
use segment::index::sparse_index::sparse_index_config::SparseIndexType;
use segment::types::{HnswConfig, HnswGlobalConfig, Indexes, VectorStorageType};
use shard::operations::optimization::OptimizerThresholds;
use shard::optimizers::config::{
    DenseVectorOptimizerConfig, SegmentOptimizerConfig, SparseVectorOptimizerConfig,
};
use shard::optimizers::config_mismatch_optimizer::ConfigMismatchOptimizer;
use shard::optimizers::indexing_optimizer::IndexingOptimizer;
use shard::optimizers::merge_optimizer::MergeOptimizer;
use shard::optimizers::segment_optimizer::{Optimizer, plan_optimizations};
use shard::optimizers::vacuum_optimizer::VacuumOptimizer;
use uuid::Uuid;

use crate::{EdgeShard, SEGMENTS_PATH};

const EDGE_OPTIMIZER_TEMP_PATH: &str = "optimizer_tmp";
const DEFAULT_MAX_SEGMENT_PER_CPU_KB: usize = 256_000;
const DEFAULT_INDEXING_THRESHOLD_KB: usize = 100_000;
const DEFAULT_DELETED_THRESHOLD: f64 = 0.1;
const DEFAULT_VACUUM_MIN_VECTOR_NUMBER: usize = 1000;

impl EdgeShard {
    /// Run shard optimizers in-process and blocking until no more optimization plans are produced.
    ///
    /// This is synchronous and does not spawn background optimization workers.
    pub fn optimize_all_segments_blocking(&self) -> OperationResult<bool> {
        let optimizers = self.build_blocking_optimizers()?;
        let stopped = AtomicBool::new(false);
        let mut optimized_any = false;

        loop {
            let planned = {
                let segments = self.segments.read();
                plan_optimizations(&segments, &optimizers)
            };

            if planned.is_empty() {
                return Ok(optimized_any);
            }

            let mut optimized_in_iteration = false;

            for (optimizer, segment_ids) in planned {
                let desired_io = num_rayon_threads(optimizer.hnsw_config().max_indexing_threads);
                let budget = ResourceBudget::new(desired_io, desired_io);
                let permit = budget.try_acquire(0, desired_io).ok_or_else(|| {
                    OperationError::service_error(format!(
                        "failed to acquire resource permit for {} optimizer",
                        optimizer.name(),
                    ))
                })?;

                let (_, progress) = new_progress_tracker();
                let points_optimized = optimizer.as_ref().optimize(
                    self.segments.clone(),
                    segment_ids,
                    Uuid::new_v4(),
                    permit,
                    budget,
                    &stopped,
                    progress,
                    Box::new(|| ()),
                )?;

                if points_optimized > 0 {
                    optimized_in_iteration = true;
                    optimized_any = true;
                }
            }

            // Avoid repeating the same plan forever if no optimizer made effective progress.
            if !optimized_in_iteration {
                return Ok(optimized_any);
            }
        }
    }

    fn build_blocking_optimizers(&self) -> OperationResult<Vec<Arc<Optimizer>>> {
        let segments_path = self.path.join(SEGMENTS_PATH);
        let temp_segments_path = self.path.join(EDGE_OPTIMIZER_TEMP_PATH);
        self.reset_temp_segments_dir(&temp_segments_path)?;

        let hnsw_config = HnswConfig::default();
        let hnsw_global_config = HnswGlobalConfig::default();
        let segment_optimizer_config = self.build_segment_optimizer_config(hnsw_config);
        let threshold_config = self.default_optimizer_thresholds(hnsw_config);
        let default_segments_number = default_segment_number();

        Ok(vec![
            Arc::new(MergeOptimizer::new(
                default_segments_number,
                threshold_config,
                segments_path.clone(),
                temp_segments_path.clone(),
                segment_optimizer_config.clone(),
                hnsw_config,
                hnsw_global_config.clone(),
            )),
            Arc::new(IndexingOptimizer::new(
                default_segments_number,
                threshold_config,
                segments_path.clone(),
                temp_segments_path.clone(),
                segment_optimizer_config.clone(),
                hnsw_config,
                hnsw_global_config.clone(),
            )),
            Arc::new(VacuumOptimizer::new(
                DEFAULT_DELETED_THRESHOLD,
                DEFAULT_VACUUM_MIN_VECTOR_NUMBER,
                threshold_config,
                segments_path.clone(),
                temp_segments_path.clone(),
                segment_optimizer_config.clone(),
                hnsw_config,
                hnsw_global_config.clone(),
            )),
            Arc::new(ConfigMismatchOptimizer::new(
                threshold_config,
                segments_path,
                temp_segments_path,
                segment_optimizer_config,
                hnsw_config,
                hnsw_global_config,
            )),
        ])
    }

    fn default_optimizer_thresholds(&self, hnsw_config: HnswConfig) -> OptimizerThresholds {
        let indexing_threads = num_rayon_threads(hnsw_config.max_indexing_threads);
        OptimizerThresholds {
            memmap_threshold_kb: usize::MAX,
            indexing_threshold_kb: DEFAULT_INDEXING_THRESHOLD_KB,
            max_segment_size_kb: indexing_threads.saturating_mul(DEFAULT_MAX_SEGMENT_PER_CPU_KB),
        }
    }

    fn build_segment_optimizer_config(
        &self,
        default_hnsw_config: HnswConfig,
    ) -> SegmentOptimizerConfig {
        let base_vector_data = self
            .config
            .vector_data
            .iter()
            .map(|(name, config)| {
                let mut config = config.clone();
                config.index = Indexes::Plain {};
                config.storage_type = if config.storage_type.is_on_disk() {
                    VectorStorageType::ChunkedMmap
                } else {
                    VectorStorageType::InRamChunkedMmap
                };
                config.quantization_config = common::flags::feature_flags()
                    .appendable_quantization
                    .then(|| {
                        config
                            .quantization_config
                            .as_ref()
                            .filter(|quantization| quantization.supports_appendable())
                            .cloned()
                    })
                    .flatten();
                (name.clone(), config)
            })
            .collect();

        let base_sparse_vector_data = self
            .config
            .sparse_vector_data
            .iter()
            .map(|(name, config)| {
                let mut config = *config;
                config.index.index_type = SparseIndexType::MutableRam;
                (name.clone(), config)
            })
            .collect();

        let dense_vector = self
            .config
            .vector_data
            .iter()
            .map(|(name, config)| {
                let target_hnsw = match &config.index {
                    Indexes::Plain {} => default_hnsw_config,
                    Indexes::Hnsw(hnsw) => *hnsw,
                };

                (
                    name.clone(),
                    DenseVectorOptimizerConfig {
                        on_disk: Some(config.storage_type.is_on_disk()),
                        hnsw_config: target_hnsw,
                        quantization_config: config.quantization_config.clone(),
                    },
                )
            })
            .collect();

        let sparse_vector = self
            .config
            .sparse_vector_data
            .iter()
            .map(|(name, config)| {
                (
                    name.clone(),
                    SparseVectorOptimizerConfig {
                        on_disk: Some(config.index.index_type.is_on_disk()),
                    },
                )
            })
            .collect();

        SegmentOptimizerConfig {
            payload_storage_type: self.config.payload_storage_type,
            base_vector_data,
            base_sparse_vector_data,
            dense_vector,
            sparse_vector,
        }
    }

    fn reset_temp_segments_dir(&self, temp_segments_path: &std::path::Path) -> OperationResult<()> {
        if temp_segments_path.exists() {
            fs::remove_dir_all(temp_segments_path).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to clear edge optimizer temp directory {}: {err}",
                    temp_segments_path.display(),
                ))
            })?;
        }

        fs::create_dir_all(temp_segments_path).map_err(|err| {
            OperationError::service_error(format!(
                "failed to create edge optimizer temp directory {}: {err}",
                temp_segments_path.display(),
            ))
        })?;

        Ok(())
    }
}

fn default_segment_number() -> usize {
    let expected_segments = common::cpu::get_num_cpus() / 2;
    expected_segments.clamp(2, 8)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;

    use fs_err as fs;
    use segment::data_types::vectors::{VectorInternal, VectorStructInternal};
    use segment::types::{
        Distance, ExtendedPointId, Indexes, PayloadStorageType, SegmentConfig, VectorDataConfig,
        VectorStorageType,
    };
    use shard::operations::CollectionUpdateOperations::PointOperation;
    use shard::operations::point_ops::PointInsertOperationsInternal::PointsList;
    use shard::operations::point_ops::PointOperations::{DeletePoints, UpsertPoints};
    use shard::operations::point_ops::PointStructPersisted;
    use uuid::Uuid;

    use crate::EdgeShard;

    const VECTOR_NAME: &str = "edge-test-vector";

    #[test]
    fn does_not_force_merge_all_segments_into_one() {
        let dir = tempfile::Builder::new()
            .prefix("edge-opt-do-not-force-one")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::load(dir.path(), Some(test_config())).unwrap();
        shard
            .update(PointOperation(UpsertPoints(PointsList(vec![point(1)]))))
            .unwrap();
        drop(shard);

        duplicate_single_segment(dir.path());

        let reopened = EdgeShard::load(dir.path(), None).unwrap();
        assert_eq!(reopened.info().segments_count, 2);

        let optimized = reopened.optimize_all_segments_blocking().unwrap();
        assert!(!optimized, "optimizer should not force-merge all segments");
        assert_eq!(reopened.info().segments_count, 2);
    }

    #[test]
    fn vacuum_optimizer_runs_in_blocking_mode_until_idle() {
        let dir = tempfile::Builder::new()
            .prefix("edge-opt-vacuum")
            .tempdir()
            .unwrap();

        let shard = EdgeShard::load(dir.path(), Some(test_config())).unwrap();

        let points = (1..=1000).map(point).collect::<Vec<_>>();
        shard
            .update(PointOperation(UpsertPoints(PointsList(points))))
            .unwrap();

        let deleted_ids = (1..=200)
            .map(|id| ExtendedPointId::NumId(id as u64))
            .collect::<Vec<_>>();
        shard
            .update(PointOperation(DeletePoints { ids: deleted_ids }))
            .unwrap();

        let optimized = shard.optimize_all_segments_blocking().unwrap();
        assert!(optimized, "vacuum candidate should be optimized");

        let optimized_again = shard.optimize_all_segments_blocking().unwrap();
        assert!(
            !optimized_again,
            "second run should be idle after blocking optimization"
        );
    }

    fn test_config() -> SegmentConfig {
        SegmentConfig {
            vector_data: {
                let mut vectors = HashMap::new();
                vectors.insert(
                    VECTOR_NAME.to_string(),
                    VectorDataConfig {
                        size: 1,
                        distance: Distance::Dot,
                        storage_type: VectorStorageType::ChunkedMmap,
                        index: Indexes::Plain {},
                        quantization_config: None,
                        multivector_config: None,
                        datatype: None,
                    },
                );
                vectors
            },
            sparse_vector_data: HashMap::new(),
            payload_storage_type: PayloadStorageType::Mmap,
        }
    }

    fn point(id: u64) -> PointStructPersisted {
        let mut vectors = HashMap::new();
        vectors.insert(
            VECTOR_NAME.to_string(),
            VectorInternal::from(vec![id as f32]),
        );
        PointStructPersisted {
            id: ExtendedPointId::NumId(id),
            vector: VectorStructInternal::Named(vectors).into(),
            payload: None,
        }
    }

    fn duplicate_single_segment(shard_dir: &Path) {
        let segments_path = shard_dir.join("segments");
        let segment_dirs = fs::read_dir(&segments_path)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_dir())
            .collect::<Vec<_>>();
        assert_eq!(segment_dirs.len(), 1, "expected exactly one source segment");

        let source = &segment_dirs[0];
        let target = segments_path.join(Uuid::new_v4().to_string());
        copy_dir_recursive(source, &target);
    }

    fn copy_dir_recursive(from: &Path, to: &Path) {
        fs::create_dir_all(to).unwrap();
        for entry in fs::read_dir(from).unwrap().filter_map(Result::ok) {
            let from_path = entry.path();
            let to_path = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_dir_recursive(&from_path, &to_path);
            } else {
                fs::copy(&from_path, &to_path).unwrap();
            }
        }
    }
}
