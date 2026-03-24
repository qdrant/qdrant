use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use common::types::PointOffsetType;
use fs_err as fs;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::types::{HnswConfig, HnswGlobalConfig, QuantizationConfig, VectorStorageDatatype};
use serde::{Deserialize, Serialize};
use shard::files::SEGMENTS_PATH;
use shard::operations::optimization::OptimizerThresholds;
use shard::optimizers::config::{
    DEFAULT_DELETED_THRESHOLD, DEFAULT_VACUUM_MIN_VECTOR_NUMBER, DenseVectorOptimizerInput,
    SegmentOptimizerConfig, SparseVectorOptimizerInput, TEMP_SEGMENTS_PATH,
    get_deferred_points_threshold_bytes, get_indexing_threshold_kb, get_max_segment_size_kb,
    get_number_segments,
};
use shard::optimizers::segment_optimizer::max_num_indexing_threads;
use validator::Validate;

use crate::collection_manager::optimizers::config_mismatch_optimizer::ConfigMismatchOptimizer;
use crate::collection_manager::optimizers::indexing_optimizer::IndexingOptimizer;
use crate::collection_manager::optimizers::merge_optimizer::MergeOptimizer;
use crate::collection_manager::optimizers::vacuum_optimizer::VacuumOptimizer;
use crate::config::CollectionParams;
use crate::operations::config_diff::DiffConfig;
use crate::operations::types::{SparseVectorParams, VectorParams};
use crate::update_handler::Optimizer;

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Anonymize, Clone, PartialEq)]
#[anonymize(false)]
pub struct OptimizersConfig {
    /// The minimal fraction of deleted vectors in a segment, required to perform segment optimization
    #[serde(default = "default_deleted_threshold")]
    #[validate(range(min = 0.0, max = 1.0))]
    pub deleted_threshold: f64,
    /// The minimal number of vectors in a segment, required to perform segment optimization
    #[serde(default = "default_vacuum_min_vector_number")]
    #[validate(range(min = 100))]
    pub vacuum_min_vector_number: usize,
    /// Target amount of segments optimizer will try to keep.
    /// Real amount of segments may vary depending on multiple parameters:
    ///  - Amount of stored points
    ///  - Current write RPS
    ///
    /// It is recommended to select default number of segments as a factor of the number of search threads,
    /// so that each segment would be handled evenly by one of the threads.
    /// If `default_segment_number = 0`, will be automatically selected by the number of available CPUs.
    pub default_segment_number: usize,
    /// Do not create segments larger this size (in kilobytes).
    /// Large segments might require disproportionately long indexation times,
    /// therefore it makes sense to limit the size of segments.
    ///
    /// If indexing speed is more important - make this parameter lower.
    /// If search speed is more important - make this parameter higher.
    /// Note: 1Kb = 1 vector of size 256
    /// If not set, will be automatically selected considering the number of available CPUs.
    #[serde(alias = "max_segment_size_kb")]
    #[serde(default)]
    #[validate(range(min = 1))]
    pub max_segment_size: Option<usize>,
    /// Maximum size (in kilobytes) of vectors to store in-memory per segment.
    /// Segments larger than this threshold will be stored as read-only memmapped file.
    ///
    /// Memmap storage is disabled by default, to enable it, set this threshold to a reasonable value.
    ///
    /// To disable memmap storage, set this to `0`. Internally it will use the largest threshold possible.
    ///
    /// Note: 1Kb = 1 vector of size 256
    #[serde(alias = "memmap_threshold_kb")]
    #[serde(default)]
    #[deprecated(since = "1.15.0", note = "Use `on_disk` flags instead")]
    pub memmap_threshold: Option<usize>,
    /// Maximum size (in kilobytes) of vectors allowed for plain index, exceeding this threshold will enable vector indexing
    ///
    /// Default value is 10,000, based on experiments and observations.
    ///
    /// To disable vector indexing, set to `0`.
    ///
    /// Note: 1kB = 1 vector of size 256.
    #[serde(alias = "indexing_threshold_kb")]
    #[serde(default)]
    pub indexing_threshold: Option<usize>,
    /// Minimum interval between forced flushes.
    pub flush_interval_sec: u64,
    /// Max number of threads (jobs) for running optimizations per shard.
    /// Note: each optimization job will also use `max_indexing_threads` threads by itself for index building.
    /// If null - have no limit and choose dynamically to saturate CPU.
    /// If 0 - no optimization threads, optimizations will be disabled.
    #[serde(default)]
    pub max_optimization_threads: Option<usize>,

    /// If this option is set, service will try to prevent creation of large unoptimized segments.
    /// When enabled, updates may be blocked at request level if there are unoptimized segments larger than indexing threshold.
    /// Updates will be resumed when optimization is completed and segments are optimized below the threshold.
    /// Using this option may lead to increased delay between submitting an update and its application.
    /// Default is disabled.
    #[serde(default)]
    pub prevent_unoptimized: Option<bool>,
}

fn default_deleted_threshold() -> f64 {
    DEFAULT_DELETED_THRESHOLD
}

fn default_vacuum_min_vector_number() -> usize {
    DEFAULT_VACUUM_MIN_VECTOR_NUMBER
}

impl OptimizersConfig {
    #[cfg(test)]
    pub fn fixture() -> Self {
        Self {
            deleted_threshold: 0.1,
            vacuum_min_vector_number: 1000,
            default_segment_number: 0,
            max_segment_size: None,
            #[expect(deprecated)]
            memmap_threshold: None,
            indexing_threshold: Some(100_000),
            flush_interval_sec: 60,
            max_optimization_threads: Some(0),
            prevent_unoptimized: None,
        }
    }

    pub fn get_number_segments(&self) -> usize {
        get_number_segments(self.default_segment_number)
    }

    pub fn get_indexing_threshold_kb(&self) -> usize {
        get_indexing_threshold_kb(self.indexing_threshold)
    }

    pub fn optimizer_thresholds(
        &self,
        num_indexing_threads: usize,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> OptimizerThresholds {
        let indexing_threshold_kb = self.get_indexing_threshold_kb();

        #[expect(deprecated)]
        let memmap_threshold_kb = match self.memmap_threshold {
            None | Some(0) => usize::MAX, // default | disable memmap
            Some(custom) => custom,
        };

        OptimizerThresholds {
            memmap_threshold_kb,
            indexing_threshold_kb,
            max_segment_size_kb: get_max_segment_size_kb(
                self.max_segment_size,
                num_indexing_threads,
            ),
            deferred_internal_id,
        }
    }

    pub fn get_max_segment_size_in_kilobytes(&self, num_indexing_threads: usize) -> usize {
        get_max_segment_size_kb(self.max_segment_size, num_indexing_threads)
    }

    pub fn get_deferred_points_threshold_bytes(&self) -> Option<NonZeroUsize> {
        get_deferred_points_threshold_bytes(
            self.prevent_unoptimized,
            self.get_indexing_threshold_kb(),
        )
    }
}

pub fn clear_temp_segments(shard_path: &Path) {
    let temp_segments_path = shard_path.join(TEMP_SEGMENTS_PATH);
    if temp_segments_path.exists() {
        log::debug!("Removing temp_segments directory: {temp_segments_path:?}");
        if let Err(err) = fs::remove_dir_all(&temp_segments_path) {
            log::warn!(
                "Failed to remove temp_segments directory: {temp_segments_path:?}, error: {err:?}"
            );
        }
    }
}

pub fn build_segment_optimizer_config(
    collection_params: &CollectionParams,
    global_hnsw_config: &HnswConfig,
    global_quantization_config: &Option<QuantizationConfig>,
) -> SegmentOptimizerConfig {
    let dense_vectors = collection_params
        .vectors
        .params_iter()
        .map(|(name, params)| {
            let VectorParams {
                size,
                distance,
                hnsw_config,
                quantization_config,
                on_disk,
                datatype,
                multivector_config,
            } = params;

            (
                name.into(),
                DenseVectorOptimizerInput {
                    size: size.get() as usize,
                    distance: *distance,
                    on_disk: *on_disk,
                    hnsw_config: global_hnsw_config.update_opt(hnsw_config.as_ref()),
                    quantization_config: quantization_config
                        .as_ref()
                        .or(global_quantization_config.as_ref())
                        .cloned(),
                    multivector_config: *multivector_config,
                    datatype: datatype.map(VectorStorageDatatype::from),
                },
            )
        })
        .collect();

    let sparse_vectors = collection_params
        .sparse_vectors
        .as_ref()
        .map(|config| {
            config
                .iter()
                .map(|(name, params)| {
                    let SparseVectorParams { index, modifier } = params;

                    (
                        name.clone(),
                        SparseVectorOptimizerInput {
                            on_disk: index.and_then(|index| index.on_disk),
                            full_scan_threshold: index.and_then(|index| index.full_scan_threshold),
                            index_datatype: index
                                .and_then(|index| index.datatype)
                                .map(VectorStorageDatatype::from),
                            storage_type: params.storage_type(),
                            modifier: *modifier,
                        },
                    )
                })
                .collect()
        })
        .unwrap_or_default();

    SegmentOptimizerConfig::new(
        collection_params.payload_storage_type(),
        dense_vectors,
        sparse_vectors,
    )
}

pub fn build_optimizers(
    shard_path: &Path,
    collection_params: &CollectionParams,
    optimizers_config: &OptimizersConfig,
    hnsw_config: &HnswConfig,
    hnsw_global_config: &HnswGlobalConfig,
    quantization_config: &Option<QuantizationConfig>,
) -> Arc<Vec<Arc<Optimizer>>> {
    let segments_path = shard_path.join(SEGMENTS_PATH);
    let temp_segments_path = shard_path.join(TEMP_SEGMENTS_PATH);
    let segment_config =
        build_segment_optimizer_config(collection_params, hnsw_config, quantization_config);
    let num_indexing_threads = max_num_indexing_threads(&segment_config);
    let threshold_config = optimizers_config.optimizer_thresholds(
        num_indexing_threads,
        collection_params.get_deferred_point_id(
            hnsw_config,
            optimizers_config.get_deferred_points_threshold_bytes(),
        ),
    );

    Arc::new(vec![
        Arc::new(MergeOptimizer::new(
            optimizers_config.get_number_segments(),
            threshold_config,
            segments_path.clone(),
            temp_segments_path.clone(),
            segment_config.clone(),
            hnsw_global_config.clone(),
        )),
        Arc::new(IndexingOptimizer::new(
            optimizers_config.get_number_segments(),
            threshold_config,
            segments_path.clone(),
            temp_segments_path.clone(),
            segment_config.clone(),
            hnsw_global_config.clone(),
        )),
        Arc::new(VacuumOptimizer::new(
            optimizers_config.deleted_threshold,
            optimizers_config.vacuum_min_vector_number,
            threshold_config,
            segments_path.clone(),
            temp_segments_path.clone(),
            segment_config.clone(),
            hnsw_global_config.clone(),
        )),
        Arc::new(ConfigMismatchOptimizer::new(
            threshold_config,
            segments_path,
            temp_segments_path,
            segment_config,
            *hnsw_config,
            hnsw_global_config.clone(),
        )),
    ])
}
