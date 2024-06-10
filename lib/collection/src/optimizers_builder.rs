use std::path::Path;
use std::sync::Arc;

use schemars::JsonSchema;
use segment::index::hnsw_index::num_rayon_threads;
use segment::types::{HnswConfig, QuantizationConfig};
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::collection_manager::optimizers::config_mismatch_optimizer::ConfigMismatchOptimizer;
use crate::collection_manager::optimizers::indexing_optimizer::IndexingOptimizer;
use crate::collection_manager::optimizers::merge_optimizer::MergeOptimizer;
use crate::collection_manager::optimizers::segment_optimizer::OptimizerThresholds;
use crate::collection_manager::optimizers::vacuum_optimizer::VacuumOptimizer;
use crate::config::CollectionParams;
use crate::update_handler::Optimizer;

const DEFAULT_MAX_SEGMENT_PER_CPU_KB: usize = 200_000;
pub const DEFAULT_INDEXING_THRESHOLD_KB: usize = 20_000;
const SEGMENTS_PATH: &str = "segments";
const TEMP_SEGMENTS_PATH: &str = "temp_segments";

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, PartialEq)]
pub struct OptimizersConfig {
    /// The minimal fraction of deleted vectors in a segment, required to perform segment optimization
    #[validate(range(min = 0.0, max = 1.0))]
    pub deleted_threshold: f64,
    /// The minimal number of vectors in a segment, required to perform segment optimization
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
    pub max_segment_size: Option<usize>,
    /// Maximum size (in kilobytes) of vectors to store in-memory per segment.
    /// Segments larger than this threshold will be stored as read-only memmaped file.
    ///
    /// Memmap storage is disabled by default, to enable it, set this threshold to a reasonable value.
    ///
    /// To disable memmap storage, set this to `0`. Internally it will use the largest threshold possible.
    ///
    /// Note: 1Kb = 1 vector of size 256
    #[serde(alias = "memmap_threshold_kb")]
    #[serde(default)]
    pub memmap_threshold: Option<usize>,
    /// Maximum size (in kilobytes) of vectors allowed for plain index, exceeding this threshold will enable vector indexing
    ///
    /// Default value is 20,000, based on <https://github.com/google-research/google-research/blob/master/scann/docs/algorithms.md>.
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
}

impl OptimizersConfig {
    #[cfg(test)]
    pub fn fixture() -> Self {
        Self {
            deleted_threshold: 0.1,
            vacuum_min_vector_number: 1000,
            default_segment_number: 0,
            max_segment_size: None,
            memmap_threshold: None,
            indexing_threshold: Some(100_000),
            flush_interval_sec: 60,
            max_optimization_threads: Some(0),
        }
    }

    pub fn get_number_segments(&self) -> usize {
        if self.default_segment_number == 0 {
            let num_cpus = common::cpu::get_num_cpus();
            // Do not configure less than 2 and more than 8 segments
            // until it is not explicitly requested
            num_cpus.clamp(2, 8)
        } else {
            self.default_segment_number
        }
    }

    pub fn optimizer_thresholds(&self, num_indexing_threads: usize) -> OptimizerThresholds {
        let indexing_threshold_kb = match self.indexing_threshold {
            None => DEFAULT_INDEXING_THRESHOLD_KB, // default value
            Some(0) => usize::MAX,                 // disable vector index
            Some(custom) => custom,
        };

        let memmap_threshold_kb = match self.memmap_threshold {
            None | Some(0) => usize::MAX, // default | disable memmap
            Some(custom) => custom,
        };

        OptimizerThresholds {
            memmap_threshold_kb,
            indexing_threshold_kb,
            max_segment_size_kb: self.get_max_segment_size_in_kilobytes(num_indexing_threads),
        }
    }

    pub fn get_max_segment_size_in_kilobytes(&self, num_indexing_threads: usize) -> usize {
        if let Some(max_segment_size) = self.max_segment_size {
            max_segment_size
        } else {
            num_indexing_threads.saturating_mul(DEFAULT_MAX_SEGMENT_PER_CPU_KB)
        }
    }
}

pub fn clear_temp_segments(shard_path: &Path) {
    let temp_segments_path = shard_path.join(TEMP_SEGMENTS_PATH);
    if temp_segments_path.exists() {
        log::debug!("Removing temp_segments directory: {:?}", temp_segments_path);
        if let Err(err) = std::fs::remove_dir_all(&temp_segments_path) {
            log::warn!(
                "Failed to remove temp_segments directory: {:?}, error: {:?}",
                temp_segments_path,
                err
            );
        }
    }
}

pub fn build_optimizers(
    shard_path: &Path,
    collection_params: &CollectionParams,
    optimizers_config: &OptimizersConfig,
    hnsw_config: &HnswConfig,
    quantization_config: &Option<QuantizationConfig>,
) -> Arc<Vec<Arc<Optimizer>>> {
    let num_indexing_threads = num_rayon_threads(hnsw_config.max_indexing_threads);
    let segments_path = shard_path.join(SEGMENTS_PATH);
    let temp_segments_path = shard_path.join(TEMP_SEGMENTS_PATH);
    let threshold_config = optimizers_config.optimizer_thresholds(num_indexing_threads);

    Arc::new(vec![
        Arc::new(MergeOptimizer::new(
            optimizers_config.get_number_segments(),
            threshold_config.clone(),
            segments_path.clone(),
            temp_segments_path.clone(),
            collection_params.clone(),
            hnsw_config.clone(),
            quantization_config.clone(),
        )),
        Arc::new(IndexingOptimizer::new(
            optimizers_config.get_number_segments(),
            threshold_config.clone(),
            segments_path.clone(),
            temp_segments_path.clone(),
            collection_params.clone(),
            hnsw_config.clone(),
            quantization_config.clone(),
        )),
        Arc::new(VacuumOptimizer::new(
            optimizers_config.deleted_threshold,
            optimizers_config.vacuum_min_vector_number,
            threshold_config.clone(),
            segments_path.clone(),
            temp_segments_path.clone(),
            collection_params.clone(),
            hnsw_config.clone(),
            quantization_config.clone(),
        )),
        Arc::new(ConfigMismatchOptimizer::new(
            threshold_config,
            segments_path,
            temp_segments_path,
            collection_params.clone(),
            hnsw_config.clone(),
            quantization_config.clone(),
        )),
    ])
}
