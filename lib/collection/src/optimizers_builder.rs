use std::path::Path;
use std::sync::Arc;

use schemars::JsonSchema;
use segment::common::cpu::get_num_cpus;
use segment::types::{HnswConfig, QuantizationConfig};
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::collection_manager::optimizers::indexing_optimizer::IndexingOptimizer;
use crate::collection_manager::optimizers::merge_optimizer::MergeOptimizer;
use crate::collection_manager::optimizers::segment_optimizer::OptimizerThresholds;
use crate::collection_manager::optimizers::sparse_index_optimizer::SparseIndexOptimizer;
use crate::collection_manager::optimizers::vacuum_optimizer::VacuumOptimizer;
use crate::config::CollectionParams;
use crate::update_handler::Optimizer;

const DEFAULT_MAX_SEGMENT_PER_CPU_KB: usize = 200_000;

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
    /// Do not create segments larger this size (in KiloBytes).
    /// Large segments might require disproportionately long indexation times,
    /// therefore it makes sense to limit the size of segments.
    ///
    /// If indexation speed have more priority for your - make this parameter lower.
    /// If search speed is more important - make this parameter higher.
    /// Note: 1Kb = 1 vector of size 256
    /// If not set, will be automatically selected considering the number of available CPUs.
    #[serde(alias = "max_segment_size_kb")]
    #[serde(default)]
    pub max_segment_size: Option<usize>,
    /// Maximum size (in KiloBytes) of vectors to store in-memory per segment.
    /// Segments larger than this threshold will be stored as read-only memmaped file.
    /// To enable memmap storage, lower the threshold
    /// Note: 1Kb = 1 vector of size 256
    /// If not set, mmap will not be used.
    #[serde(alias = "memmap_threshold_kb")]
    #[serde(default)]
    #[validate(range(min = 1000))]
    pub memmap_threshold: Option<usize>,
    /// Maximum size (in KiloBytes) of vectors allowed for plain index.
    /// Default value based on <https://github.com/google-research/google-research/blob/master/scann/docs/algorithms.md>
    /// Note: 1Kb = 1 vector of size 256
    #[serde(alias = "indexing_threshold_kb")]
    #[validate(range(min = 1000))]
    pub indexing_threshold: Option<usize>,
    /// Minimum interval between forced flushes.
    pub flush_interval_sec: u64,
    /// Maximum available threads for optimization workers
    pub max_optimization_threads: usize,
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
            max_optimization_threads: 0,
        }
    }

    pub fn get_number_segments(&self) -> usize {
        if self.default_segment_number == 0 {
            let num_cpus = get_num_cpus();
            // Do not configure less than 2 and more than 8 segments
            // until it is not explicitly requested
            num_cpus.clamp(2, 8)
        } else {
            self.default_segment_number
        }
    }

    pub fn get_max_segment_size(&self) -> usize {
        if let Some(max_segment_size) = self.max_segment_size {
            max_segment_size
        } else {
            let num_cpus = get_num_cpus();
            num_cpus.saturating_mul(DEFAULT_MAX_SEGMENT_PER_CPU_KB)
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
    let segments_path = shard_path.join("segments");
    let temp_segments_path = shard_path.join("temp_segments");

    let threshold_config = OptimizerThresholds {
        memmap_threshold: optimizers_config.memmap_threshold.unwrap_or(usize::MAX),
        indexing_threshold: optimizers_config.indexing_threshold.unwrap_or(usize::MAX),
        max_segment_size: optimizers_config.get_max_segment_size(),
    };

    Arc::new(vec![
        Arc::new(MergeOptimizer::new(
            optimizers_config.get_number_segments(),
            threshold_config.clone(),
            segments_path.clone(),
            temp_segments_path.clone(),
            collection_params.clone(),
            *hnsw_config,
            quantization_config.clone(),
        )),
        Arc::new(IndexingOptimizer::new(
            threshold_config.clone(),
            segments_path.clone(),
            temp_segments_path.clone(),
            collection_params.clone(),
            *hnsw_config,
            quantization_config.clone(),
        )),
        Arc::new(SparseIndexOptimizer::new(
            optimizers_config.deleted_threshold,
            optimizers_config.vacuum_min_vector_number,
            threshold_config.clone(),
            segments_path.clone(),
            temp_segments_path.clone(),
            collection_params.clone(),
            *hnsw_config,
            quantization_config.clone(),
        )),
        Arc::new(VacuumOptimizer::new(
            optimizers_config.deleted_threshold,
            optimizers_config.vacuum_min_vector_number,
            threshold_config,
            segments_path,
            temp_segments_path,
            collection_params.clone(),
            *hnsw_config,
            quantization_config.clone(),
        )),
    ])
}
