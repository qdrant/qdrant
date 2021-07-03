use crate::config::CollectionParams;
use crate::segment_manager::optimizers::indexing_optimizer::IndexingOptimizer;
use crate::segment_manager::optimizers::merge_optimizer::MergeOptimizer;
use crate::segment_manager::optimizers::segment_optimizer::OptimizerThresholds;
use crate::segment_manager::optimizers::vacuum_optimizer::VacuumOptimizer;
use crate::update_handler::Optimizer;
use schemars::JsonSchema;
use segment::types::HnswConfig;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct OptimizersConfig {
    /// The minimal fraction of deleted vectors in a segment, required to perform segment optimization
    pub deleted_threshold: f64,
    /// The minimal number of vectors in a segment, required to perform segment optimization
    pub vacuum_min_vector_number: usize,
    /// If the number of segments exceeds this value, the optimizer will merge the smallest segments.
    pub max_segment_number: usize,
    /// Maximum number of vectors to store in-memory per segment.
    /// Segments larger than this threshold will be stored as read-only memmaped file.
    pub memmap_threshold: usize,
    /// Maximum number of vectors allowed for plain index.
    /// Default value based on https://github.com/google-research/google-research/blob/master/scann/docs/algorithms.md
    pub indexing_threshold: usize,
    /// Starting from this amount of vectors per-segment the engine will start building index for payload.
    pub payload_indexing_threshold: usize,
    /// Minimum interval between forced flushes.
    pub flush_interval_sec: u64,
}

pub fn build_optimizers(
    collection_path: &Path,
    collection_params: &CollectionParams,
    optimizers_config: &OptimizersConfig,
    hnsw_config: &HnswConfig,
) -> Arc<Vec<Box<Optimizer>>> {
    let segments_path = collection_path.join("segments");
    let temp_segments_path = collection_path.join("temp_segments");

    let threshold_config = OptimizerThresholds {
        memmap_threshold: optimizers_config.memmap_threshold,
        indexing_threshold: optimizers_config.indexing_threshold,
        payload_indexing_threshold: optimizers_config.payload_indexing_threshold,
    };

    Arc::new(vec![
        Box::new(IndexingOptimizer::new(
            threshold_config.clone(),
            segments_path.clone(),
            temp_segments_path.clone(),
            collection_params.clone(),
            *hnsw_config,
        )),
        Box::new(MergeOptimizer::new(
            optimizers_config.max_segment_number,
            threshold_config.clone(),
            segments_path.clone(),
            temp_segments_path.clone(),
            collection_params.clone(),
            *hnsw_config,
        )),
        Box::new(VacuumOptimizer::new(
            optimizers_config.deleted_threshold,
            optimizers_config.vacuum_min_vector_number,
            threshold_config,
            segments_path,
            temp_segments_path,
            collection_params.clone(),
            *hnsw_config,
        )),
    ])
}
