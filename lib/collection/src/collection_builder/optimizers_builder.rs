use crate::update_handler::update_handler::Optimizer;
use std::sync::Arc;
use crate::segment_manager::optimizers::vacuum_optimizer::VacuumOptimizer;
use segment::types::SegmentConfig;
use crate::segment_manager::optimizers::merge_optimizer::MergeOptimizer;
use std::path::Path;
use serde::{Deserialize, Serialize};


#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct OptimizersConfig {
    pub deleted_threshold: f64,
    pub vacuum_min_vector_number: usize,
    pub max_segment_number: usize,
    pub memmap_threshold: usize,
    pub indexing_threshold: usize,
    pub flush_interval_sec: u64,
}


pub fn build_optimizers(
    collection_path: &Path,
    segment_config: &SegmentConfig,
    optimizers_config: &OptimizersConfig,
) -> Arc<Vec<Box<Optimizer>>> {
    let segments_path = collection_path.join("segments");

    Arc::new(vec![
        Box::new(
            MergeOptimizer::new(
                optimizers_config.max_segment_number,
                optimizers_config.memmap_threshold,
                optimizers_config.indexing_threshold,
                segments_path.clone(),
                segment_config.clone(),
            )
        ),
        Box::new(VacuumOptimizer::new(
            optimizers_config.deleted_threshold,
            optimizers_config.vacuum_min_vector_number,
            segments_path.clone(),
            segment_config.clone(),
        ))
    ])
}
