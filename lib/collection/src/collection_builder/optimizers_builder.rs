use crate::update_handler::update_handler::Optimizer;
use std::sync::Arc;
use crate::segment_manager::optimizers::vacuum_optimizer::VacuumOptimizer;
use segment::types::SegmentConfig;
use crate::segment_manager::optimizers::merge_optimizer::MergeOptimizer;
use std::path::Path;

pub fn build_optimizers(
    collection_path: &Path,
    segment_config: SegmentConfig,
    deleted_threshold: f64,
    vacuum_min_vector_number: usize,
    max_segment_number: usize,
) -> Arc<Vec<Box<Optimizer>>> {

    let segments_path = collection_path.join("segments");

    Arc::new(vec![
        Box::new(
            MergeOptimizer::new(
                max_segment_number,
                segments_path.clone(),
                segment_config.clone()
            )
        ),
        Box::new(VacuumOptimizer::new(
            deleted_threshold,
            vacuum_min_vector_number,
            segments_path.clone(),
            segment_config
        ))
    ])
}