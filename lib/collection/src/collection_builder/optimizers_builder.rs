use crate::update_handler::update_handler::Optimizer;
use std::sync::Arc;
use crate::segment_manager::optimizers::vacuum_optimizer::VacuumOptimizer;
use segment::types::SegmentConfig;
use crate::segment_manager::optimizers::merge_optimizer::MergeOptimizer;

pub fn build_optimizers(
    segment_config: SegmentConfig,
    deleted_threshold: f64,
    vacuum_min_vector_number: usize,
    max_segment_number: usize,
) -> Arc<Vec<Box<Optimizer>>> {
    Arc::new(vec![
        Box::new(
            MergeOptimizer::new(
                max_segment_number,
                segment_config.clone()
            )
        ),
        Box::new(VacuumOptimizer::new(
            deleted_threshold,
            vacuum_min_vector_number,
            segment_config
        ))
    ])
}