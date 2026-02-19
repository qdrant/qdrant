/// SegmentOptimizer - trait implementing common functionality of the optimizers.
///
/// The implementation and planner logic are defined in `shard::optimizers::segment_optimizer`
/// and re-exported here for backward compatibility.
pub use shard::optimizers::segment_optimizer::{
    OptimizationPlanner, Optimizer, SegmentOptimizer, plan_optimizations,
};
