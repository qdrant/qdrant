/// Optimizer which looks for segments with high amount of soft-deleted points or vectors
///
/// Since the creation of a segment, a lot of points or vectors may have been soft-deleted. This
/// results in the index slowly breaking apart, and unnecessary storage usage.
///
/// This optimizer will look for the worst segment to rebuilt the index and minimize storage usage.
pub use shard::optimizers::vacuum_optimizer::VacuumOptimizer;
