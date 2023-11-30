mod build_cache;
pub mod build_condition_checker;
mod config;
mod entry_points;
pub mod graph_layers;
pub mod graph_layers_builder;
pub mod graph_links;
pub mod hnsw;
pub mod point_scorer;
mod search_context;

#[cfg(test)]
mod tests;

/// Default number of CPUs for HNSW graph building and optimization tasks in general.
///
/// Even on high-CPU systems, a value higher than 16 is discouraged. It will most likely not
/// improve performance and is more likely to cause disconnected HNSW graphs.
/// Will be less if currently available CPU budget is lower.
const MAX_AUTO_RAYON_THREADS: usize = 8;

pub fn max_rayon_threads(max_indexing_threads: usize) -> usize {
    if max_indexing_threads == 0 {
        let num_cpu = common::cpu::get_num_cpus();
        num_cpu.clamp(1, MAX_AUTO_RAYON_THREADS)
    } else {
        max_indexing_threads
    }
}
