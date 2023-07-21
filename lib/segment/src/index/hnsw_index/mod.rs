mod build_cache;
pub mod build_condition_checker;
mod config;
mod entry_points;
pub mod graph_layers;
pub mod graph_layers_builder;
pub mod graph_linear_builder;
pub mod graph_links;
pub mod hnsw;
pub mod point_scorer;
mod search_context;
pub mod recorder;

#[cfg(test)]
mod tests;

pub fn max_rayon_threads(max_indexing_threads: usize) -> usize {
    if max_indexing_threads == 0 {
        let num_cpu = crate::common::cpu::get_num_cpus();
        std::cmp::max(1, num_cpu - 1)
    } else {
        max_indexing_threads
    }
}
