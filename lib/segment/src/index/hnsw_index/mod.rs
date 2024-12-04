use common::defaults::thread_count_for_hnsw;

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

#[cfg(feature = "gpu")]
pub mod gpu;

/// Placeholders for GPU logic when the `gpu` feature is not enabled.
#[cfg(not(feature = "gpu"))]
pub mod gpu {
    pub mod gpu_devices_manager {
        /// Placeholder for GPU device to process indexing on.
        pub struct LockedGpuDevice<'a> {
            phantom: std::marker::PhantomData<&'a usize>,
        }
    }

    pub mod gpu_vector_storage {
        /// Placeholder for GPU vector storage.
        pub struct GpuVectorStorage {}
    }
}

#[cfg(test)]
mod tests;

/// Number of threads to use with rayon for HNSW index building.
///
/// Uses [`thread_count_for_hnsw`] heuristic but accepts a `max_indexing_threads` parameter to
/// allow configuring this.
pub fn num_rayon_threads(max_indexing_threads: usize) -> usize {
    if max_indexing_threads == 0 {
        let num_cpu = common::cpu::get_num_cpus();
        num_cpu.clamp(1, thread_count_for_hnsw(num_cpu))
    } else {
        max_indexing_threads
    }
}
