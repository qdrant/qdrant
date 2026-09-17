use common::defaults::thread_count_for_hnsw;
use point_scorer::{FilteredBytesScorer, FilteredScorer};

use crate::vector_storage::query_scorer::QueryScorerBytes;

pub mod build_condition_checker;
mod config;
mod entry_points;
mod graph;
pub use graph::{HnswGraph, HnswLinksStorage};
pub mod graph_layers;
mod graph_layers_batched;
pub mod graph_layers_builder;
mod graph_layers_healer;
pub mod graph_links;
pub mod hnsw;
mod links_container;
pub mod point_scorer;
mod search_context;

#[cfg(feature = "gpu")]
pub mod gpu;

/// Scorers used for search over [CompressedWithVectors] format.
///
/// [CompressedWithVectors]: graph_links::GraphLinksFormat::CompressedWithVectors
#[derive(Clone, Copy)]
pub struct GraphWithVectorsScorers<'a> {
    pub links: &'a FilteredScorer<'a>,
    pub links_bytes: &'a FilteredBytesScorer<'a>,
    pub base: &'a dyn QueryScorerBytes,
}

/// Maximum number of links per level.
#[derive(Debug, Clone, Copy)]
pub struct HnswM {
    /// M for all levels except level 0.
    pub m: usize,
    /// M for level 0.
    pub m0: usize,
}

impl HnswM {
    /// Explicitly set both `m` and `m0`.
    pub fn new(m: usize, m0: usize) -> Self {
        Self { m, m0 }
    }

    /// Initialize with `m0 = 2 * m`.
    pub fn new2(m: usize) -> Self {
        Self { m, m0: 2 * m }
    }

    pub fn level_m(&self, level: usize) -> usize {
        if level == 0 { self.m0 } else { self.m }
    }
}

/// Placeholders for GPU logic when the `gpu` feature is not enabled.
#[cfg(not(feature = "gpu"))]
pub mod gpu {
    pub mod gpu_devices_manager {
        /// Placeholder for GPU device to process indexing on.
        pub struct LockedGpuDevice<'a> {
            phantom: std::marker::PhantomData<&'a usize>,
        }
    }

    pub mod gpu_insert_context {
        /// Placeholder for GPU insertion context to process indexing on.
        pub struct GpuInsertContext<'a> {
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

/// Longest run of points a rayon job inserts without splitting further during an HNSW build.
///
/// Rayon's adaptive splitter stops splitting once its budget is spent unless a job gets stolen, so
/// with a non-power-of-two thread count one thread can be left with up to half the points while
/// the others idle. Capping the job length keeps the work stealable to the end. Sizing it from the
/// work (about 16 jobs per thread) keeps each thread on long runs of consecutive points.
pub fn hnsw_build_max_par_len(num_points: usize, num_threads: usize) -> usize {
    (num_points / (num_threads.max(1) * 16)).max(1)
}

/// Number of threads to use with rayon for HNSW index building.
///
/// Uses [`thread_count_for_hnsw`] heuristic but accepts a `max_indexing_threads` parameter to
/// allow configuring this.
pub fn get_num_indexing_threads(max_indexing_threads: usize) -> usize {
    if max_indexing_threads == 0 {
        let num_cpu = common::cpu::get_num_cpus();
        num_cpu.clamp(1, thread_count_for_hnsw(num_cpu))
    } else {
        max_indexing_threads
    }
}
