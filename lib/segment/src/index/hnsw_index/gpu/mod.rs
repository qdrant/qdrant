pub mod gpu_candidates_heap;
pub mod gpu_graph_builder;
pub mod gpu_links;
pub mod gpu_nearest_heap;
pub mod gpu_search_context;
pub mod gpu_vector_storage;
pub mod gpu_visited_flags;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use common::types::{PointOffsetType, ScoredPointOffset};

static GPU_INDEXING: AtomicBool = AtomicBool::new(false);
static GPU_FORCE_HALF_PRECISION: AtomicBool = AtomicBool::new(false);
static GPU_MAX_MEMORY_MB: AtomicUsize = AtomicUsize::new(0);
pub const GPU_MAX_GROUPS_COUNT_DEFAULT: usize = 2048;

pub fn set_gpu_indexing(gpu_indexing: bool) {
    GPU_INDEXING.store(gpu_indexing, Ordering::Relaxed);
}

pub fn get_gpu_indexing() -> bool {
    GPU_INDEXING.load(Ordering::Relaxed)
}

pub fn set_gpu_force_half_precision(force_half_precision: bool) {
    GPU_FORCE_HALF_PRECISION.store(force_half_precision, Ordering::Relaxed);
}

pub fn get_gpu_force_half_precision() -> bool {
    GPU_FORCE_HALF_PRECISION.load(Ordering::Relaxed)
}

pub fn set_gpu_max_memory_mb(memory_mb: usize) {
    GPU_MAX_MEMORY_MB.store(memory_mb, Ordering::Relaxed);
}

pub fn get_gpu_max_memory_mb() -> usize {
    GPU_MAX_MEMORY_MB.load(Ordering::Relaxed)
}

pub fn get_gpu_max_groups_count(
    vector_storage_size: usize,
    has_bq: bool,
    num_vectors: usize,
    m0: usize,
    ef: usize,
) -> usize {
    if get_gpu_max_memory_mb() != 0 {
        let links_size = num_vectors * (m0 + 1) * std::mem::size_of::<PointOffsetType>();
        let nearest_heap_size = ef * std::mem::size_of::<ScoredPointOffset>();
        let candidates_heap_size = 10_000 * std::mem::size_of::<ScoredPointOffset>();
        let visited_flags_size = num_vectors * std::mem::size_of::<u8>();
        let vector_storage_size = if has_bq {
            vector_storage_size.div_ceil(32)
        } else {
            vector_storage_size
        };

        let left_memory =
            (get_gpu_max_memory_mb() * 1024 * 1024).checked_sub(links_size + vector_storage_size);
        if let Some(left_memory) = left_memory {
            std::cmp::min(
                (left_memory / (nearest_heap_size + candidates_heap_size + visited_flags_size))
                    .div_ceil(32)
                    * 32,
                1024 * 32,
            )
        } else {
            128
        }
    } else {
        GPU_MAX_GROUPS_COUNT_DEFAULT
    }
}
