pub mod gpu_candidates_heap;
pub mod gpu_graph_builder;
pub mod gpu_links;
pub mod gpu_nearest_heap;
pub mod gpu_search_context;
pub mod gpu_vector_storage;
pub mod gpu_visited_flags;
pub mod graph_patches_dumper;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

static GPU_INDEXING: AtomicBool = AtomicBool::new(false);
static GPU_FORCE_HALF_PRECISION: AtomicBool = AtomicBool::new(false);
static GPU_MAX_GROUPS: AtomicUsize = AtomicUsize::new(256);
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

pub fn set_gpu_max_groups(max_groups: usize) {
    GPU_MAX_GROUPS.store(max_groups, Ordering::Relaxed);
}

pub fn get_gpu_max_groups() -> usize {
    GPU_MAX_GROUPS.load(Ordering::Relaxed)
}
