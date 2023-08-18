pub mod combined_graph_builder;
pub mod gpu_builder_context;
pub mod gpu_graph_builder;
pub mod gpu_links;
pub mod gpu_search_context;
pub mod gpu_vector_storage;

use std::sync::atomic::{AtomicBool, Ordering};

static GPU_INDEXING: AtomicBool = AtomicBool::new(false);

pub fn set_gpu_indexing(gpu_indexing: bool) {
    GPU_INDEXING.store(gpu_indexing, Ordering::Relaxed);
}

pub fn get_gpu_indexing() -> bool {
    GPU_INDEXING.load(Ordering::Relaxed)
}
