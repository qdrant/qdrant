use once_cell::sync::OnceCell;
use rayon::ThreadPool;

use super::build_thread_pool;
use crate::common::operation_error::OperationResult;

/// The thread pool of a graph build, created when the first method asks for it.
///
/// A build that never links on this machine, e.g. one whose graph comes from a remote indexer,
/// starts no threads.
pub(in crate::index::hnsw_index::hnsw) struct BuildPool {
    num_threads: usize,
    pool: OnceCell<ThreadPool>,
}

impl BuildPool {
    pub fn new(num_threads: usize) -> Self {
        Self {
            num_threads,
            pool: OnceCell::new(),
        }
    }

    pub fn get(&self) -> OperationResult<&ThreadPool> {
        self.pool
            .get_or_try_init(|| build_thread_pool(self.num_threads))
    }
}
