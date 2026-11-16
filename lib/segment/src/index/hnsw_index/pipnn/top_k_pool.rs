//! Top-k buffers of matrix calls, reused across the calls of a phase.

use parking_lot::Mutex;

use crate::vector_storage::matrix_scorer::MatrixTopK;

/// Top-k buffers of the same `k`, handed out to one matrix call at a time and taken back after
/// it. A matrix call doesn't use rayon, so a phase whose calls run on the current pool holds at
/// most one buffer per thread.
///
/// A buffer grows its scratch space to the rows and columns of its first call, megabytes with
/// large vectors (about 7 MB for f32 vectors of dim 1536). Reusing the buffers keeps that to one
/// per thread, where one per call, or per job as `map_init` would give, would allocate it
/// thousands of times.
pub(super) struct TopKPool {
    k: usize,
    free: Mutex<Vec<MatrixTopK>>,
}

impl TopKPool {
    /// An empty pool of buffers that keep `k` per row; buffers are created on demand.
    pub fn new(k: usize) -> Self {
        Self {
            k,
            free: Mutex::default(),
        }
    }

    /// Runs `f` with a free buffer, a new one if none is free, and takes the buffer back after
    /// it, also if `f` returns an error.
    pub fn with<R>(&self, f: impl FnOnce(&mut MatrixTopK) -> R) -> R {
        let mut top_k = self
            .free
            .lock()
            .pop()
            .unwrap_or_else(|| MatrixTopK::new(self.k));
        let result = f(&mut top_k);
        self.free.lock().push(top_k);
        result
    }
}
