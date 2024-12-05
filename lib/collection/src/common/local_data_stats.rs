use std::sync::atomic::{AtomicUsize, Ordering};

/// Amount of requests that have to be done until the cached data gets updated.
const UPDATE_INTERVAL: usize = 32;

/// A cache for `LocalDataStats` utilizing `AtomicUsize` for better performance.
#[derive(Default)]
pub(crate) struct LocalDataStatsCache {
    vector_storage_size: AtomicUsize,

    request_counter: AtomicUsize,
}

impl LocalDataStatsCache {
    pub fn new_with_values(stats: LocalDataStats) -> Self {
        let LocalDataStats {
            vector_storage_size,
        } = stats;
        let vector_storage_size = AtomicUsize::new(vector_storage_size);
        Self {
            vector_storage_size,
            request_counter: AtomicUsize::new(1), // Prevent same data getting loaded a second time when doing the first request.
        }
    }

    /// Checks whether the cache needs to be updated.
    /// For performance reasons, this also assumes a cached value gets read afterwards and brings the
    /// Update counter one tick closer to the next update.
    pub fn check_need_update_and_increment(&self) -> bool {
        let req_counter = self.request_counter.fetch_add(1, Ordering::Relaxed);
        req_counter % UPDATE_INTERVAL == 0
    }

    /// Sets all cache values to `new_stats`.
    pub fn update(&self, new_stats: LocalDataStats) {
        let LocalDataStats {
            vector_storage_size,
        } = new_stats;

        self.vector_storage_size
            .store(vector_storage_size, Ordering::Relaxed);
    }

    /// Returns cached vector storage size estimation.
    pub fn get_vector_storage(&self) -> usize {
        self.vector_storage_size.load(Ordering::Relaxed)
    }
}

/// Statistics for local data, like the size of vector storage.
#[derive(Clone, Copy, Default)]
pub struct LocalDataStats {
    /// Estimated amount of vector storage size.
    pub vector_storage_size: usize,
}

impl LocalDataStats {
    pub fn accumulate_from(&mut self, other: &Self) {
        let LocalDataStats {
            vector_storage_size,
        } = other;

        self.vector_storage_size += vector_storage_size;
    }
}
