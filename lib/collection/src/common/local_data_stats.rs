use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Amount of requests that have to be done until the cached data gets updated.
const UPDATE_INTERVAL: usize = 32;

/// A cache for `LocalDataStats` utilizing `AtomicUsize` for better performance.
#[derive(Default)]
pub(crate) struct LocalDataStatsCache {
    stats: LocalDataAtomicStats,
    request_counter: AtomicUsize,
}

impl LocalDataStatsCache {
    pub fn new_with_values(stats: LocalDataStats) -> Self {
        let stats = LocalDataAtomicStats::new(stats);
        Self {
            stats,
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

    /// Returns the cached values. Automatically updates the cache every 32 calls using the given `update` function.
    pub async fn get_or_update_cache<U>(
        &self,
        update_fn: impl FnOnce() -> U,
    ) -> &LocalDataAtomicStats
    where
        U: Future<Output = LocalDataStats>,
    {
        // Update if necessary
        if self.check_need_update_and_increment() {
            let updated = update_fn().await;
            self.update(updated);
        }

        // Give caller access to cached (inner) values which are always updated if required
        &self.stats
    }

    /// Sets all cache values to `new_stats`.
    pub fn update(&self, new_stats: LocalDataStats) {
        self.stats.update(new_stats)
    }
}

/// Same as `LocalDataStats` but each value is atomic.
#[derive(Default)]
pub struct LocalDataAtomicStats {
    vector_storage_size: AtomicUsize,
    payload_storage_size: AtomicUsize,
}

impl LocalDataAtomicStats {
    /// Get the vector storage size.
    pub fn get_vector_storage_size(&self) -> usize {
        self.vector_storage_size.load(Ordering::Relaxed)
    }

    /// Get the payload storage size.
    pub fn get_payload_storage_size(&self) -> usize {
        self.payload_storage_size.load(Ordering::Relaxed)
    }

    fn new(data: LocalDataStats) -> Self {
        let LocalDataStats {
            vector_storage_size,
            payload_storage_size,
        } = data;

        let vector_storage_size = AtomicUsize::new(vector_storage_size);
        let payload_storage_size = AtomicUsize::new(payload_storage_size);
        Self {
            vector_storage_size,
            payload_storage_size,
        }
    }

    fn update(&self, new_values: LocalDataStats) {
        let LocalDataStats {
            vector_storage_size,
            payload_storage_size,
        } = new_values;

        self.vector_storage_size
            .store(vector_storage_size, Ordering::Relaxed);
        self.payload_storage_size
            .store(payload_storage_size, Ordering::Relaxed);
    }
}

/// Statistics for local data, like the size of vector storage.
#[derive(Clone, Copy, Default)]
pub struct LocalDataStats {
    /// Estimated amount of vector storage size.
    pub vector_storage_size: usize,
    /// Estimated amount of payload storage size.
    pub payload_storage_size: usize,
}

impl LocalDataStats {
    pub fn accumulate_from(&mut self, other: &Self) {
        let LocalDataStats {
            vector_storage_size,
            payload_storage_size,
        } = other;

        self.vector_storage_size += vector_storage_size;
        self.payload_storage_size += payload_storage_size;
    }
}
