use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Amount of requests that have to be done until the cached data gets updated.
const UPDATE_INTERVAL: usize = 32;

/// A cache for `LocalDataStats` utilizing `AtomicUsize` for better performance.
#[derive(Default)]
pub(crate) struct CollectionSizeStatsCache {
    stats: Option<CollectionSizeAtomicStats>,

    request_counter: AtomicUsize,
}

impl CollectionSizeStatsCache {
    pub fn new_with_values(stats: Option<CollectionSizeStats>) -> Self {
        let stats = stats.map(CollectionSizeAtomicStats::new);
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
    ) -> Option<&CollectionSizeAtomicStats>
    where
        U: Future<Output = Option<CollectionSizeStats>>,
    {
        // Update if necessary
        if self.check_need_update_and_increment() {
            let updated = update_fn().await?;
            self.update(updated);
        }

        // Give caller access to cached (inner) values which are always updated if required
        self.stats.as_ref()
    }

    /// Sets all cache values to `new_stats`.
    pub fn update(&self, new_stats: CollectionSizeStats) {
        if let Some(stats) = self.stats.as_ref() {
            stats.update(new_stats)
        }
    }
}

/// Same as `LocalDataStats` but each value is atomic.
#[derive(Default)]
pub(crate) struct CollectionSizeAtomicStats {
    vector_storage_size: AtomicUsize,
    payload_storage_size: AtomicUsize,
    points_count: AtomicUsize,
}

impl CollectionSizeAtomicStats {
    /// Get the vector storage size.
    pub fn get_vector_storage_size(&self) -> usize {
        self.vector_storage_size.load(Ordering::Relaxed)
    }

    /// Get the payload storage size.
    pub fn get_payload_storage_size(&self) -> usize {
        self.payload_storage_size.load(Ordering::Relaxed)
    }

    /// Get the points count.
    pub fn get_points_count(&self) -> usize {
        self.points_count.load(Ordering::Relaxed)
    }

    fn new(data: CollectionSizeStats) -> Self {
        let CollectionSizeStats {
            vector_storage_size,
            payload_storage_size,
            points_count,
        } = data;

        Self {
            vector_storage_size: AtomicUsize::new(vector_storage_size),
            payload_storage_size: AtomicUsize::new(payload_storage_size),
            points_count: AtomicUsize::new(points_count),
        }
    }

    fn update(&self, new_stats: CollectionSizeStats) {
        let CollectionSizeStats {
            vector_storage_size,
            payload_storage_size,
            points_count,
        } = new_stats;
        self.vector_storage_size
            .store(vector_storage_size, Ordering::Relaxed);
        self.payload_storage_size
            .store(payload_storage_size, Ordering::Relaxed);
        self.points_count.store(points_count, Ordering::Relaxed);
    }
}

/// Statistics for local data, like the size of vector storage.
#[derive(Clone, Copy, Default)]
pub struct CollectionSizeStats {
    /// Estimated amount of vector storage size.
    pub vector_storage_size: usize,
    /// Estimated amount of payload storage size.
    pub payload_storage_size: usize,
    /// Estimated amount of points.
    pub points_count: usize,
}

impl CollectionSizeStats {
    pub(crate) fn accumulate_metrics_from(&mut self, other: &Self) {
        let CollectionSizeStats {
            vector_storage_size,
            payload_storage_size,
            points_count,
        } = other;

        self.vector_storage_size += vector_storage_size;
        self.payload_storage_size += payload_storage_size;
        self.points_count += points_count;
    }

    pub(crate) fn multiplied_with(self, factor: usize) -> Self {
        let CollectionSizeStats {
            mut vector_storage_size,
            mut payload_storage_size,
            mut points_count,
        } = self;

        vector_storage_size *= factor;
        payload_storage_size *= factor;
        points_count *= factor;

        Self {
            vector_storage_size,
            payload_storage_size,
            points_count,
        }
    }
}
