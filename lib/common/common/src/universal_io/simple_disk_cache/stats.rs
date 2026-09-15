//! Shared, cumulative statistics for remote data fetches. These do not count backend HTTP requests,
//! metadata operations, retries, or wire bytes.
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

// Exclusive upper bounds; the final bucket has no upper bound.
const LATENCY_BOUNDS: [Duration; 8] = [
    Duration::from_millis(1),
    Duration::from_millis(5),
    Duration::from_millis(10),
    Duration::from_millis(25),
    Duration::from_millis(50),
    Duration::from_millis(100),
    Duration::from_millis(500),
    Duration::from_secs(1),
];

/// Cloneable observer shared by a filesystem, its clones, and all files it opens.
#[derive(Clone, Debug, Default)]
pub struct DiskCacheStats(Arc<Counters>);

#[derive(Debug, Default)]
struct Counters {
    remote_fetches_started: AtomicU64,
    remote_fetches_completed: AtomicU64,
    remote_fetch_errors: AtomicU64,
    remote_fetches_abandoned: AtomicU64,
    downloaded_bytes: AtomicU64,
    fetch_duration_ns: AtomicU64,
    fetch_duration_histogram: [AtomicU64; LATENCY_BOUNDS.len() + 1],
}

/// Cumulative snapshot. Fields may reflect slightly different instants during concurrent updates.
/// Once updates settle, started fetches equal completed + errors + abandoned + pending.
/// A dropped pipeline may leave backend work running; uncollected results are not counted as downloads.
#[derive(Clone, Debug, Default)]
pub struct DiskCacheStatsSnapshot {
    /// Logical data fetches scheduled or async reads started (including whole-file reads of empty files).
    pub remote_fetches_started: u64,
    /// Successful fetches whose results were observed by the cache.
    pub remote_fetches_completed: u64,
    /// Errors from individual async fetches.
    pub remote_fetch_errors: u64,
    /// Started fetches dropped without an observed result, including pending fetches on pipeline errors.
    pub remote_fetches_abandoned: u64,
    /// Successful response bytes, including alignment and repeated downloads; excludes hidden retries and partial failed responses.
    pub downloaded_bytes: u64,
    /// Sum of successful fetch durations, from scheduling to observing completion.
    /// Includes queueing and delayed collection in `wait`, but excludes local mirror writes.
    pub total_fetch_duration: Duration,
    /// Non-cumulative bucket counts for successful observed fetch durations.
    /// Buckets are [lower, upper), starting at zero; the last bucket is unbounded.
    /// See [`Self::FETCH_DURATION_BUCKET_BOUNDS`] for the exclusive upper bounds.
    /// The sum equals `remote_fetches_completed` once concurrent updates settle.
    pub fetch_duration_histogram: [u64; LATENCY_BOUNDS.len() + 1],
}

impl DiskCacheStats {
    pub fn snapshot(&self) -> DiskCacheStatsSnapshot {
        DiskCacheStatsSnapshot {
            remote_fetches_started: self.0.remote_fetches_started.load(Ordering::Relaxed),
            remote_fetches_completed: self.0.remote_fetches_completed.load(Ordering::Relaxed),
            remote_fetch_errors: self.0.remote_fetch_errors.load(Ordering::Relaxed),
            remote_fetches_abandoned: self.0.remote_fetches_abandoned.load(Ordering::Relaxed),
            downloaded_bytes: self.0.downloaded_bytes.load(Ordering::Relaxed),
            fetch_duration_histogram: std::array::from_fn(|i| {
                self.0.fetch_duration_histogram[i].load(Ordering::Relaxed)
            }),
            total_fetch_duration: Duration::from_nanos(
                self.0.fetch_duration_ns.load(Ordering::Relaxed),
            ),
        }
    }

    fn record_completed(&self, bytes: usize, duration: Duration) {
        self.0
            .downloaded_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.0.fetch_duration_ns.fetch_add(
            duration.as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        let bucket = LATENCY_BOUNDS.partition_point(|bound| duration >= *bound);
        self.0.fetch_duration_histogram[bucket].fetch_add(1, Ordering::Relaxed);
        self.0
            .remote_fetches_completed
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn fetch(&self, started: Instant) -> FetchStats {
        self.0
            .remote_fetches_started
            .fetch_add(1, Ordering::Relaxed);
        FetchStats {
            stats: self.clone(),
            started,
            finished: false,
        }
    }
}

impl DiskCacheStatsSnapshot {
    /// Exclusive upper bounds for `fetch_duration_histogram`, followed by an overflow bucket.
    pub const FETCH_DURATION_BUCKET_BOUNDS: [Duration; LATENCY_BOUNDS.len()] = LATENCY_BOUNDS;

    /// Average observed duration of successful fetches; absent before any complete.
    pub fn avg_fetch_duration(&self) -> Option<Duration> {
        (self.remote_fetches_completed != 0).then(|| {
            Duration::from_nanos(
                (self.total_fetch_duration.as_nanos() / u128::from(self.remote_fetches_completed))
                    as u64,
            )
        })
    }

    /// Interval counters against an earlier snapshot from the same observer.
    /// Concurrent background work is included; this is not per-request attribution.
    pub fn delta_since(&self, earlier: &Self) -> Self {
        Self {
            remote_fetches_started: self
                .remote_fetches_started
                .saturating_sub(earlier.remote_fetches_started),
            remote_fetches_completed: self
                .remote_fetches_completed
                .saturating_sub(earlier.remote_fetches_completed),
            remote_fetch_errors: self
                .remote_fetch_errors
                .saturating_sub(earlier.remote_fetch_errors),
            remote_fetches_abandoned: self
                .remote_fetches_abandoned
                .saturating_sub(earlier.remote_fetches_abandoned),
            downloaded_bytes: self
                .downloaded_bytes
                .saturating_sub(earlier.downloaded_bytes),
            fetch_duration_histogram: std::array::from_fn(|i| {
                self.fetch_duration_histogram[i].saturating_sub(earlier.fetch_duration_histogram[i])
            }),
            total_fetch_duration: self
                .total_fetch_duration
                .saturating_sub(earlier.total_fetch_duration),
        }
    }
}

/// Tracks cancellation and early returns without guessing whether remote work actually stopped.
#[derive(Debug)]
pub(crate) struct FetchStats {
    stats: DiskCacheStats,
    started: Instant,
    finished: bool,
}

impl FetchStats {
    pub(super) fn complete(mut self, bytes: usize) {
        self.finished = true;
        self.stats.record_completed(bytes, self.started.elapsed());
    }

    pub(super) fn failed(mut self) {
        self.finished = true;
        self.stats
            .0
            .remote_fetch_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn result<T: std::ops::Deref<Target = [u8]>>(
        self,
        result: &crate::universal_io::UioResult<T>,
    ) {
        match result {
            Ok(bytes) => self.complete(bytes.len()),
            Err(_) => self.failed(),
        }
    }
}

impl Drop for FetchStats {
    fn drop(&mut self) {
        if !self.finished {
            self.stats
                .0
                .remote_fetches_abandoned
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_boundaries_and_overflow() {
        let stats = DiskCacheStats::default();
        stats.record_completed(0, Duration::ZERO);
        for bound in LATENCY_BOUNDS {
            stats.record_completed(0, bound - Duration::from_nanos(1));
            stats.record_completed(0, bound);
        }
        stats.record_completed(0, Duration::from_secs(60));
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.fetch_duration_histogram, [2; 9]);
        assert_eq!(
            snapshot.fetch_duration_histogram.iter().sum::<u64>(),
            snapshot.remote_fetches_completed
        );
    }
}
