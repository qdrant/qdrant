//! Cumulative counters for one kind of IO operation: requests by outcome, transferred bytes,
//! and a fixed latency histogram. Every layer that meters its requests reuses this primitive
//! and only decides which operations it labels.
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Exclusive upper bounds of the latency histogram; the final bucket has no upper bound.
pub const LATENCY_BOUNDS: [Duration; 8] = [
    Duration::from_millis(1),
    Duration::from_millis(5),
    Duration::from_millis(10),
    Duration::from_millis(25),
    Duration::from_millis(50),
    Duration::from_millis(100),
    Duration::from_millis(500),
    Duration::from_secs(1),
];

pub const LATENCY_BUCKETS: usize = LATENCY_BOUNDS.len() + 1;

/// Cloneable handle to one operation's counters, shared by everything that reports into it.
#[derive(Clone, Debug, Default)]
pub struct OpStats(Arc<Counters>);

#[derive(Debug, Default)]
struct Counters {
    started: AtomicU64,
    completed: AtomicU64,
    not_found: AtomicU64,
    errors: AtomicU64,
    abandoned: AtomicU64,
    bytes: AtomicU64,
    duration_ns: AtomicU64,
    duration_histogram: [AtomicU64; LATENCY_BUCKETS],
}

/// Cumulative snapshot. Fields may reflect slightly different instants during concurrent updates.
/// Once updates settle, `started` equals `completed + not_found + errors + abandoned + in flight`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OpStatsSnapshot {
    /// Operations started.
    pub started: u64,
    /// Operations whose successful result was observed.
    pub completed: u64,
    /// Operations answered with "not found". An expected answer to existence and length
    /// probes, so not counted as an error; no bytes or duration are recorded.
    pub not_found: u64,
    /// Operations that failed.
    pub errors: u64,
    /// Operations dropped without an observed result.
    pub abandoned: u64,
    /// Payload bytes of successful operations: downloaded for reads, uploaded for writes.
    /// Excludes hidden retries and partial failed transfers.
    pub bytes: u64,
    /// Sum of successful operation durations, from start to observed completion.
    pub total_duration: Duration,
    /// Non-cumulative bucket counts for successful operation durations.
    /// Buckets are `[lower, upper)`, starting at zero; the last bucket is unbounded.
    /// See [`LATENCY_BOUNDS`] for the exclusive upper bounds.
    pub duration_histogram: [u64; LATENCY_BUCKETS],
}

impl OpStats {
    pub fn snapshot(&self) -> OpStatsSnapshot {
        OpStatsSnapshot {
            started: self.0.started.load(Ordering::Relaxed),
            completed: self.0.completed.load(Ordering::Relaxed),
            not_found: self.0.not_found.load(Ordering::Relaxed),
            errors: self.0.errors.load(Ordering::Relaxed),
            abandoned: self.0.abandoned.load(Ordering::Relaxed),
            bytes: self.0.bytes.load(Ordering::Relaxed),
            total_duration: Duration::from_nanos(self.0.duration_ns.load(Ordering::Relaxed)),
            duration_histogram: std::array::from_fn(|i| {
                self.0.duration_histogram[i].load(Ordering::Relaxed)
            }),
        }
    }

    /// Count one operation as started at `started`. The returned guard reports its outcome,
    /// or counts it as abandoned when dropped without one.
    pub fn start(&self, started: Instant) -> OpGuard {
        self.0.started.fetch_add(1, Ordering::Relaxed);
        OpGuard {
            stats: self.clone(),
            started,
            finished: false,
        }
    }

    fn record_completed(&self, bytes: usize, duration: Duration) {
        self.0.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        self.0.duration_ns.fetch_add(
            duration.as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        let bucket = LATENCY_BOUNDS.partition_point(|bound| duration >= *bound);
        self.0.duration_histogram[bucket].fetch_add(1, Ordering::Relaxed);
        self.0.completed.fetch_add(1, Ordering::Relaxed);
    }
}

impl OpStatsSnapshot {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// Average observed duration of successful operations; absent before any complete.
    pub fn avg_duration(&self) -> Option<Duration> {
        (self.completed != 0).then(|| {
            Duration::from_nanos(
                (self.total_duration.as_nanos() / u128::from(self.completed)) as u64,
            )
        })
    }

    /// Interval counters against an earlier snapshot of the same counters.
    /// Concurrent background work is included; this is not per-request attribution.
    pub fn delta_since(&self, earlier: &Self) -> Self {
        Self {
            started: self.started.saturating_sub(earlier.started),
            completed: self.completed.saturating_sub(earlier.completed),
            not_found: self.not_found.saturating_sub(earlier.not_found),
            errors: self.errors.saturating_sub(earlier.errors),
            abandoned: self.abandoned.saturating_sub(earlier.abandoned),
            bytes: self.bytes.saturating_sub(earlier.bytes),
            total_duration: self.total_duration.saturating_sub(earlier.total_duration),
            duration_histogram: std::array::from_fn(|i| {
                self.duration_histogram[i].saturating_sub(earlier.duration_histogram[i])
            }),
        }
    }

    /// One line of `key=value` counters, omitting zeros; `None` when nothing was recorded.
    pub fn format_summary(&self) -> Option<String> {
        let mut fields = Vec::new();
        for (label, value) in [
            ("started", self.started),
            ("completed", self.completed),
            ("bytes", self.bytes),
            ("not_found", self.not_found),
            ("errors", self.errors),
            ("abandoned", self.abandoned),
        ] {
            if value != 0 {
                fields.push(format!("{label}={value}"));
            }
        }
        if let Some(average) = self.avg_duration().filter(|d| !d.is_zero()) {
            fields.push(format!("avg={average:.3?}"));
        }
        (!fields.is_empty()).then(|| fields.join(" "))
    }

    /// Non-empty histogram buckets as `(label, count)`, labels in milliseconds.
    pub fn histogram_rows(&self) -> impl Iterator<Item = (String, u64)> + '_ {
        self.duration_histogram
            .iter()
            .enumerate()
            .filter(|(_, count)| **count != 0)
            .map(|(i, &count)| {
                let label = if i == 0 {
                    format!("<{}", LATENCY_BOUNDS[0].as_millis())
                } else if i == LATENCY_BOUNDS.len() {
                    format!(">={}", LATENCY_BOUNDS[i - 1].as_millis())
                } else {
                    format!(
                        "{}-{}",
                        LATENCY_BOUNDS[i - 1].as_millis(),
                        LATENCY_BOUNDS[i].as_millis()
                    )
                };
                (label, count)
            })
    }

    /// Summary line followed by an indented latency histogram; `None` when nothing was recorded.
    pub fn format_compact(&self) -> Option<String> {
        let mut out = self.format_summary()?;
        let max_count = self.duration_histogram.iter().copied().max().unwrap_or(0);
        if max_count == 0 {
            return Some(out);
        }
        out.push_str("\n  latency (ms, upper bounds exclusive):");
        for (label, count) in self.histogram_rows() {
            // Scale to the busiest bucket; keep every non-empty bucket visible.
            let width = (u128::from(count) * 20).div_ceil(u128::from(max_count)) as usize;
            out.push_str(&format!(
                "\n  {label:>9} | {:<20} {count}",
                "#".repeat(width)
            ));
        }
        Some(out)
    }
}

impl std::ops::AddAssign<&OpStatsSnapshot> for OpStatsSnapshot {
    fn add_assign(&mut self, other: &Self) {
        self.started += other.started;
        self.completed += other.completed;
        self.not_found += other.not_found;
        self.errors += other.errors;
        self.abandoned += other.abandoned;
        self.bytes += other.bytes;
        self.total_duration += other.total_duration;
        for (bucket, count) in self
            .duration_histogram
            .iter_mut()
            .zip(other.duration_histogram)
        {
            *bucket += count;
        }
    }
}

/// One in-flight operation. Reports completion or failure explicitly; a guard dropped
/// without either counts the operation as abandoned, without guessing whether the
/// underlying work stopped.
#[derive(Debug)]
pub struct OpGuard {
    stats: OpStats,
    started: Instant,
    finished: bool,
}

impl OpGuard {
    pub fn complete(mut self, bytes: usize) {
        self.finished = true;
        self.stats.record_completed(bytes, self.started.elapsed());
    }

    pub fn not_found(mut self) {
        self.finished = true;
        self.stats.0.not_found.fetch_add(1, Ordering::Relaxed);
    }

    pub fn failed(mut self) {
        self.finished = true;
        self.stats.0.errors.fetch_add(1, Ordering::Relaxed);
    }
}

impl Drop for OpGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.stats.0.abandoned.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_boundaries_and_overflow() {
        let stats = OpStats::default();
        stats.record_completed(0, Duration::ZERO);
        for bound in LATENCY_BOUNDS {
            stats.record_completed(0, bound - Duration::from_nanos(1));
            stats.record_completed(0, bound);
        }
        stats.record_completed(0, Duration::from_secs(60));
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.duration_histogram, [2; LATENCY_BUCKETS]);
        assert_eq!(
            snapshot.duration_histogram.iter().sum::<u64>(),
            snapshot.completed
        );
    }

    #[test]
    fn guard_outcomes() {
        let stats = OpStats::default();
        stats.start(Instant::now()).complete(10);
        stats.start(Instant::now()).not_found();
        stats.start(Instant::now()).failed();
        drop(stats.start(Instant::now()));
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.started, 4);
        assert_eq!(snapshot.completed, 1);
        assert_eq!(snapshot.not_found, 1);
        assert_eq!(snapshot.errors, 1);
        assert_eq!(snapshot.abandoned, 1);
        assert_eq!(snapshot.bytes, 10);
        assert!(!snapshot.is_empty());
        assert!(snapshot.delta_since(&snapshot).is_empty());
    }
}
