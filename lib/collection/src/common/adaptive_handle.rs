//! Adaptive wrapper that routes search `spawn_blocking` calls between two
//! pre-built Tokio runtimes — one with a smaller blocking pool for CPU-bound
//! load and one sized via [`common::defaults::search_thread_count`] for
//! IO-bound load. The choice
//! is re-evaluated lazily on the spawn path, at most once per
//! [`ADJUST_INTERVAL`].
//!
//! - Start in [`SearchMode::HighIo`] (over-committed) so cold starts make
//!   progress on IO without waiting for a CPU sample.
//! - Switch to [`SearchMode::HighCpu`] when the process is over
//!   [`HIGH_CPU_THRESHOLD`] of total cores — extra threads just thrash.
//! - Switch back to [`SearchMode::HighIo`] when CPU drops below
//!   [`LOW_CPU_THRESHOLD`] — search is likely IO-bound and more parallelism
//!   helps.
//!
//! The hot path is two relaxed atomic loads (mode + last-adjust timestamp)
//! plus the same `Handle::spawn_blocking` call as before — no semaphore.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::process_cpu_usage::{CPU_USAGE_WINDOW, process_cpu_usage_cores};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

/// Minimum interval between mode re-evaluations. Matches the CPU sampling
/// window so each adjustment sees a fresh reading.
const ADJUST_INTERVAL: Duration = CPU_USAGE_WINDOW;

/// Switch to [`SearchMode::HighCpu`] when the process uses more than this
/// fraction of all CPUs.
const HIGH_CPU_THRESHOLD: f32 = 0.9;

/// Switch back to [`SearchMode::HighIo`] when the process uses less than
/// this fraction of all CPUs.
const LOW_CPU_THRESHOLD: f32 = 0.5;

/// Active search runtime — encoded in [`Inner::mode`] as a `u8`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SearchMode {
    /// CPU-bound: route to the runtime with the smaller blocking pool.
    HighCpu,
    /// IO-bound: route to the runtime sized via [`common::defaults::search_thread_count`].
    HighIo,
}

impl SearchMode {
    fn as_u8(self) -> u8 {
        match self {
            SearchMode::HighCpu => 0,
            SearchMode::HighIo => 1,
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            0 => SearchMode::HighCpu,
            _ => SearchMode::HighIo,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            SearchMode::HighCpu => "high_cpu",
            SearchMode::HighIo => "high_io",
        }
    }
}

#[derive(Clone)]
pub struct AdaptiveSearchHandle {
    inner: Arc<Inner>,
}

struct Inner {
    high_cpu: Handle,
    high_io: Handle,
    /// Number of logical CPUs, read once at construction. Used as the
    /// denominator when converting process CPU usage to a utilization ratio.
    num_cpus: usize,
    /// Current mode (encoded via [`SearchMode::as_u8`]).
    mode: AtomicU8,
    /// Monotonic ns (relative to [`Inner::start`]) at which the mode was
    /// last re-evaluated. Initialized so the first adjust is suppressed
    /// until [`ADJUST_INTERVAL`] elapses — [`process_cpu_usage_cores`]
    /// returns `None` until it has two samples anyway.
    last_adjust_ns: AtomicU64,
    /// Reference instant for `last_adjust_ns`.
    start: Instant,
}

// =============================================================================
// Production API
// =============================================================================

impl AdaptiveSearchHandle {
    /// Build from the two pre-constructed search runtimes.
    pub fn new(high_cpu: Handle, high_io: Handle) -> Self {
        let num_cpus = common::cpu::get_num_cpus().max(1);
        let inner = Arc::new(Inner {
            high_cpu,
            high_io,
            num_cpus,
            mode: AtomicU8::new(SearchMode::HighIo.as_u8()),
            last_adjust_ns: AtomicU64::new(0),
            start: Instant::now(),
        });
        Self { inner }
    }

    /// Spawn a blocking closure on the search runtime selected by the
    /// current mode. Before spawning, attempt a lazy mode re-evaluation
    /// rate-limited to once per [`ADJUST_INTERVAL`].
    pub fn spawn_blocking<F, R>(&self, f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.maybe_adjust();
        self.handle_for_current_mode().spawn_blocking(f)
    }

    /// Currently active mode.
    pub fn current_mode(&self) -> SearchMode {
        SearchMode::from_u8(self.inner.mode.load(Ordering::Relaxed))
    }

    /// Tokio handle for the currently active mode. Exposed for the rare
    /// caller that needs to `.enter()` the runtime context (e.g. snapshot
    /// creation that internally calls `tokio::task::spawn_blocking`).
    pub fn tokio_handle(&self) -> &Handle {
        self.handle_for_current_mode()
    }

    fn handle_for_current_mode(&self) -> &Handle {
        match self.current_mode() {
            SearchMode::HighCpu => &self.inner.high_cpu,
            SearchMode::HighIo => &self.inner.high_io,
        }
    }

    /// Attempt to re-evaluate the active mode. No-op if we adjusted less
    /// than [`ADJUST_INTERVAL`] ago or if a CPU sample is unavailable.
    fn maybe_adjust(&self) {
        let now_ns = self.inner.start.elapsed().as_nanos() as u64;
        let interval_ns = ADJUST_INTERVAL.as_nanos() as u64;
        let last = self.inner.last_adjust_ns.load(Ordering::Relaxed);
        if last != 0 && now_ns.saturating_sub(last) < interval_ns {
            return;
        }
        if self
            .inner
            .last_adjust_ns
            .compare_exchange(last, now_ns, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let Some(cores_used) = process_cpu_usage_cores() else {
            return;
        };
        let ratio = cores_used / self.inner.num_cpus as f32;
        let current = self.current_mode();
        let next = match current {
            SearchMode::HighIo if ratio > HIGH_CPU_THRESHOLD => SearchMode::HighCpu,
            SearchMode::HighCpu if ratio < LOW_CPU_THRESHOLD => SearchMode::HighIo,
            _ => return,
        };
        self.inner.mode.store(next.as_u8(), Ordering::Relaxed);
        log::debug!(
            "adaptive search pool: switching mode {} -> {} (cpu ratio {:.2})",
            current.as_str(),
            next.as_str(),
            ratio,
        );
    }
}

// =============================================================================
// Fallback constructors
//
// Compiled into the production binary but only hit at runtime by integration
// tests that pass `None` for the search runtime to `Collection::new` /
// `Collection::load`. Production wiring always constructs an adaptive handle
// via `AdaptiveSearchHandle::new` and threads it through, so these paths are
// unreachable in real deployments.
// =============================================================================

impl AdaptiveSearchHandle {
    /// Non-adaptive handle bound to the current runtime. Used as the
    /// `unwrap_or_else` fallback when a caller passes `None` for the search
    /// runtime.
    pub fn current() -> Self {
        Self::new_fixed(Handle::current())
    }

    /// Same as [`current`](Self::current), but only for tests.
    #[cfg(any(test, feature = "testing"))]
    pub fn current_for_tests() -> Self {
        Self::new_fixed(Handle::current())
    }
}

// =============================================================================
// Test and bench helpers
//
// Direct-construction helpers for tests and bench harnesses that want a
// predictable, non-adaptive handle without wiring up two runtimes.
// =============================================================================

impl AdaptiveSearchHandle {
    /// Non-adaptive handle bound to a single Tokio runtime. Both modes route
    /// to the same handle; `maybe_adjust` is disabled (via
    /// `last_adjust_ns = u64::MAX`) so no CPU sampling happens.
    pub fn new_fixed(handle: Handle) -> Self {
        let inner = Arc::new(Inner {
            high_cpu: handle.clone(),
            high_io: handle,
            num_cpus: common::cpu::get_num_cpus().max(1),
            mode: AtomicU8::new(SearchMode::HighIo.as_u8()),
            last_adjust_ns: AtomicU64::new(u64::MAX),
            start: Instant::now(),
        });
        Self { inner }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawn_blocking_runs_closure() {
        let adaptive = AdaptiveSearchHandle::new_fixed(Handle::current());
        let jh = adaptive.spawn_blocking(|| 42u32);
        assert_eq!(jh.await.unwrap(), 42);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fixed_handle_runs_many_tasks() {
        let adaptive = AdaptiveSearchHandle::new_fixed(Handle::current());
        let mut joins = Vec::new();
        for i in 0..16u32 {
            joins.push(adaptive.spawn_blocking(move || i * 2));
        }
        let mut sum = 0u32;
        for jh in joins {
            sum += jh.await.unwrap();
        }
        let expected: u32 = (0..16u32).map(|i| i * 2).sum();
        assert_eq!(sum, expected);
    }
}
