//! Adaptive wrapper around a Tokio runtime [`Handle`] for the search pool.
//!
//! Limits concurrency of `spawn_blocking` calls via a semaphore whose permit
//! count is re-evaluated lazily — before acquiring a permit in
//! `spawn_blocking`, but at most once per [`ADJUST_INTERVAL`].
//!
//! - Start with `2 * num_cpus` permits.
//! - Scale down by 1 (toward `num_cpus`) when CPU is saturated — more threads
//!   would only add contention.
//! - Scale up by 1 (toward `4 * num_cpus`) when CPU has headroom — search is
//!   likely IO-bound and more threads can make progress.
//!
//! Permit acquisition happens *outside* [`Handle::spawn_blocking`]: the caller
//! awaits a permit, then the closure is spawned carrying the permit. The
//! permit is released when the blocking work finishes.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use common::cpu::get_num_cpus;
use common::process_cpu_usage::{CPU_USAGE_WINDOW, process_cpu_usage_cores};
use tokio::runtime::Handle;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

/// Minimum interval between permit-budget re-evaluations. Matches the CPU
/// sampling window so each adjustment sees a fresh reading.
const ADJUST_INTERVAL: Duration = CPU_USAGE_WINDOW;

/// Scale down when the process uses more than this fraction of all CPUs.
const HIGH_CPU_THRESHOLD: f32 = 0.9;

/// Scale up when the process uses less than this fraction of all CPUs.
const LOW_CPU_THRESHOLD: f32 = 0.5;

/// Upper bound on the thread budget, as a multiple of `num_cpus`.
const MAX_MULT: usize = 4;

/// Initial thread budget, as a multiple of `num_cpus`.
const INITIAL_MULT: usize = 2;

#[derive(Clone)]
pub struct AdaptiveSearchHandle {
    inner: Arc<Inner>,
}

struct Inner {
    handle: Handle,
    semaphore: Arc<Semaphore>,
    /// Number of logical CPUs, read once at construction. Used as the
    /// denominator when converting process CPU usage to a utilization ratio.
    num_cpus: usize,
    /// Lower bound: `num_cpus` permits. Never shrink below this.
    min_permits: usize,
    /// Upper bound: `MAX_MULT * num_cpus` permits.
    max_permits: usize,
    /// Current total permit budget — including those currently held by tasks.
    /// Mirrors actual semaphore size but is cheap to read without acquiring.
    current_permits: AtomicUsize,
    /// Monotonic ns (relative to [`Inner::start`]) at which the budget was
    /// last re-evaluated. Initialized so the first adjust is suppressed until
    /// [`ADJUST_INTERVAL`] elapses — [`process_cpu_usage_cores`] returns
    /// `None` until it has two samples anyway.
    last_adjust_ns: AtomicU64,
    /// Reference instant for `last_adjust_ns`. Using a single monotonic base
    /// lets us store the timestamp as a plain `AtomicU64`.
    start: Instant,
}

// =============================================================================
// Production API
// =============================================================================

impl AdaptiveSearchHandle {
    /// Build from a Tokio runtime handle. Permit budget is re-evaluated
    /// lazily on `spawn_blocking`, no background task is spawned.
    pub fn new(handle: Handle) -> Self {
        let num_cpus = get_num_cpus().max(1);
        let min_permits = num_cpus;
        let max_permits = MAX_MULT * num_cpus;
        let initial = (INITIAL_MULT * num_cpus).clamp(min_permits, max_permits);

        let inner = Arc::new(Inner {
            handle,
            semaphore: Arc::new(Semaphore::new(initial)),
            num_cpus,
            min_permits,
            max_permits,
            current_permits: AtomicUsize::new(initial),
            last_adjust_ns: AtomicU64::new(0),
            start: Instant::now(),
        });

        Self { inner }
    }

    /// Access the underlying Tokio handle. Use only where spawning outside
    /// the adaptive pool is genuinely needed (e.g. spawning non-blocking
    /// async tasks).
    pub fn tokio_handle(&self) -> &Handle {
        &self.inner.handle
    }

    /// Spawn a blocking closure on the search runtime, awaiting an adaptive
    /// permit first. The permit is held for the duration of the closure.
    ///
    /// Before acquiring a permit we attempt a lazy budget re-evaluation,
    /// rate-limited to at most once per [`ADJUST_INTERVAL`].
    pub async fn spawn_blocking<F, R>(&self, f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let retire_on_complete = self.maybe_adjust();

        let permit = Arc::clone(&self.inner.semaphore)
            .acquire_owned()
            .await
            .expect("search semaphore was closed");

        self.inner.handle.spawn_blocking(move || {
            let result = f();
            // If this call won the shrink tick, retire its own permit
            // (forget it) instead of returning it to the semaphore. Budget
            // was decremented in `maybe_adjust` already.
            if retire_on_complete {
                permit.forget();
            } else {
                drop(permit);
            }
            result
        })
    }

    /// Current total permit budget (allocated; may include in-use permits).
    pub fn current_permits(&self) -> usize {
        self.inner.current_permits.load(Ordering::Relaxed)
    }

    /// Number of permits currently free (not held by any task).
    pub fn available_permits(&self) -> usize {
        self.inner.semaphore.available_permits()
    }

    /// Attempt to adjust the permit budget. No-op if we adjusted less than
    /// [`ADJUST_INTERVAL`] ago or if CPU usage is unavailable.
    ///
    /// Returns `true` iff the caller should retire its permit (forget it
    /// after the blocking work completes) — i.e., a shrink was elected this
    /// tick. `current_permits` has already been decremented in that case.
    fn maybe_adjust(&self) -> bool {
        // Rate-limit: at most one adjustment per window. Use CAS so concurrent
        // callers elect a single adjuster; the rest skip.
        let now_ns = self.inner.start.elapsed().as_nanos() as u64;
        let interval_ns = ADJUST_INTERVAL.as_nanos() as u64;
        let last = self.inner.last_adjust_ns.load(Ordering::Relaxed);
        if last != 0 && now_ns.saturating_sub(last) < interval_ns {
            return false;
        }
        if self
            .inner
            .last_adjust_ns
            .compare_exchange(last, now_ns, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            // Another caller just claimed this tick.
            return false;
        }

        let Some(cores_used) = process_cpu_usage_cores() else {
            // CPU usage unavailable — hold permits steady. Note we've still
            // consumed this tick; next attempt will be after ADJUST_INTERVAL.
            return false;
        };

        let ratio = cores_used / self.inner.num_cpus as f32;
        let current = self.inner.current_permits.load(Ordering::Relaxed);

        if ratio > HIGH_CPU_THRESHOLD && current > self.inner.min_permits {
            // Mark the caller's permit for retirement; budget reflects target
            // immediately.
            let prev = self.inner.current_permits.fetch_sub(1, Ordering::Relaxed);
            log::debug!(
                "adaptive search pool: shrinking to {} permits (was {prev})",
                prev - 1,
            );
            true
        } else if ratio < LOW_CPU_THRESHOLD && current < self.inner.max_permits {
            grow_one(&self.inner);
            false
        } else {
            false
        }
    }
}

fn grow_one(inner: &Inner) {
    let prev = inner.current_permits.fetch_add(1, Ordering::Relaxed);
    inner.semaphore.add_permits(1);
    log::debug!(
        "adaptive search pool: grew to {} permits (was {prev})",
        prev + 1,
    );
}

// =============================================================================
// Fallback constructors
//
// Compiled into the production binary but only hit at runtime by integration
// tests that pass `None` for the search runtime to `Collection::new` /
// `Collection::load`. Production wiring always constructs an adaptive handle
// via `AdaptiveSearchHandle::new` on the search runtime and threads it
// through, so these paths are unreachable in real deployments.
// =============================================================================

impl AdaptiveSearchHandle {
    /// Non-adaptive, fixed-size handle bound to the current runtime with
    /// `num_cpus` permits. Used as the `unwrap_or_else` fallback when a
    /// caller passes `None` for the search runtime.
    pub fn current() -> Self {
        Self::new_fixed(Handle::current(), get_num_cpus().max(1))
    }

    /// Same as current, but only for tests
    #[cfg(any(test, feature = "testing"))]
    pub fn current_for_tests() -> Self {
        Self::new_fixed(Handle::current(), get_num_cpus().max(1))
    }
}

// =============================================================================
// Test and bench helpers
//
// Direct-construction helpers for tests and bench harnesses that want a
// predictable, non-adaptive pool without wiring up the full TOC.
// =============================================================================

impl AdaptiveSearchHandle {
    /// Non-adaptive handle with a caller-chosen permit count. `maybe_adjust`
    /// is disabled (via `last_adjust_ns = u64::MAX`), so neither CPU sampling
    /// nor permit resizing happens.
    pub fn new_fixed(handle: Handle, permits: usize) -> Self {
        let permits = permits.max(1);
        let inner = Arc::new(Inner {
            handle,
            semaphore: Arc::new(Semaphore::new(permits)),
            num_cpus: get_num_cpus().max(1),
            min_permits: permits,
            max_permits: permits,
            current_permits: AtomicUsize::new(permits),
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
        let handle = Handle::current();
        let adaptive = AdaptiveSearchHandle::new_fixed(handle, 2);
        let jh = adaptive.spawn_blocking(|| 42u32).await;
        assert_eq!(jh.await.unwrap(), 42);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fixed_handle_bounds_concurrency() {
        use std::sync::atomic::AtomicUsize;
        let handle = Handle::current();
        let adaptive = AdaptiveSearchHandle::new_fixed(handle, 2);
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        let mut joins = Vec::new();
        for _ in 0..10 {
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            let jh = adaptive
                .spawn_blocking(move || {
                    let n = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(n, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(50));
                    active.fetch_sub(1, Ordering::SeqCst);
                })
                .await;
            joins.push(jh);
        }
        for jh in joins {
            jh.await.unwrap();
        }
        assert!(peak.load(Ordering::SeqCst) <= 2, "peak was {peak:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn grow_one_increases_permits() {
        let handle = Handle::current();
        let adaptive = AdaptiveSearchHandle::new_fixed(handle, 2);
        // Bypass clamp by calling grow_one directly.
        let start = adaptive.current_permits();
        grow_one(&adaptive.inner);
        assert_eq!(adaptive.current_permits(), start + 1);
        assert_eq!(adaptive.available_permits(), start + 1);
    }
}
