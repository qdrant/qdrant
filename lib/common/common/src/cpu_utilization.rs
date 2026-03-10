use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Tracks CPU utilization across multiple `spawn_blocking` tasks for a single request.
///
/// Cheap to clone — clones share the same inner counters via a single `Arc`.
/// Call `measure()` inside each `spawn_blocking` closure to accumulate
/// wall time and thread CPU time. Then call `read()` to get the ratio.
#[derive(Debug, Clone)]
pub struct CpuUtilization {
    inner: Arc<CpuUtilizationInner>,
}

#[derive(Debug, Default)]
struct CpuUtilizationInner {
    wall_time_ns: AtomicU64,
    cpu_time_ns: AtomicU64,
}

impl CpuUtilization {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CpuUtilizationInner::default()),
        }
    }

    /// Wrap a closure, measuring wall time and thread CPU time.
    /// Accumulates into the shared counters.
    pub fn measure<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let cpu_start = thread_cpu_time_ns();
        let wall_start = Instant::now();

        let result = f();

        let wall_elapsed = wall_start.elapsed().as_nanos() as u64;
        let cpu_elapsed = thread_cpu_time_ns().saturating_sub(cpu_start);

        self.inner
            .wall_time_ns
            .fetch_add(wall_elapsed, Ordering::Relaxed);
        self.inner
            .cpu_time_ns
            .fetch_add(cpu_elapsed, Ordering::Relaxed);

        result
    }

    /// Returns CPU utilization ratio: `cpu_time / wall_time`, clamped to `[0.0, 1.0]`.
    ///
    /// 1.0 means pure CPU-bound, 0.0 means pure IO-bound (or no measurements).
    pub fn ratio(&self) -> f32 {
        let wall_ns = self.inner.wall_time_ns.load(Ordering::Relaxed) as f64;
        let cpu_ns = self.inner.cpu_time_ns.load(Ordering::Relaxed) as f64;

        if wall_ns > 0.0 {
            (cpu_ns / wall_ns).clamp(0.0, 1.0) as f32
        } else {
            0.0
        }
    }
}

impl Default for CpuUtilization {
    fn default() -> Self {
        Self::new()
    }
}

/// Returns current thread's CPU time in nanoseconds.
#[cfg(target_os = "linux")]
fn thread_cpu_time_ns() -> u64 {
    let mut ts = nix::libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime with CLOCK_THREAD_CPUTIME_ID is always valid
    // for the calling thread.
    let ret = unsafe { nix::libc::clock_gettime(nix::libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    if ret == 0 {
        ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
    } else {
        0
    }
}

#[cfg(not(target_os = "linux"))]
fn thread_cpu_time_ns() -> u64 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_measure_accumulates() {
        let cu = CpuUtilization::new();

        // Do some busy work
        cu.measure(|| {
            let mut sum = 0u64;
            for i in 0..100_000 {
                sum = sum.wrapping_add(i);
            }
            std::hint::black_box(sum);
        });

        #[cfg(target_os = "linux")]
        {
            let ratio = cu.ratio();
            assert!(ratio > 0.0, "ratio should be positive for cpu-bound work");
        }
    }

    #[test]
    fn test_cloned_shares_state() {
        let cu1 = CpuUtilization::new();
        let cu2 = cu1.clone();

        cu1.measure(|| std::hint::black_box(42));
        cu2.measure(|| std::hint::black_box(42));

        assert_eq!(
            cu1.ratio(),
            cu2.ratio(),
            "clones should share the same counters"
        );
    }

    #[test]
    fn test_empty_ratio() {
        let cu = CpuUtilization::new();
        assert_eq!(cu.ratio(), 0.0);
    }
}
