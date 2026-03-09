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

    /// Returns (total_wall_time_us, total_cpu_time_us, ratio).
    ///
    /// Ratio is `cpu_time / wall_time`, clamped to `[0.0, 1.0]`.
    /// Returns `(0.0, 0.0, 0.0)` if no measurements were recorded.
    pub fn read(&self) -> (f64, f64, f64) {
        let wall_ns = self.inner.wall_time_ns.load(Ordering::Relaxed) as f64;
        let cpu_ns = self.inner.cpu_time_ns.load(Ordering::Relaxed) as f64;

        let wall_us = wall_ns / 1_000.0;
        let cpu_us = cpu_ns / 1_000.0;

        let ratio = if wall_ns > 0.0 {
            (cpu_ns / wall_ns).clamp(0.0, 1.0)
        } else {
            0.0
        };

        (wall_us, cpu_us, ratio)
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
            let (wall_us, cpu_us, ratio) = cu.read();
            assert!(wall_us > 0.0, "wall time should be positive");

            assert!(cpu_us > 0.0, "cpu time should be positive on linux");
            assert!(ratio > 0.0, "ratio should be positive for cpu-bound work");
        }
    }

    #[test]
    fn test_cloned_shares_state() {
        let cu1 = CpuUtilization::new();
        let cu2 = cu1.clone();

        cu1.measure(|| std::hint::black_box(42));
        cu2.measure(|| std::hint::black_box(42));

        let (wall1, _, _) = cu1.read();
        let (wall2, _, _) = cu2.read();
        assert_eq!(wall1, wall2, "clones should share the same counters");
    }

    #[test]
    fn test_empty_read() {
        let cu = CpuUtilization::new();
        let (wall, cpu, ratio) = cu.read();
        assert_eq!(wall, 0.0);
        assert_eq!(cpu, 0.0);
        assert_eq!(ratio, 0.0);
    }
}
