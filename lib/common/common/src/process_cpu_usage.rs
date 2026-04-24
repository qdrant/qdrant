//! Process-wide CPU usage reader with a small time-based cache.
//!
//! Exposes the average number of CPU cores used by the current process over
//! roughly the last [`CPU_USAGE_WINDOW`]. A value of `2.0` means two full
//! cores were busy on average. Intended for dynamic resource sizing (e.g.
//! search thread pool) and telemetry.
//!
//! The OS is queried at most once per window, so repeated calls are cheap.
//! Currently only Linux is supported (via `/proc/self/stat`); other
//! platforms return `None`.

use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Sampling window: how often we query the OS for process CPU time.
/// Also doubles as the averaging window for the returned value.
pub const CPU_USAGE_WINDOW: Duration = Duration::from_secs(1);

#[cfg(target_os = "linux")]
#[derive(Copy, Clone)]
struct CpuSample {
    at: Instant,
    cpu_time_secs: f64,
}

#[cfg(target_os = "linux")]
struct Cache {
    last_sample: Option<CpuSample>,
    last_value: Option<f32>,
}

#[cfg(target_os = "linux")]
static CACHE: Mutex<Cache> = Mutex::new(Cache {
    last_sample: None,
    last_value: None,
});

/// Average number of CPU cores used by this process over the last
/// [`CPU_USAGE_WINDOW`]. For example, `2.0` means two cores at 100%.
///
/// Returns `None` on unsupported platforms, when reading the source fails,
/// or on the first call (before a delta can be computed).
pub fn process_cpu_usage_cores() -> Option<f32> {
    #[cfg(target_os = "linux")]
    {
        linux::process_cpu_usage_cores()
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use procfs::ProcResult;
    use procfs::process::Process;

    use super::*;

    pub(super) fn process_cpu_usage_cores() -> Option<f32> {
        let mut guard = CACHE.lock().ok()?;
        let now = Instant::now();

        if let Some(sample) = guard.last_sample
            && now.duration_since(sample.at) < CPU_USAGE_WINDOW
        {
            return guard.last_value;
        }

        let cpu_time_secs = match read_process_cpu_time_secs() {
            Ok(v) => v,
            Err(err) => {
                log::debug!("Failed to read process CPU time from procfs: {err}");
                return guard.last_value;
            }
        };
        let new_sample = CpuSample {
            at: now,
            cpu_time_secs,
        };

        let new_value = guard.last_sample.map(|prev| {
            let wall = now.duration_since(prev.at).as_secs_f64().max(1e-6);
            let cpu = (cpu_time_secs - prev.cpu_time_secs).max(0.0);
            (cpu / wall) as f32
        });

        guard.last_sample = Some(new_sample);
        guard.last_value = new_value;

        new_value
    }

    fn read_process_cpu_time_secs() -> ProcResult<f64> {
        let stat = Process::myself()?.stat()?;
        let tps = procfs::ticks_per_second() as f64;
        // Self CPU time only — children (cutime/cstime) are intentionally excluded.
        let ticks = stat.utime.saturating_add(stat.stime);
        Ok(ticks as f64 / tps)
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn first_call_returns_none() {
        {
            let mut guard = CACHE.lock().unwrap();
            guard.last_sample = None;
            guard.last_value = None;
        }

        assert!(process_cpu_usage_cores().is_none());
    }

    #[test]
    fn returns_positive_for_busy_work() {
        {
            let mut guard = CACHE.lock().unwrap();
            guard.last_sample = None;
            guard.last_value = None;
        }

        // Prime the sample.
        let _ = process_cpu_usage_cores();

        // Busy-work past the sampling window so the next call recomputes.
        let start = Instant::now();
        let mut sum = 0u64;
        while start.elapsed() < CPU_USAGE_WINDOW + Duration::from_millis(100) {
            for i in 0..10_000 {
                sum = sum.wrapping_add(i);
            }
        }
        std::hint::black_box(sum);

        let cores = process_cpu_usage_cores().expect("should have a value after second sample");
        assert!(cores > 0.0, "expected positive CPU usage, got {cores}");
    }
}
