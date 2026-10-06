use std::cmp::Ordering;

#[cfg(target_os = "linux")]
use thiserror::Error;
#[cfg(target_os = "linux")]
use thread_priority::{ThreadPriority, ThreadPriorityValue, set_current_thread_priority};

use crate::defaults::default_cpu_budget_unallocated;

/// Try to read number of CPUs from environment variable `QDRANT_NUM_CPUS`.
/// If it is not set, use `num_cpus::get()`.
pub fn get_num_cpus() -> usize {
    match std::env::var("QDRANT_NUM_CPUS") {
        Ok(val) => {
            let num_cpus = val.parse::<usize>().unwrap_or(0);
            if num_cpus > 0 {
                num_cpus
            } else {
                num_cpus::get()
            }
        }
        Err(_) => num_cpus::get(),
    }
}

/// Get available CPU budget to use for optimizations as number of CPUs (threads).
///
/// This is user configurable via `cpu_budget` parameter in settings:
/// If 0 - auto selection, keep at least one CPU free when possible.
/// If negative - subtract this number of CPUs from the available CPUs.
/// If positive - use this exact number of CPUs.
///
/// The returned value will always be at least 1.
pub fn get_cpu_budget(cpu_budget_param: isize) -> usize {
    match cpu_budget_param.cmp(&0) {
        // If less than zero, subtract from available CPUs
        Ordering::Less => get_num_cpus()
            .saturating_sub(-cpu_budget_param as usize)
            .max(1),
        // If zero, use automatic selection
        Ordering::Equal => {
            let num_cpus = get_num_cpus();
            num_cpus
                .saturating_sub(-default_cpu_budget_unallocated(num_cpus) as usize)
                .max(1)
        }
        // If greater than zero, use exact number
        Ordering::Greater => cpu_budget_param as usize,
    }
}

#[derive(Error, Debug)]
#[cfg(target_os = "linux")]
pub enum ThreadPriorityError {
    #[error("Failed to set thread priority: {0:?}")]
    SetThreadPriority(thread_priority::Error),
    #[error("Failed to parse thread priority value: {0}")]
    ParseNice(String),
}

/// On Linux, make current thread lower priority (nice: 10).
#[cfg(target_os = "linux")]
pub fn linux_low_thread_priority() -> Result<(), ThreadPriorityError> {
    // 25% corresponds to a nice value of 10
    set_linux_thread_priority(25)
}

/// Run `f` on a new thread at low priority (nice 10 on Linux) and wait for it.
///
/// Returns `f`'s value, or the payload it panicked with, as
/// [`std::panic::catch_unwind`] would. A new thread rather than lowering the
/// caller's: an unprivileged process can raise a thread's nice value but not
/// lower it back, so a pooled thread lowered in place would stay low for
/// whatever runs on it next. Threads `f` creates inherit its priority. If the
/// thread cannot be spawned, `f` runs on the caller at the caller's priority.
pub fn run_with_low_priority<T: Send>(
    name: &str,
    f: impl FnOnce() -> T + Send,
) -> std::thread::Result<T> {
    let f = std::sync::Mutex::new(Some(f));
    let take = || {
        f.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("`f` runs once")
    };
    std::thread::scope(|scope| {
        let spawned = std::thread::Builder::new()
            .name(name.to_owned())
            .spawn_scoped(scope, || {
                #[cfg(target_os = "linux")]
                if let Err(err) = linux_low_thread_priority() {
                    log::debug!("Failed to set low thread priority for {name}, ignoring: {err}");
                }
                take()()
            });
        match spawned {
            Ok(handle) => handle.join(),
            Err(err) => {
                log::warn!(
                    "Failed to spawn a low priority thread for {name}, running inline: {err}"
                );
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(take()))
            }
        }
    })
}

/// On Linux, make current thread high priority (nice: -10).
///
/// # Warning
///
/// This is very likely to fail because decreasing the nice value requires special privileges. It
/// is therefore recommended to soft-fail.
/// See: <https://manned.org/renice.1#head6>
#[cfg(target_os = "linux")]
pub fn linux_high_thread_priority() -> Result<(), ThreadPriorityError> {
    // 75% corresponds to a nice value of -10
    set_linux_thread_priority(75)
}

/// On Linux, update priority of current thread.
///
/// Only works on Linux because POSIX threads share their priority/nice value with all process
/// threads. Linux breaks this behaviour though and uses a per-thread priority/nice value.
/// - <https://linux.die.net/man/7/pthreads>
/// - <https://linux.die.net/man/2/setpriority>
#[cfg(target_os = "linux")]
fn set_linux_thread_priority(priority: u8) -> Result<(), ThreadPriorityError> {
    let new_priority = ThreadPriority::Crossplatform(
        ThreadPriorityValue::try_from(priority).map_err(ThreadPriorityError::ParseNice)?,
    );
    set_current_thread_priority(new_priority).map_err(ThreadPriorityError::SetThreadPriority)
}

#[cfg(test)]
mod low_priority_tests {
    use super::*;

    #[test]
    fn test_run_with_low_priority_returns_the_value_or_the_panic() {
        assert_eq!(run_with_low_priority("test-low", || 42).unwrap(), 42);
        let err = run_with_low_priority("test-low", || -> u32 { panic!("boom") }).unwrap_err();
        assert_eq!(err.downcast_ref::<&str>(), Some(&"boom"));
    }

    /// The work runs at a lower priority than the caller, whose own priority
    /// is untouched. (`thread_priority` maps 25 to nice 9 here, not the 10
    /// `linux_low_thread_priority` documents, so the test asserts the order.)
    #[cfg(target_os = "linux")]
    #[test]
    fn test_run_with_low_priority_lowers_only_the_new_thread() {
        fn nice() -> i64 {
            let stat = fs_err::read_to_string("/proc/thread-self/stat").unwrap();
            // The fields after the `(comm)`; nice is the 19th field overall.
            let rest = &stat[stat.rfind(')').unwrap() + 2..];
            rest.split_whitespace().nth(16).unwrap().parse().unwrap()
        }
        let before = nice();
        let inside = run_with_low_priority("test-low", nice).unwrap();
        // A caller already at nice 9 or above cannot be lowered further.
        if before < 9 {
            assert!(inside > before, "inside {inside}, caller {before}");
        }
        assert_eq!(nice(), before);
    }
}
