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
