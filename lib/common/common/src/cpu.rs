#[cfg(target_os = "linux")]
use thiserror::Error;
#[cfg(target_os = "linux")]
use thread_priority::{set_current_thread_priority, ThreadPriority, ThreadPriorityValue};

#[derive(Error, Debug)]
#[cfg(target_os = "linux")]
pub enum ThreadPriorityError {
    #[error("Failed to set thread priority: {0:?}")]
    SetThreadPriority(thread_priority::Error),
    #[error("Failed to parse thread priority value: {0}")]
    ParseNice(&'static str),
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
