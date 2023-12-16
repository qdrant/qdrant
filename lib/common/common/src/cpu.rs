use thiserror::Error;
use thread_priority::{
    get_current_thread_priority, set_current_thread_priority, ThreadPriority, ThreadPriorityValue,
};

#[derive(Error, Debug)]
pub enum ThreadPriorityError {
    #[error("Failed to get thread priority: {0:?}")]
    GetThreadPriority(thread_priority::Error),
    #[error("Failed to set thread priority: {0:?}")]
    SetThreadPriority(thread_priority::Error),
    #[error("Got unexpected thread priority type, cannot change: {0:?}")]
    UnexpectedThreadPriorityType(ThreadPriority),
    #[error("Thread priority is left unchanged to keep it in bounds")]
    UnchangedNice,
    #[error("Failed to parse niceness value: {0}")]
    ParseNice(&'static str),
}

/// Make current thread lower priority (renice=+1).
///
/// Only has an effect on Unix platforms, ignored on other platforms.
pub fn current_thread_lower_priority() -> Result<(), ThreadPriorityError> {
    current_thread_renice(1)
}

/// Make current thread high priority (renice=-10).
///
/// Only has an effect on Unix platforms, ignored on other platforms.
pub fn current_thread_high_priority() -> Result<(), ThreadPriorityError> {
    current_thread_renice(-10)
}

/// Update thread priority and niceness.
///
/// Only has an effect on Unix platforms, ignored on other platforms.
fn current_thread_renice(relative_nice: i8) -> Result<(), ThreadPriorityError> {
    #[cfg(not(unix))]
    {
        return Ok(());
    }

    #[cfg(unix)]
    {
        // Get thread priority value
        let current =
            match get_current_thread_priority().map_err(ThreadPriorityError::GetThreadPriority)? {
                ThreadPriority::Crossplatform(current) => current,
                thread_priority => {
                    return Err(ThreadPriorityError::UnexpectedThreadPriorityType(
                        thread_priority,
                    ))
                }
            };

        // Calculate new niceness, but stay within bounds
        let old_nice: u8 = current.into();
        let new_nice = (old_nice as i8).saturating_add(relative_nice).clamp(
            ThreadPriorityValue::MIN as i8,
            ThreadPriorityValue::MAX as i8,
        ) as u8;
        if old_nice == new_nice {
            return Err(ThreadPriorityError::UnchangedNice);
        }

        let new_priority = ThreadPriority::Crossplatform(
            ThreadPriorityValue::try_from(new_nice).map_err(ThreadPriorityError::ParseNice)?,
        );
        set_current_thread_priority(new_priority).map_err(ThreadPriorityError::SetThreadPriority)
    }
}
