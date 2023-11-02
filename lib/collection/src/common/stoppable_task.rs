use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use tokio::task::JoinHandle;

type PanicPayload = Box<dyn Any + Send + 'static>;

pub struct StoppableTaskHandle<T> {
    pub join_handle: JoinHandle<Option<T>>,
    started: Arc<AtomicBool>,
    stopped: Weak<AtomicBool>,
    panic_handler: Option<Box<dyn Fn(PanicPayload) + Sync + Send>>,
}

impl<T> StoppableTaskHandle<T> {
    pub fn is_started(&self) -> bool {
        self.started.load(Ordering::Relaxed)
    }

    pub fn is_finished(&self) -> bool {
        self.join_handle.is_finished()
    }

    pub fn ask_to_stop(&self) {
        if let Some(v) = self.stopped.upgrade() {
            v.store(true, Ordering::Relaxed);
        }
    }

    pub fn stop(self) -> Option<JoinHandle<Option<T>>> {
        self.ask_to_stop();
        self.is_started().then_some(self.join_handle)
    }

    /// Join this stoppable task and handle any panics
    ///
    /// Any panics are propagated through the configured panic handler. If no handler is
    /// configured, nothing happens.
    ///
    /// To call this, the task must already be finished. Otherwise it panics in development, or
    /// blocks in release.
    pub async fn join_and_handle_panic(self) {
        debug_assert!(
            self.join_handle.is_finished(),
            "Task must be finished, we cannot block here on awaiting the join handle",
        );

        match self.join_handle.await {
            Ok(_) => {}
            Err(err) if err.is_cancelled() => {}
            // Propagate panic
            Err(err) if err.is_panic() => match self.panic_handler {
                Some(panic_handler) => {
                    log::trace!("Handling stoppable task panic through custom panic handler");
                    let panic = err.into_panic();
                    panic_handler(panic);
                }
                None => {
                    log::debug!("Stoppable task panicked without panic handler");
                }
            },
            // Log error on unknown error
            Err(err) => {
                log::error!("Stoppable task handle error for unknown reason: {err}");
            }
        }
    }
}

/// Spawn stoppable task `f`
///
/// An optional `panic_handler` may be given, eventually called if the task panicked.
pub fn spawn_stoppable<F, T>(
    f: F,
    panic_handler: Option<Box<dyn Fn(PanicPayload) + Sync + Send>>,
) -> StoppableTaskHandle<T>
where
    F: FnOnce(&AtomicBool) -> T + Send + 'static,
    T: Send + 'static,
{
    let started = Arc::new(AtomicBool::new(false));
    let started_c = started.clone();

    let stopped = Arc::new(AtomicBool::new(false));
    // We are OK if original value is destroyed with the thread
    // Weak reference is sufficient
    let stopped_w = Arc::downgrade(&stopped);

    StoppableTaskHandle {
        join_handle: tokio::task::spawn_blocking(move || {
            // TODO: Should we use `Ordering::Acquire` or `Ordering::SeqCst`? 🤔
            if stopped.load(Ordering::Relaxed) {
                return None;
            }

            // TODO: Should we use `Ordering::Release` or `Ordering::SeqCst`? 🤔
            started.store(true, Ordering::Relaxed);

            Some(f(&stopped))
        }),
        started: started_c,
        stopped: stopped_w,
        panic_handler,
    }
}

/// Convert a panic payload into a string
///
/// This converts `String` and `&str` panic payloads into a string.
/// Other payload types are formatted as is, and may be non descriptive.
pub(crate) fn panic_payload_into_string(payload: PanicPayload) -> String {
    payload
        .downcast::<&str>()
        .map(|msg| msg.to_string())
        .or_else(|payload| payload.downcast::<String>().map(|msg| msg.to_string()))
        .unwrap_or_else(|payload| format!("{payload:?}"))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::thread;
    use std::time::{Duration, Instant};

    use tokio::time::sleep;

    use super::*;

    const STEP: Duration = Duration::from_millis(5);

    /// Simple stoppable task counting steps until stopped. Panics after 1 minute.
    fn counting_task(stop: &AtomicBool) -> usize {
        let mut count = 0;
        let start = Instant::now();

        while !stop.load(Ordering::SeqCst) {
            count += 1;

            if start.elapsed() > Duration::from_secs(60) {
                panic!("Task is not stopped within 60 seconds");
            }

            thread::sleep(STEP);
        }

        count
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_task_stop() {
        let handle = spawn_stoppable(counting_task, None);

        // Signal task to stop after ~20 steps
        sleep(STEP * 20).await;
        assert!(!handle.is_finished());
        handle.ask_to_stop();

        sleep(Duration::from_secs(1)).await;
        assert!(handle.is_finished());

        // Expect task counter to be between [5, 25], we cannot be exact on busy systems
        if let Some(handle) = handle.stop() {
            if let Some(count) = handle.await.unwrap() {
                assert!(
                    count < 25,
                    "Stoppable task should have count should be less than 25, but it is {count}",
                );
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_task_stop_many() {
        const TASKS: usize = 64;

        let handles = (0..TASKS)
            .map(|_| spawn_stoppable(counting_task, None))
            .collect::<Vec<_>>();

        // Signal tasks to stop after ~20 steps
        sleep(STEP * 20).await;
        for handle in &handles {
            assert!(!handle.is_finished());
            handle.ask_to_stop();
        }

        // Expect task counters to be between [5, 30], we cannot be exact on busy systems
        for handle in handles {
            if let Some(handle) = handle.stop() {
                if let Some(count) = handle.await.unwrap() {
                    assert!(
                        count < 30, // 10 extra steps to stop all tasks
                        "Stoppable task should have count should be less than 30, but it is {count}",
                    );
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_task_panic() {
        let panic_payload = Arc::new(Mutex::new(String::new()));
        let handle = spawn_stoppable(
            |_| {
                thread::sleep(STEP * 50);
                panic!("stoppable task panicked");
            },
            Some(Box::new({
                let panic_payload = panic_payload.clone();
                move |payload| {
                    *panic_payload.lock().unwrap() = panic_payload_into_string(payload);
                }
            })),
        );

        sleep(STEP * 20).await;
        assert!(!handle.is_finished());
        sleep(STEP * 100).await;
        assert!(handle.is_finished());

        // Join handle to call back panic
        handle.join_and_handle_panic().await;

        assert_eq!(*panic_payload.lock().unwrap(), "stoppable task panicked");
    }
}
