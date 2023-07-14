use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use tokio::task::JoinHandle;

pub struct StoppableTaskHandle<T> {
    pub join_handle: JoinHandle<Option<T>>,
    started: Arc<AtomicBool>,
    stopped: Weak<AtomicBool>,
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
}

pub fn spawn_stoppable<F, T>(f: F) -> StoppableTaskHandle<T>
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
    }
}

#[cfg(test)]
mod tests {
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

    #[tokio::test]
    async fn test_task_stop() {
        let handle = spawn_stoppable(counting_task);

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
                    (5..=25).contains(&count),
                    "Stoppable task should have count between [5, 25], but it is {count}",
                );
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_task_stop_many() {
        const TASKS: usize = 64;

        let handles = (0..TASKS)
            .map(|_| spawn_stoppable(counting_task))
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
}
