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
    use std::time::Duration;

    use super::*;

    const STEP_MILLIS: u64 = 5;

    fn long_task(stop: &AtomicBool) -> i32 {
        let mut n = 0;
        for i in 0..10 {
            n = i;
            if stop.load(Ordering::Relaxed) {
                break;
            }
            thread::sleep(Duration::from_millis(STEP_MILLIS));
        }
        n
    }

    #[tokio::test]
    async fn test_task_stop() {
        let handle = spawn_stoppable(long_task);
        tokio::time::sleep(Duration::from_millis(STEP_MILLIS * 5)).await;
        assert!(!handle.is_finished());
        handle.ask_to_stop();
        tokio::time::sleep(Duration::from_millis(STEP_MILLIS * 3)).await;
        assert!(handle.is_finished());

        if let Some(handle) = handle.stop() {
            if let Some(res) = handle.await.unwrap() {
                assert!(res < 10);
            }
        }
    }
}
