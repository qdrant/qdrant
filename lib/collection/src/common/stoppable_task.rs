use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use tokio::task::JoinHandle;

pub struct StoppableTaskHandle<T> {
    pub join_handle: JoinHandle<T>,
    finished: Arc<AtomicBool>,
    stopped: Weak<AtomicBool>,
}

impl<T> StoppableTaskHandle<T> {
    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Relaxed)
    }

    pub fn ask_to_stop(&self) {
        if let Some(v) = self.stopped.upgrade() {
            v.store(true, Ordering::Relaxed);
        }
    }

    pub fn stop(self) -> JoinHandle<T> {
        self.ask_to_stop();
        self.join_handle
    }
}

pub fn spawn_stoppable<F, T>(f: F) -> StoppableTaskHandle<T>
where
    F: FnOnce(&AtomicBool) -> T,
    F: Send + 'static,
    T: Send + 'static,
{
    let finished = Arc::new(AtomicBool::new(false));
    let finished_c = finished.clone();

    let stopped = Arc::new(AtomicBool::new(false));
    // We are OK if original value is destroyed with the thread
    // Weak reference is sufficient
    let stopped_w = Arc::downgrade(&stopped);

    StoppableTaskHandle {
        join_handle: tokio::task::spawn_blocking(move || {
            let res = f(&stopped);
            // We use `Release` ordering to ensure that `f` won't be moved after the `store`
            // by the compiler
            finished.store(true, Ordering::Release);
            res
        }),
        stopped: stopped_w,
        finished: finished_c,
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

        thread::sleep(Duration::from_millis(STEP_MILLIS * 5));
        assert!(!handle.is_finished());
        handle.ask_to_stop();
        thread::sleep(Duration::from_millis(STEP_MILLIS * 3));
        // If windows, we need to wait a bit more
        #[cfg(windows)]
        thread::sleep(Duration::from_millis(STEP_MILLIS * 10));
        assert!(handle.is_finished());

        let res = handle.stop().await.unwrap();
        assert!(res < 10);
    }
}
