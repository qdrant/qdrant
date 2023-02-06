use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use tokio::task::JoinHandle;

pub struct StoppableAsyncTaskHandle<T: Clone> {
    pub join_handle: JoinHandle<T>,
    result_holder: Arc<Mutex<Option<T>>>,
    finished: Arc<AtomicBool>,
    stopped: Weak<AtomicBool>,
}

impl<T: Clone> StoppableAsyncTaskHandle<T> {
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

    pub fn get_result(&self) -> Option<T> {
        self.result_holder.lock().clone()
    }
}

pub fn spawn_async_stoppable<F, T>(f: F) -> StoppableAsyncTaskHandle<T::Output>
where
    F: FnOnce(Arc<AtomicBool>) -> T,
    F: Send + 'static,
    T: Future + Send + 'static,
    T::Output: Clone + Send + 'static,
{
    let finished = Arc::new(AtomicBool::new(false));
    let finished_c = finished.clone();

    let stopped = Arc::new(AtomicBool::new(false));
    // We are OK if original value is destroyed with the thread
    // Weak reference is sufficient
    let stopped_w = Arc::downgrade(&stopped);

    let result_holder = Arc::new(Mutex::new(None));
    let result_holder_c = result_holder.clone();

    StoppableAsyncTaskHandle {
        join_handle: tokio::task::spawn(async move {
            let res = f(stopped).await;
            let mut result_holder_w = result_holder_c.lock();
            result_holder_w.replace(res.clone());

            // We use `Release` ordering to ensure that `f` won't be moved after the `store`
            // by the compiler
            finished.store(true, Ordering::Release);
            res
        }),
        result_holder,
        stopped: stopped_w,
        finished: finished_c,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::sleep;

    use super::*;

    const STEP_MILLIS: u64 = 5;

    async fn long_task(stop: Arc<AtomicBool>) -> i32 {
        let mut n = 0;
        for i in 0..10 {
            n = i;
            if stop.load(Ordering::Relaxed) {
                break;
            }
            sleep(Duration::from_millis(STEP_MILLIS)).await;
        }
        n
    }

    #[tokio::test]
    async fn test_task_stop() {
        let handle = spawn_async_stoppable(long_task);

        sleep(Duration::from_millis(STEP_MILLIS * 5)).await;
        assert!(!handle.is_finished());
        handle.ask_to_stop();
        sleep(Duration::from_millis(STEP_MILLIS * 2)).await;
        assert!(handle.is_finished());

        let res = handle.stop().await.unwrap();
        assert!(res < 10);
    }
}
