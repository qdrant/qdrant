use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

pub struct StoppableAsyncTaskHandle<T> {
    pub join_handle: JoinHandle<T>,
    stopped: Weak<AtomicBool>,
}

impl<T> StoppableAsyncTaskHandle<T> {
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

pub fn async_spawn_stoppable<F, Fut, T>(
    runtime_handle: &Handle,
    f: F,
) -> StoppableAsyncTaskHandle<T>
where
    F: FnOnce(Arc<AtomicBool>) -> Fut,
    T: Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
{
    let stopped = Arc::new(AtomicBool::new(false));
    // We are OK if original value is destroyed with the thread
    // Weak reference is sufficient
    let stopped_w = Arc::downgrade(&stopped);

    StoppableAsyncTaskHandle {
        join_handle: runtime_handle.spawn(f(stopped)),
        stopped: stopped_w,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const STEP_MILLIS: u64 = 5;

    async fn long_task(stop: Arc<AtomicBool>) -> i32 {
        let mut n = 0;
        for i in 0..100 {
            n = i;
            if stop.load(Ordering::Relaxed) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(STEP_MILLIS)).await;
        }
        n
    }

    #[tokio::test]
    async fn test_async_task_stop() {
        let runtime_handle = Handle::try_current().unwrap();
        let handle = async_spawn_stoppable(&runtime_handle, long_task);
        tokio::time::sleep(Duration::from_millis(STEP_MILLIS * 5)).await;
        // asking to stop after at most 5 loops
        handle.ask_to_stop();
        // waiting longer does not change the value as it is already stopped
        tokio::time::sleep(Duration::from_millis(STEP_MILLIS * 10)).await;
        let res = handle.stop().await.unwrap();
        assert!(res < 10);
    }
}
