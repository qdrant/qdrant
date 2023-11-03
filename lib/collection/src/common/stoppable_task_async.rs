use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub struct CancellableAsyncTaskHandle<T: Clone> {
    pub join_handle: JoinHandle<T>,
    result_holder: Arc<Mutex<Option<T>>>,
    cancelled: CancellationToken,
    finished: Arc<AtomicBool>,
}

impl<T: Clone> CancellableAsyncTaskHandle<T> {
    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Relaxed)
    }

    pub fn ask_to_cancel(&self) {
        self.cancelled.cancel();
    }

    pub fn cancel(self) -> JoinHandle<T> {
        self.ask_to_cancel();
        self.join_handle
    }

    pub fn get_result(&self) -> Option<T> {
        self.result_holder.lock().clone()
    }
}

pub fn spawn_async_cancellable<F, T>(f: F) -> CancellableAsyncTaskHandle<T::Output>
where
    F: FnOnce(CancellationToken) -> T,
    F: Send + 'static,
    T: Future + Send + 'static,
    T::Output: Clone + Send + 'static,
{
    let cancelled = CancellationToken::new();
    let finished = Arc::new(AtomicBool::new(false));
    let result_holder = Arc::new(Mutex::new(None));

    CancellableAsyncTaskHandle {
        join_handle: tokio::task::spawn({
            let (cancel, finished, result_holder) =
                (cancelled.clone(), finished.clone(), result_holder.clone());
            async move {
                let res = f(cancel).await;
                let mut result_holder_w = result_holder.lock();
                result_holder_w.replace(res.clone());

                // We use `Release` ordering to ensure that `f` won't be moved after the `store`
                // by the compiler
                finished.store(true, Ordering::Release);
                res
            }
        }),
        result_holder,
        cancelled,
        finished,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::sleep;

    use super::*;

    const STEP_MILLIS: u64 = 5;

    async fn long_task(cancel: CancellationToken) -> i32 {
        let mut n = 0;
        for i in 0..10 {
            n = i;
            if cancel.is_cancelled() {
                break;
            }
            sleep(Duration::from_millis(STEP_MILLIS)).await;
        }
        n
    }

    #[tokio::test]
    async fn test_task_stop() {
        let handle = spawn_async_cancellable(long_task);

        sleep(Duration::from_millis(STEP_MILLIS * 5)).await;
        assert!(!handle.is_finished());
        handle.ask_to_cancel();
        sleep(Duration::from_millis(STEP_MILLIS * 3)).await;
        // If windows, we need to wait a bit more
        #[cfg(windows)]
        sleep(Duration::from_millis(STEP_MILLIS * 10)).await;
        assert!(handle.is_finished());

        let res = handle.cancel().await.unwrap();
        assert!(res < 10);
    }
}
