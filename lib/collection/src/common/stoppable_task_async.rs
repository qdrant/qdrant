use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[must_use = "dropping this handle detaches the task"]
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

    use tokio::time::{sleep, timeout};

    use super::*;

    const STEP_MILLIS: u64 = 5;

    /// Upper bound on how long a cancelled task may take to report that it
    /// stopped. Deliberately far longer than it can plausibly need: it is only
    /// reached when the task never stops at all, which is the failure the test
    /// is here to catch.
    const STOP_TIMEOUT: Duration = Duration::from_secs(30);

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

        // Wait until the task reports that it stopped, rather than sleeping for a
        // fixed budget and assuming that was long enough. The flag is set by the
        // task itself once it observes the cancellation, so any scheduling delay
        // past the budget failed the assertion even though nothing was wrong --
        // which is why the Windows arm kept being padded, and why this still
        // flaked on macOS.
        timeout(STOP_TIMEOUT, async {
            while !handle.is_finished() {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("cancelled task never reported that it finished");

        let res = handle.cancel().await.unwrap();
        assert!(res < 10);
    }
}
