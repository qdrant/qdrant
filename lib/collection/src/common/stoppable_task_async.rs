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

    use tokio::sync::oneshot;
    use tokio::time::{sleep, timeout};

    use super::*;

    const STEP_MILLIS: u64 = 5;

    /// Number of steps [`long_task`] runs when nothing cancels it.
    ///
    /// Sized so that an uncancelled task stays busy for far longer than
    /// [`STOP_TIMEOUT`]: it cannot reach the end of its own loop while the test
    /// is watching, so cancellation is the only thing that can make it stop.
    const TASK_STEPS: i32 = 10_000;

    /// Upper bound on how long a *cancelled* task may take to stop.
    ///
    /// A passing run never comes close to it, since the task checks for
    /// cancellation once every [`STEP_MILLIS`]. It is a safety bound for a task
    /// that ignores cancellation altogether -- the failure this test exists to
    /// catch -- so that such a task fails the test instead of hanging it.
    const STOP_TIMEOUT: Duration = Duration::from_secs(1);

    const _: () = assert!(
        TASK_STEPS as u128 * STEP_MILLIS as u128 > STOP_TIMEOUT.as_millis(),
        "an uncancelled `long_task` must outlast `STOP_TIMEOUT`, otherwise the \
         test would pass without anything having been cancelled",
    );

    /// Counts steps until it is cancelled, then reports how far it got.
    ///
    /// Only cancellation ends it within the time bounds of the test.
    async fn long_task(cancel: CancellationToken, started: oneshot::Sender<()>) -> i32 {
        // Report that the task body is running, so that the test asks a task it
        // knows to be alive to cancel.
        started.send(()).expect("test dropped the start signal");

        let mut n = 0;
        for i in 0..TASK_STEPS {
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
        let (started_tx, started_rx) = oneshot::channel();
        let mut handle = spawn_async_cancellable(move |cancel| long_task(cancel, started_tx));

        // Synchronise on the task instead of guessing how long it takes to be
        // scheduled: once this resolves the task is running, and it still has
        // essentially all of its steps ahead of it.
        started_rx.await.expect("task never started");
        assert!(!handle.is_finished());

        handle.ask_to_cancel();

        // Wait for the task to finish, rather than sleeping for a fixed budget
        // and assuming that was long enough. Left alone the task would keep
        // going for `TASK_STEPS * STEP_MILLIS`, so joining it can only succeed
        // because the request above stopped it -- the timeout is a bound on a
        // task that ignores cancellation, not the reason the test passes.
        let res = timeout(STOP_TIMEOUT, &mut handle.join_handle)
            .await
            .expect("cancelled task did not stop")
            .expect("task panicked");

        // The task sets this flag itself, just before it returns.
        assert!(handle.is_finished());

        // An uncancelled run would have returned `TASK_STEPS - 1`; anything
        // below that means the task broke out of its loop early.
        assert!(res < TASK_STEPS - 1, "task was not stopped early: {res}");
    }
}
