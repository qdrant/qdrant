use std::future::Future;

use tokio::task::JoinHandle;

use super::*;

/// # Cancel safety
///
/// This function is cancel safe.
///
/// If cancelled, the cancellation token provided to the `task` will be triggered automatically.
pub async fn spawn_cancel_on_drop<Task, Fut>(task: Task) -> Result<Fut::Output, Error>
where
    Task: FnOnce(CancellationToken) -> Fut,
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    let cancel = CancellationToken::new();

    let future = task(cancel.child_token());

    let guard = cancel.drop_guard();
    let output = tokio::task::spawn(future).await?;
    guard.disarm();

    Ok(output)
}

/// # Cancel safety
///
/// This function is cancel safe.
///
/// The provided future must be cancel safe.
pub async fn cancel_on_token<Fut>(
    cancel: CancellationToken,
    future: Fut,
) -> Result<Fut::Output, Error>
where
    Fut: Future,
{
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(Error::Cancelled),
        output = future => Ok(output),
    }
}

/// Cancel and abort the given blocking task when triggering the cancellation token.
///
/// This may prematurely abort the blocking task if it has not started yet.
///
/// # Cancel safety
///
/// This function is cancel safe.
///
/// The provided future must be cancel safe.
pub async fn cancel_and_abort_on_token<T>(
    cancel: CancellationToken,
    handle: JoinHandle<T>,
) -> Result<T, Error> {
    let abort_handle = handle.abort_handle();
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            // Prematurely abort blocking task if not started yet
            abort_handle.abort();
            Err(Error::Cancelled)
        },
        output = handle => Ok(output.map_err(Error::Join)?),
    }
}
