use std::future::Future;

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
