use tokio_util::task::AbortOnDropHandle;

use super::*;

/// # Cancel safety
///
/// This function is cancel safe.
///
/// If cancelled, the cancellation token provided to the `task` will be triggered automatically.
///
/// This may prematurely abort the blocking task if it has not started yet.
pub async fn spawn_cancel_on_drop<Out, Task>(task: Task) -> Result<Out, Error>
where
    Task: FnOnce(CancellationToken) -> Out + Send + 'static,
    Out: Send + 'static,
{
    let cancel = CancellationToken::new();

    let task = {
        let cancel = cancel.child_token();
        move || task(cancel)
    };

    let guard = cancel.drop_guard();
    let handle = AbortOnDropHandle::new(tokio::task::spawn_blocking(task));
    let output = handle.await?;
    guard.disarm();

    Ok(output)
}

/// # Cancel safety
///
/// This function is cancel safe.
///
/// If cancelled without triggering the cancellation token, the `task` will still run to completion.
///
/// This function *will* return early, and the `task` *may* never run or return early by triggering
/// the cancellation token.
pub async fn spawn_cancel_on_token<Out, Task>(
    cancel: CancellationToken,
    task: Task,
) -> Result<Out, Error>
where
    Task: FnOnce(CancellationToken) -> Out + Send + 'static,
    Out: Send + 'static,
{
    let task = {
        let cancel = cancel.child_token();
        move || task(cancel)
    };

    let handle = tokio::task::spawn_blocking(task);
    let output = future::cancel_and_abort_on_token(cancel, handle).await?;

    Ok(output)
}
