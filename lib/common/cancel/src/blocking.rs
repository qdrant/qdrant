use super::*;

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
    let output = tokio::task::spawn_blocking(task).await?;
    guard.disarm();

    Ok(output)
}

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

    let output = future::cancel_on_token(cancel, tokio::task::spawn_blocking(task)).await??;

    Ok(output)
}
