use std::future::Future;

use tokio::task;
pub use tokio_util::sync::{CancellationToken, DropGuard};

pub async fn spawn<F, Fut>(task: F) -> Result<Fut::Output, task::JoinError>
where
    F: FnOnce(CancellationToken) -> Fut,
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    let cancel = CancellationToken::new();

    let future = task(cancel.child_token());

    let guard = cancel.drop_guard();
    let result = task::spawn(future).await;
    guard.disarm();

    result
}

pub async fn spawn_blocking<T, F>(task: F) -> Result<T, task::JoinError>
where
    F: FnOnce(CancellationToken) -> T + Send + 'static,
    T: Send + 'static,
{
    let cancel = CancellationToken::new();

    let task = {
        let cancel = cancel.child_token();
        move || task(cancel)
    };

    let guard = cancel.drop_guard();
    let result = task::spawn_blocking(task).await;
    guard.disarm();

    result
}

pub async fn resolve<Fut>(cancel: CancellationToken, future: Fut) -> Result<Fut::Output, Cancelled>
where
    Fut: Future,
{
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(Cancelled),
        output = future => Ok(output),
    }
}

pub async fn resolve_blocking<T, F>(cancel: CancellationToken, task: F) -> Result<T, Error>
where
    F: FnOnce(CancellationToken) -> T + Send + 'static,
    T: Send + 'static,
{
    let task = {
        let cancel = cancel.child_token();
        move || task(cancel)
    };

    let output = resolve(cancel, task::spawn_blocking(task)).await??;

    Ok(output)
}

#[derive(Copy, Clone, Debug, thiserror::Error)]
#[error("task was cancelled")]
pub struct Cancelled;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Join(#[from] task::JoinError),

    #[error("task was cancelled")]
    Cancelled,
}

impl From<Cancelled> for Error {
    fn from(_: Cancelled) -> Self {
        Self::Cancelled
    }
}
