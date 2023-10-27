use std::future::Future;

use tokio_util::sync::CancellationToken;

use crate::operations::types::CollectionError;

pub async fn with_cancellation<F: Future>(
    future: F,
    cancel: CancellationToken,
) -> Result<F::Output, Cancelled> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(Cancelled),
        output = future => Ok(output),
    }
}

#[derive(Copy, Clone, Debug, thiserror::Error)]
#[error("task was cancelled")]
pub struct Cancelled;

impl From<Cancelled> for CollectionError {
    fn from(err: Cancelled) -> Self {
        Self::Cancelled {
            description: err.to_string(),
        }
    }
}
