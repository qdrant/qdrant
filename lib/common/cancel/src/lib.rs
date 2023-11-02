pub mod blocking;
pub mod future;

pub use tokio_util::sync::{CancellationToken, DropGuard};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),

    #[error("task was cancelled")]
    Cancelled,
}

pub trait CancelError {
    /// Return an error if cancelled.
    #[must_use = "a returned error must be propagated down function calls"]
    fn error_if_cancelled(&self) -> Result<(), Error>;
}

impl CancelError for CancellationToken {
    fn error_if_cancelled(&self) -> Result<(), Error> {
        if self.is_cancelled() {
            Err(Error::Cancelled)
        } else {
            Ok(())
        }
    }
}
