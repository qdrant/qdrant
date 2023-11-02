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
