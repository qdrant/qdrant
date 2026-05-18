mod file_ops;
mod pipeline;
mod read;
mod write;

pub use file_ops::UniversalReadFileOps;
pub use pipeline::{BorrowedReadPipeline, OwnedReadPipeline};
pub use read::UniversalRead;
pub use write::UniversalWrite;

/// An arbitrary value to distinguish requests.
///
/// Batched universal I/O methods let callers to add an arbitrary user-provided
/// value to each request. This value will be passed back to the caller/callback
/// when the request completes.
///
/// Similar to `user_data` in `io_uring`, but allows arbitrary type, not just
/// `u64`.
///
/// This trait exists for documentation/code navigation purposes only.
pub trait UserData {}
impl<T> UserData for T {}
