mod file_ops;
mod open_extra;
mod pipeline;
mod read;
mod write;

pub use file_ops::{UniversalReadFileOps, UniversalReadFs};
pub use open_extra::OpenExtra;
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

/// Element type read from or written to a universal I/O storage.
///
/// Acts as a short alias for `bytemuck::Pod + Send`. The [`Send`] bound is
/// required because some [`UniversalRead`] implementations transfer item
/// buffers across threads (e.g. background fetch in disk-cache / future
/// async backends). All `Pod` types in practice are plain bytes and thus
/// naturally [`Send`], so the bound does not restrict realistic usage.
pub trait Item: bytemuck::Pod + Send {}
impl<T: bytemuck::Pod + Send> Item for T {}
