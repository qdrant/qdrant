use std::future::Future;
use std::path::Path;

use bytes::Bytes;
use common::universal_io::Result;

use crate::read::AsyncRead;

/// Mutating blob-backend operations (filesystem-level).
///
/// The write-side counterpart of [`AsyncRead`], powering the
/// [`UniversalWriteFileOps`] impl on [`BlobFs`](crate::BlobFs). Same rules as
/// [`AsyncRead`]: implementations only describe the async work as
/// `Send + 'static` futures; the sync wrappers own the runtime and drive
/// them.
///
/// [`UniversalWriteFileOps`]: common::universal_io::UniversalWriteFileOps
pub trait AsyncWrite: AsyncRead {
    /// Create (or truncate to empty) the object at `path`.
    fn create(&self, path: &Path) -> impl Future<Output = Result<()>> + Send + 'static;

    /// Delete the object at `path`.
    fn remove(&self, path: &Path) -> impl Future<Output = Result<()>> + Send + 'static;

    /// Atomically replace the object at `path` with `bytes` in a single put.
    fn save(&self, path: &Path, bytes: Bytes) -> impl Future<Output = Result<()>> + Send + 'static;
}

/// Blob backends supporting native single-request appends.
///
/// Powers the [`UniversalAppend`] impl on [`BlobFile`](crate::BlobFile).
///
/// [`UniversalAppend`]: common::universal_io::UniversalAppend
pub trait AsyncAppend: AsyncWrite {
    /// Append `data` to the object at `path` in a single request, growing it
    /// in place. Returns the new total object size in bytes.
    ///
    /// `offset` MUST equal the current object size (`0` creates the object
    /// if it is missing) and acts as a compare-and-swap token: the backend
    /// rejects a mismatching append with
    /// [`UniversalIoError::AppendOffsetConflict`], so concurrent appenders
    /// cannot silently interleave.
    ///
    /// [`UniversalIoError::AppendOffsetConflict`]: common::universal_io::UniversalIoError::AppendOffsetConflict
    fn append(
        &self,
        path: &Path,
        offset: u64,
        data: Bytes,
    ) -> impl Future<Output = Result<u64>> + Send + 'static;
}
