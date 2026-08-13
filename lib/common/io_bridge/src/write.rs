use std::future::Future;
use std::path::Path;

use bytes::Bytes;
use common::universal_io::UioResult;

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
    fn create(&self, path: &Path) -> impl Future<Output = UioResult<()>> + Send + 'static;

    /// Delete the object at `path`.
    fn remove(&self, path: &Path) -> impl Future<Output = UioResult<()>> + Send + 'static;

    /// Atomically replace the object at `path` with `bytes` in a single put.
    fn save(
        &self,
        path: &Path,
        bytes: Bytes,
    ) -> impl Future<Output = UioResult<()>> + Send + 'static;
}

/// How a backend grows objects, advertised by
/// [`AsyncAppend::supported_append`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppendMethod {
    /// Native single-request write-offset appends (S3 Express One Zone,
    /// MinIO AiStor). Stores cap appended blocks per object; hitting the cap
    /// surfaces as [`UniversalIoError::AppendRewriteRequired`] and callers
    /// recover with a whole-object rewrite.
    ///
    /// [`UniversalIoError::AppendRewriteRequired`]: common::universal_io::UniversalIoError::AppendRewriteRequired
    Native,
    /// No native append (plain S3): the object only grows by a rewrite —
    /// a server-side copy of the existing prefix with the new data uploaded
    /// as the final multipart part.
    PartialUpload,
}

/// One append operation; each variant matches an [`AppendMethod`].
///
/// In both variants `offset` MUST equal the current object size and acts as
/// a compare-and-swap token: the backend rejects a mismatching request with
/// [`UniversalIoError::AppendOffsetConflict`], so concurrent appenders
/// cannot silently interleave.
///
/// [`UniversalIoError::AppendOffsetConflict`]: common::universal_io::UniversalIoError::AppendOffsetConflict
#[derive(Clone, Debug)]
pub enum AppendRequest {
    /// Append `data` at `offset` in a single native write-offset request,
    /// growing the object in place. `offset == 0` creates the object if it
    /// is missing.
    Native { offset: u64, data: Bytes },
    /// Rewrite the object as its existing `[0, offset)` prefix (server-side
    /// copy, never downloaded) followed by `data`. Also the appended-block
    /// cap recovery path for [`AppendMethod::Native`] backends, which all
    /// sit on multipart-capable stores.
    PartialUpload { offset: u64, data: Bytes },
}

/// Blob backends supporting appends.
///
/// Powers the [`UniversalAppend`] impl on [`BlobFile`](crate::BlobFile).
///
/// [`UniversalAppend`]: common::universal_io::UniversalAppend
pub trait AsyncAppend: AsyncWrite {
    /// The append method this backend advertises; callers pick the matching
    /// [`AppendRequest`] variant. Implementations reject request variants
    /// they do not support.
    fn supported_append(&self) -> AppendMethod;

    /// Perform `request` on the object at `path`. Returns the new total
    /// object size in bytes. See [`AppendRequest`] for the offset
    /// compare-and-swap contract.
    fn append(
        &self,
        path: &Path,
        request: AppendRequest,
    ) -> impl Future<Output = UioResult<u64>> + Send + 'static;
}
