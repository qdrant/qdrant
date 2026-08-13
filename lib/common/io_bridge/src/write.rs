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

/// When the backend accepts a direct append ([`AppendRequest::Append`]),
/// advertised by [`AsyncAppend::append_support`]. Deliberately silent on
/// *how* the backend performs the operation — that is the backend's
/// business; this only tells the caller when it must fall back to building
/// the whole object itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppendSupport {
    /// Direct appends at any offset: a write-offset append (S3 Express,
    /// MinIO AiStor) or a `compose` concatenation (GCS). Stores that cap
    /// appended blocks per object surface the cap as
    /// [`UniversalIoError::AppendRewriteRequired`], and callers recover
    /// with one [`AppendRequest::Rewrite`].
    ///
    /// [`UniversalIoError::AppendRewriteRequired`]: common::universal_io::UniversalIoError::AppendRewriteRequired
    Always,
    /// Direct appends only once the existing object is at least
    /// `min_offset` bytes (plain S3: appends land as multipart prefix
    /// copies, whose non-last parts must be ≥ 5 MiB). Below the threshold
    /// the caller must write the whole object itself.
    AboveThreshold { min_offset: u64 },
    /// No direct appends: the caller always writes the whole object itself.
    Never,
}

/// One append operation.
///
/// In both variants `offset` MUST equal the current object size and acts as
/// a compare-and-swap token: the backend rejects a mismatching request with
/// [`UniversalIoError::AppendOffsetConflict`], so concurrent appenders
/// cannot silently interleave.
///
/// [`UniversalIoError::AppendOffsetConflict`]: common::universal_io::UniversalIoError::AppendOffsetConflict
#[derive(Clone, Debug)]
pub enum AppendRequest {
    /// Append `data` at `offset`, growing the object in place — by whatever
    /// mechanism the backend has. `offset == 0` creates the object if it is
    /// missing. Valid within the backend's advertised [`AppendSupport`].
    Append { offset: u64, data: Bytes },
    /// Append `data` at `offset` AND rebuild the object as a single blob:
    /// the appended-block cap recovery for [`AppendSupport::Always`]
    /// stores whose direct appends accumulate per-object blocks.
    Rewrite { offset: u64, data: Bytes },
}

/// Blob backends supporting appends.
///
/// Powers the [`UniversalAppend`] impl on [`BlobFile`](crate::BlobFile).
///
/// [`UniversalAppend`]: common::universal_io::UniversalAppend
pub trait AsyncAppend: AsyncWrite {
    /// When this backend accepts direct appends; below/without that, the
    /// caller writes the whole object itself through [`AsyncWrite::save`].
    fn append_support(&self) -> AppendSupport;

    /// Perform `request` on the object at `path`. Returns the new total
    /// object size in bytes. See [`AppendRequest`] for the offset
    /// compare-and-swap contract.
    fn append(
        &self,
        path: &Path,
        request: AppendRequest,
    ) -> impl Future<Output = UioResult<u64>> + Send + 'static;
}
