use std::future::Future;
use std::ops::Range;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use common::universal_io::{Result, UniversalKind};
use futures::stream::BoxStream;

/// Read-capable blob backend (S3, GCS, …). One impl per backend.
///
/// - [`open`](AsyncRead::open) builds the backend handle from a
///   backend-specific [`Config`](AsyncRead::Config). It performs no IO and does
///   not touch any runtime.
/// - The remaining methods are per-handle and take the object (or prefix)
///   `path` they operate on. Each one that may hit the network returns a
///   `Send + 'static` future, so the caller decides how to drive it: a single
///   read is run via [`BridgeRuntime::block_on`], while batched/pipelined reads
///   are shipped through the runtime worker's MPSC channel after being boxed at
///   the channel boundary.
///
/// Implementations must not block or own a runtime themselves — they only
/// describe the async work. The sync [`BlobFile`](crate::BlobFile) wrapper owns
/// the [`BridgeRuntime`](crate::BridgeRuntime) and is responsible for executing
/// these futures.
///
/// A future `AsyncWrite` trait will live next to this one in `write.rs`.
pub trait AsyncRead: Send + Sync + Sized + 'static {
    type Config;

    fn open(config: &Self::Config) -> Result<Self>;

    fn list_files(
        &self,
        prefix: &Path,
    ) -> impl Future<Output = Result<Vec<PathBuf>>> + Send + 'static;

    fn exists(&self, path: &Path) -> impl Future<Output = Result<bool>> + Send + 'static;

    /// Create or truncate an empty object at `path`.
    fn create(&self, path: &Path) -> impl Future<Output = Result<()>> + Send + 'static;

    /// Remove the object at `path`.
    fn remove(&self, path: &Path) -> impl Future<Output = Result<()>> + Send + 'static;

    /// Remove all objects matching the directory prefix at `path`.
    fn remove_dir(&self, path: &Path) -> impl Future<Output = Result<()>> + Send + 'static;

    /// Save bytes by overwriting the full object at `path`.
    fn atomic_save(
        &self,
        path: &Path,
        bytes: Bytes,
    ) -> impl Future<Output = Result<()>> + Send + 'static;

    /// Fetch `range` from `path` as a stream of byte chunks.
    ///
    /// The returned future resolves once the request has been initiated and the
    /// server has acknowledged the range; the actual bytes are yielded by the
    /// stream. Consumers can fold the chunks directly into a typed, aligned
    /// destination buffer (see [`BlobFile::read`](crate::BlobFile)) without an
    /// intermediate `Bytes` aggregation.
    fn read_range(
        &self,
        path: &Path,
        range: Range<u64>,
    ) -> impl Future<Output = Result<BoxStream<'static, Result<Bytes>>>> + Send + 'static;

    /// Fetch the object at `path` from byte offset `from` to its end in one
    /// request — no separate `len`/HEAD round-trip. `from == 0` reads the whole
    /// object.
    ///
    /// The returned `u64` is the **total size of the whole object, in bytes**
    /// (as reported by the GET response, e.g. parsed from `Content-Range`/
    /// `Content-Length`). It is *not* the length of the returned tail: the
    /// stream yields exactly `total - from` bytes, so the absolute offset of EOF
    /// is `total`, and on success `from <= total` always holds. For `from == 0`
    /// the two coincide (`total` bytes are streamed).
    ///
    /// If `from` is at or past the end of the object the request may be rejected
    /// by the backend as an unsatisfiable range (e.g. HTTP 416). Callers that
    /// must tolerate an empty tail should disambiguate with [`len`](Self::len);
    /// see `pipeline::read_from_into_byte_buffer`.
    fn read_from(
        &self,
        path: &Path,
        from: u64,
    ) -> impl Future<Output = Result<(u64, BoxStream<'static, Result<Bytes>>)>> + Send + 'static;

    /// Fetch the whole object at `path` in one request. Convenience for
    /// [`read_from(path, 0)`](Self::read_from).
    fn read_whole(
        &self,
        path: &Path,
    ) -> impl Future<Output = Result<(u64, BoxStream<'static, Result<Bytes>>)>> + Send + 'static
    {
        self.read_from(path, 0)
    }

    fn len(&self, path: &Path) -> impl Future<Output = Result<u64>> + Send + 'static;

    fn is_empty(&self, path: &Path) -> impl Future<Output = Result<bool>> + Send + 'static {
        let len = self.len(path);
        async move { Ok(len.await? == 0) }
    }

    fn kind() -> UniversalKind;
}
