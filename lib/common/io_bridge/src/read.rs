use std::future::Future;
use std::ops::Range;
use std::path::Path;

use bytes::Bytes;
use common::universal_io::{ListedFile, UioResult, UniversalKind};
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
/// The write-side counterparts ([`AsyncWrite`], [`AsyncAppend`]) live next
/// to this one in `write.rs`.
///
/// [`AsyncAppend`]: crate::AsyncAppend
/// [`AsyncWrite`]: crate::AsyncWrite
pub trait AsyncRead: Send + Sync + Sized + 'static {
    type Config;

    fn open(config: &Self::Config) -> UioResult<Self>;

    fn list_files(
        &self,
        prefix: &Path,
    ) -> impl Future<Output = UioResult<Vec<ListedFile>>> + Send + 'static;

    fn exists(&self, path: &Path) -> impl Future<Output = UioResult<bool>> + Send + 'static;

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
    ) -> impl Future<Output = UioResult<BoxStream<'static, UioResult<Bytes>>>> + Send + 'static;

    /// Fetch the object at `path` from byte offset `from` to its end — no
    /// separate `len`/HEAD round-trip. `from == 0` reads the whole object.
    /// Implementations may split a large object into multiple range requests
    /// behind the returned stream, but must yield the bytes of a single
    /// consistent version of the object (or fail).
    ///
    /// Each stream item is `(offset, bytes)`: the position of that chunk
    /// **relative to `from`**, plus its bytes. Chunks may arrive **out of
    /// order** — a multi-request implementation is free to yield each range as
    /// it completes — but must be disjoint and tile the tail exactly.
    /// Sequential backends can tag an in-order stream with
    /// [`with_running_offsets`].
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
    ) -> impl Future<Output = UioResult<(u64, OffsetByteStream)>> + Send + 'static;

    /// Fetch the whole object at `path`. Convenience for
    /// [`read_from(path, 0)`](Self::read_from).
    fn read_whole(
        &self,
        path: &Path,
    ) -> impl Future<Output = UioResult<(u64, OffsetByteStream)>> + Send + 'static {
        self.read_from(path, 0)
    }

    fn len(&self, path: &Path) -> impl Future<Output = UioResult<u64>> + Send + 'static;

    fn is_empty(&self, path: &Path) -> impl Future<Output = UioResult<bool>> + Send + 'static {
        let len = self.len(path);
        async move { Ok(len.await? == 0) }
    }

    fn kind() -> UniversalKind;
}

/// Stream of `(offset, bytes)` chunks yielded by [`AsyncRead::read_from`]:
/// each chunk's position relative to the requested start, plus its bytes.
/// Chunks may arrive out of order; they must be disjoint and tile the tail
/// exactly.
pub type OffsetByteStream = BoxStream<'static, UioResult<(u64, Bytes)>>;

/// Tag an in-order byte stream with running offsets, producing the
/// `(offset, bytes)` item shape [`AsyncRead::read_from`] requires. For
/// backends that deliver the tail as one sequential stream.
pub fn with_running_offsets(
    stream: impl futures::Stream<Item = UioResult<Bytes>> + Send + 'static,
) -> OffsetByteStream {
    use futures::StreamExt as _;
    stream
        .scan(0u64, |offset, item| {
            let item = item.map(|bytes| {
                let at = *offset;
                *offset += bytes.len() as u64;
                (at, bytes)
            });
            futures::future::ready(Some(item))
        })
        .boxed()
}
