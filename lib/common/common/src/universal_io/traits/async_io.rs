use std::ops::Range;
use std::path::PathBuf;

use futures::future::BoxFuture;

use super::{UniversalRead, UniversalReadFs};
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::AccessPattern;
use crate::universal_io::{OpenOptions, UioResult};

/// Async-capable extension of [`UniversalRead`].
///
/// Implemented only by backends whose reads can genuinely overlap with the
/// caller (the blob family and the disk caches layered over it), plus a
/// trivial [`MmapFile`](crate::universal_io::MmapFile) impl so local-backend
/// configurations (tests, mmap-backed lookup segments) satisfy async-bounded
/// code. Purely local backends with no async story (io_uring, the block
/// cache) deliberately do not implement it.
pub trait UniversalReadAsync: UniversalRead {
    /// Async-capable version of [`UniversalRead::read_bytes`].
    fn read_bytes_async<P: AccessPattern>(
        &self,
        range: Range<u64>,
        access_pattern: P,
        align: usize,
    ) -> impl Future<Output = UioResult<ACow<'_>>> + Send;

    /// Warm the cache for a range of bytes.
    /// No-op on backends without caching.
    fn populate_range_async(&self, range: Range<u64>)
    -> impl Future<Output = UioResult<()>> + Send;
}

/// Async-capable extension of [`UniversalReadFs`]: filesystems whose opens
/// (including any populate) can be driven as a future. See
/// [`UniversalReadAsync`] for which backends implement the async surface.
///
/// [`CachedFs`](crate::universal_io::CachedFs) requires this of its inner
/// filesystem: scheduled prefetches are `open_async` futures handed to
/// [`Self::spawn`] and parked until an awaited barrier
/// ([`CachedFs::resolve_prefetched`](crate::universal_io::CachedFs::resolve_prefetched))
/// or consume time.
pub trait UniversalReadFsAsync: UniversalReadFs<File: UniversalReadAsync> {
    /// Open a file, and populate it asynchronously.
    ///
    /// Must not depend on ambient async context; capture any runtime by value.
    fn open_async(
        &self,
        path: PathBuf,
        options: OpenOptions,
        extra: Self::OpenExtra,
    ) -> impl Future<Output = UioResult<Self::File>> + Send + '_;

    fn spawn<T: Send + 'static>(
        &self,
        fut: BoxFuture<'static, UioResult<T>>,
    ) -> BoxFuture<'static, UioResult<T>>;
}

/// The async whole-file write surface, mirroring the same operations on
/// [`UniversalWriteFileOps`]: a batch of saves runs as one concurrent wave
/// instead of a thread per file.
///
/// As with [`UniversalReadFsAsync::open_async`], the futures must not depend on
/// ambient async context, so any executor can drive them — including
/// `futures::executor::block_on` from synchronous code. Nothing runs before the
/// first poll, so a caller that bounds its wave really does bound the in-flight
/// saves, however it collects the futures.
///
/// [`UniversalWriteFileOps`]: crate::universal_io::UniversalWriteFileOps
pub trait UniversalWriteFsAsync: Send + Sync {
    /// Backends without materialized directories may treat this as a no-op.
    fn create_dir_async(&self, path: PathBuf) -> impl Future<Output = UioResult<()>> + Send + '_;

    fn atomic_save_async(
        &self,
        path: PathBuf,
        bytes: Vec<u8>,
    ) -> impl Future<Output = UioResult<()>> + Send + '_;

    fn remove_async(&self, path: PathBuf) -> impl Future<Output = UioResult<()>> + Send + '_;
}
