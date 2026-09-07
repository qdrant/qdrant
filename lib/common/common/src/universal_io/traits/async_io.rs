use std::ops::Range;
use std::path::PathBuf;

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
}

/// Async-capable extension of [`UniversalReadFs`]: filesystems whose opens
/// (including any populate) can be driven as a future. See
/// [`UniversalReadAsync`] for which backends implement the async surface.
///
/// [`CachedFs`](crate::universal_io::CachedFs) requires this of its inner
/// filesystem: scheduled prefetches are parked `open_async` futures, resolved
/// either by an awaited barrier
/// ([`CachedFs::resolve_prefetched`](crate::universal_io::CachedFs::resolve_prefetched))
/// or lazily at consume time.
pub trait UniversalReadFsAsync: UniversalReadFs {
    /// Open a file, and populate it asynchronously.
    ///
    /// Must not depend on ambient async context; capture any runtime by value.
    fn open_async(
        &self,
        path: PathBuf,
        options: OpenOptions,
        extra: Self::OpenExtra,
    ) -> impl Future<Output = UioResult<Self::File>> + Send + '_;
}

/// The async whole-file write surface, mirroring the same operations on
/// [`UniversalWriteFileOps`].
///
/// A batch of saves runs as one concurrent wave on the backend's own IO
/// runtime instead of a thread per file; synchronous callers drive the wave
/// through [`Self::block_on`]. Local backends resolve inline, as they do for
/// [`UniversalReadFsAsync`].
///
/// [`UniversalWriteFileOps`]: crate::universal_io::UniversalWriteFileOps
pub trait UniversalWriteFsAsync: Send + Sync {
    /// Create a directory. Backends without materialized directories may
    /// treat this as a no-op.
    fn create_dir_async(&self, path: PathBuf) -> impl Future<Output = UioResult<()>> + Send + '_;

    /// Atomically save `bytes` at `path` in a single write.
    fn atomic_save_async(
        &self,
        path: PathBuf,
        bytes: Vec<u8>,
    ) -> impl Future<Output = UioResult<()>> + Send + '_;

    /// Remove the file at `path`.
    fn remove_async(&self, path: PathBuf) -> impl Future<Output = UioResult<()>> + Send + '_;

    /// Drive a future built from this filesystem's async operations to
    /// completion from synchronous code, on the executor those operations need.
    fn block_on<F: Future>(&self, fut: F) -> F::Output;
}
