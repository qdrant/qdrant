use std::ops::Range;
use std::path::{Path, PathBuf};

use super::{UniversalRead, UniversalReadFs};
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::{AccessPattern, Sequential};
use crate::universal_io::{ListedFile, OpenOptions, UioResult};

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

    /// Read the file starting from `from` into a sink: `init` receives the length of the file as read and
    /// returns the sink, which then gets the file as `(offset, bytes)` chunks as they arrive,
    /// each byte exactly once. Yields the sink once every byte was written.
    fn read_whole_into_async<W, I>(
        &self,
        from: u64,
        init: I,
    ) -> impl Future<Output = UioResult<W>> + Send
    where
        I: FnOnce(u64) -> UioResult<W> + Send + 'static,
        W: ChunkSink + Send + 'static;
}

/// Destination of [`UniversalReadAsync::read_whole_into_async`]'s chunks.
pub trait ChunkSink {
    /// Write the chunk of `bytes` at byte `offset` of the file.
    fn write_chunk(&mut self, offset: u64, bytes: &[u8]) -> UioResult<()>;
}

/// [`UniversalReadAsync::read_whole_into_async`] for files that read the whole file with a
/// single [`UniversalReadAsync::read_bytes_async`].
pub(crate) async fn read_whole_via_read_bytes<F, W, I>(file: &F, from: u64, init: I) -> UioResult<W>
where
    F: UniversalReadAsync + Sync,
    I: FnOnce(u64) -> UioResult<W>,
    W: ChunkSink,
{
    let len = file.len::<u8>()?;
    let mut sink = init(len)?;
    if from < len {
        let bytes = file.read_bytes_async(from..len, Sequential, 1).await?;
        sink.write_chunk(from, &bytes)?;
    }
    Ok(sink)
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

    /// List files matching `prefix_path` asynchronously.
    fn list_files_async<'a>(
        &'a self,
        prefix_path: &'a Path,
    ) -> impl Future<Output = UioResult<Vec<ListedFile>>> + Send + use<'a, Self>;
}

/// The async whole-file write surface, mirroring the same operations on
/// [`UniversalWriteFs`]: a batch of saves runs as one concurrent wave
/// instead of a thread per file.
///
/// As with [`UniversalReadFsAsync::open_async`], the futures must not depend on
/// ambient async context, so any executor can drive them — including
/// `futures::executor::block_on` from synchronous code. Nothing runs before the
/// first poll, so a caller that bounds its wave really does bound the in-flight
/// saves, however it collects the futures.
///
/// [`UniversalWriteFs`]: crate::universal_io::UniversalWriteFs
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
