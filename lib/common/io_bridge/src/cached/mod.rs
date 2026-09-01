//! Combined remote-blob + local-cache file: the append-capable universal-IO
//! citizen for object stores.
//!
//! [`BlobFile`] alone can only append on backends with a native write-offset
//! append, and [`DiskCache`] alone is strictly read-only. [`CachedBlobFile`]
//! pairs the two: reads are served through the lazily-populated local mirror,
//! while [`UniversalAppend::append`] performs the remote mutation
//! immediately — a direct append, or a whole-object rewrite when the
//! backend's advertised [`AppendSupport`] leaves the caller to build the
//! object itself.
//!
//! Appends are durable once the remote acknowledges them, like raw
//! [`BlobFile`] appends (the flusher is a no-op); callers batch upstream, so
//! one `append` call is one remote append operation. The mirror's length is
//! advanced in the same call without extra IO, so this handle's `len`/reads
//! observe the appended bytes right away; the appended blocks themselves
//! fault in from the remote on first read.

mod fs;
mod pipeline;
#[cfg(test)]
mod tests;

use std::ops::Range;
use std::path::Path;

use bytes::Bytes;
use common::ext::aligned_vec::ACow;
use common::generic_consts::{AccessPattern, Sequential};
use common::universal_io::{
    ByteOffset, DiskCache, FileInfo, Flusher, OkNotFound as _, UioResult, UniversalAppend,
    UniversalFlush, UniversalIoError, UniversalKind, UniversalRead, UserData,
};
pub use fs::{CachedBlobFs, CachedBlobFsContext};
pub use pipeline::CachedBlobReadPipeline;

use crate::file::BlobFile;
use crate::read::AsyncRead;
use crate::write::{AppendSupport, AsyncAppend};

/// A remote object handle that reads through a local [`DiskCache`] mirror and
/// appends straight to the remote. See the module docs.
pub struct CachedBlobFile<A: AsyncRead + Clone> {
    cache: DiskCache<BlobFile<A>>,
    remote: BlobFile<A>,
    writeable: bool,
}

impl<A: AsyncRead + Clone> std::fmt::Debug for CachedBlobFile<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            cache,
            remote,
            writeable,
        } = self;
        f.debug_struct("CachedBlobFile")
            .field("cache", cache)
            .field("remote", remote)
            .field("writeable", writeable)
            .finish()
    }
}

impl<A: AsyncRead + Clone> CachedBlobFile<A> {
    pub(crate) fn new(cache: DiskCache<BlobFile<A>>, remote: BlobFile<A>, writeable: bool) -> Self {
        Self {
            cache,
            remote,
            writeable,
        }
    }
}

impl<A: AsyncAppend + Clone> CachedBlobFile<A> {
    /// One remote append operation per call: direct append or whole-object
    /// rewrite, then advance the mirror to the new length without extra IO
    /// (a successful append proves `offset` was the remote EOF).
    fn append_bytes(&mut self, offset: ByteOffset, data: Bytes) -> UioResult<()> {
        if data.is_empty() {
            return Ok(());
        }

        if !self.writeable {
            return Err(UniversalIoError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "append requires a handle opened with writeable=true",
            )));
        }

        let new_len = offset + data.len() as u64;

        let enabled = log::log_enabled!(target: crate::LATENCY_LOG_TARGET, log::Level::Trace);
        let start_time = enabled.then(std::time::Instant::now);

        let strategy = match self.remote.source().append_support() {
            AppendSupport::Always => {
                self.remote.append_bytes(offset, data, self.cache.etag())?;
                "append"
            }
            AppendSupport::AboveThreshold { min_offset } => {
                if offset >= min_offset {
                    self.remote.append_bytes(offset, data, self.cache.etag())?;
                    "append"
                } else {
                    self.local_rewrite(offset, data)?;
                    "rewrite"
                }
            }
            AppendSupport::Never => {
                self.local_rewrite(offset, data)?;
                "rewrite"
            }
        };

        if let Some(start_time) = start_time {
            log::trace!(
                target: crate::LATENCY_LOG_TARGET,
                "append_bytes({}, {offset}..{new_len}) took {:?} via {strategy}",
                self.remote.path().display(),
                start_time.elapsed(),
            );
        }

        let _ = self.cache.live_preload(|_| {
            Some(FileInfo {
                size: new_len,
                last_modified: None,
                etag: None,
            })
        });
        self.cache.live_reload()
    }

    /// Replace the whole remote object with `[0, offset) + data`, built on
    /// this side — for stores (or object sizes) without direct appends.
    fn local_rewrite(&self, offset: ByteOffset, data: Bytes) -> UioResult<()> {
        self.check_offset(offset)?;

        if offset == 0 {
            self.save_whole(data)
        } else {
            // The prefix is small by construction (below the direct-append
            // threshold); read it through the cache (served locally once
            // mirrored).
            let prefix = self.cache.read_bytes(0..offset, Sequential, 1)?;
            let mut whole = Vec::with_capacity(prefix.len() + data.len());
            whole.extend_from_slice(&prefix);
            whole.extend_from_slice(&data);
            self.save_whole(Bytes::from(whole))
        }
    }

    /// The rewrites offer no backend compare-and-swap, so `offset` is
    /// validated against the mirror length — this handle's view of the
    /// remote EOF, kept in step after every append (single-writer contract).
    ///
    /// A remote object that is not there yet reads as length zero: the
    /// offset-0 rewrite is what creates it, matching the direct-append
    /// backends (a GCS compose or native append at offset 0 also creates
    /// the object).
    fn check_offset(&self, offset: ByteOffset) -> UioResult<()> {
        let local_len = self.cache.len::<u8>().ok_not_found()?.unwrap_or(0);
        if offset != local_len {
            return Err(UniversalIoError::AppendOffsetConflict {
                path: self.remote.path().to_path_buf(),
                offset,
            });
        }
        Ok(())
    }

    /// Whole-object atomic PUT.
    // TODO: move to an inherent `BlobFile::save_whole` alongside the other
    // specialized remote ops.
    fn save_whole(&self, data: Bytes) -> UioResult<()> {
        self.remote
            .runtime()
            .block_on(self.remote.source().save(self.remote.path(), data))
    }
}

impl<A: AsyncAppend + Clone> UniversalRead for CachedBlobFile<A>
where
    A::Config: Clone,
{
    type Fs = CachedBlobFs<A>;

    type ReadPipeline<'a, U>
        = CachedBlobReadPipeline<'a, A, U>
    where
        Self: 'a,
        U: UserData;

    fn live_reload(&mut self) -> UioResult<()> {
        self.cache.live_reload()
    }

    fn live_preload<F: FnOnce(&Path) -> Option<FileInfo>>(
        &self,
        get_file_info: F,
    ) -> UioResult<impl Future<Output = ()> + Send + 'static> {
        self.cache.live_preload(get_file_info)
    }

    fn read_bytes<P: AccessPattern>(
        &self,
        range: Range<u64>,
        access_pattern: P,
        align: usize,
    ) -> UioResult<ACow<'_>> {
        self.cache.read_bytes(range, access_pattern, align)
    }

    fn read_bytes_async<P: AccessPattern>(
        &self,
        range: Range<u64>,
        access_pattern: P,
        align: usize,
    ) -> impl Future<Output = UioResult<ACow<'_>>> {
        self.cache.read_bytes_async(range, access_pattern, align)
    }

    fn read_whole<T: common::universal_io::Item>(&self) -> UioResult<std::borrow::Cow<'_, [T]>> {
        self.cache.read_whole()
    }

    fn len<T>(&self) -> UioResult<u64> {
        self.cache.len::<T>()
    }

    fn populate(&self) -> UioResult<()> {
        self.cache.populate()
    }

    fn populate_auto() -> bool {
        false
    }

    fn clear_ram_cache(&self) -> UioResult<()> {
        self.cache.clear_ram_cache()
    }

    fn kind() -> UniversalKind {
        UniversalKind::CachedBlob
    }
}

impl<A: AsyncAppend + Clone> UniversalFlush for CachedBlobFile<A> {
    fn flusher(&self) -> Flusher {
        // Appends are durable once the remote acknowledges them.
        Box::new(|| Ok(()))
    }
}

impl<A: AsyncAppend + Clone> UniversalAppend for CachedBlobFile<A>
where
    A::Config: Clone,
{
    fn append<T: bytemuck::Pod>(&mut self, offset: ByteOffset, data: &[T]) -> UioResult<()> {
        self.append_bytes(offset, Bytes::copy_from_slice(bytemuck::cast_slice(data)))
    }

    fn append_batch<'a, T: bytemuck::Pod>(
        &mut self,
        offset: ByteOffset,
        items: impl IntoIterator<Item = &'a [T]>,
    ) -> UioResult<()> {
        // Concatenate into a single buffer so the whole batch is one remote
        // mutation.
        let slices: Vec<&[u8]> = items
            .into_iter()
            .map(|item| bytemuck::cast_slice(item))
            .collect();
        let total: usize = slices.iter().map(|slice| slice.len()).sum();
        let mut buffer = Vec::with_capacity(total);
        for slice in slices {
            buffer.extend_from_slice(slice);
        }

        self.append_bytes(offset, Bytes::from(buffer))
    }
}
