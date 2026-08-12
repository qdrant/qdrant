//! Combined remote-blob + local-cache file: the append-capable universal-IO
//! citizen for object stores.
//!
//! [`BlobFile`] alone can only append on backends with a native write-offset
//! append, and [`DiskCache`] alone is strictly read-only. [`CachedBlobFile`]
//! pairs the two: reads are served through the lazily-populated local mirror,
//! while [`UniversalAppend::append`] performs the remote mutation
//! immediately — a native write-offset append, or a whole-object rewrite for
//! stores without native append (see [`AppendMode`]).
//!
//! Appends are durable once the remote acknowledges them, like raw
//! [`BlobFile`] appends (the flusher is a no-op); callers batch upstream, so
//! one `append` call is one remote mutation. The mirror's length is advanced
//! in the same call without extra IO, so this handle's `len`/reads observe
//! the appended bytes right away; the appended blocks themselves fault in
//! from the remote on first read.

mod fs;
mod pipeline;

use std::ops::Range;
use std::path::Path;

use bytes::Bytes;
use common::ext::aligned_vec::ACow;
use common::generic_consts::{AccessPattern, Sequential};
use common::universal_io::{
    ByteOffset, DiskCache, FileInfo, Flusher, UioResult, UniversalAppend, UniversalFlush,
    UniversalIoError, UniversalKind, UniversalRead, UserData,
};
pub use fs::{CachedBlobFs, CachedBlobFsContext};
pub use pipeline::CachedBlobReadPipeline;

use crate::file::BlobFile;
use crate::read::AsyncRead;
use crate::write::AsyncAppend;

/// Minimum remote-prefix size worth a server-side multipart copy; below it a
/// whole-object PUT re-uploads the prefix instead. Mirrors the S3 5 MiB
/// minimum for non-last multipart parts.
// TODO: move to `AsyncRewrite::MIN_COPY_PREFIX` once the backend trait lands.
const MIN_COPY_PREFIX: u64 = 5 * 1024 * 1024;

/// How an append is performed on the remote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppendMode {
    /// The store supports native write-offset appends (S3 Express One Zone,
    /// MinIO AiStor). `soft_limit` rewrites the whole object instead after
    /// that many native appends, staying clear of the store's cap on appended
    /// blocks per object; `None` relies on the reactive fallback alone.
    Native { soft_limit: Option<u32> },
    /// No native append (plain S3): every append rewrites the object — a
    /// whole PUT while it is small, a server-side copy of the remote prefix
    /// once it is large enough.
    Rewrite,
}

/// A remote object handle that reads through a local [`DiskCache`] mirror and
/// appends straight to the remote. See the module docs.
pub struct CachedBlobFile<A: AsyncRead + Clone> {
    cache: DiskCache<BlobFile<A>>,
    remote: BlobFile<A>,
    mode: AppendMode,
    writeable: bool,
    /// Native appends since the last whole-object write, driving
    /// [`AppendMode::Native`]'s `soft_limit`.
    native_appends: u32,
}

impl<A: AsyncRead + Clone> std::fmt::Debug for CachedBlobFile<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            cache,
            remote,
            mode,
            writeable,
            native_appends,
        } = self;
        f.debug_struct("CachedBlobFile")
            .field("cache", cache)
            .field("remote", remote)
            .field("mode", mode)
            .field("writeable", writeable)
            .field("native_appends", native_appends)
            .finish()
    }
}

impl<A: AsyncRead + Clone> CachedBlobFile<A> {
    pub(crate) fn new(
        cache: DiskCache<BlobFile<A>>,
        remote: BlobFile<A>,
        mode: AppendMode,
        writeable: bool,
    ) -> Self {
        Self {
            cache,
            remote,
            mode,
            writeable,
            native_appends: 0,
        }
    }
}

impl<A: AsyncAppend + Clone> CachedBlobFile<A> {
    /// One remote mutation per call: native append or rewrite, then advance
    /// the mirror to the new length without extra IO (a successful append
    /// proves `offset` was the remote EOF).
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

        match self.mode {
            AppendMode::Native { soft_limit } => {
                if soft_limit.is_some_and(|limit| self.native_appends >= limit) {
                    self.rewrite(offset, &data)?;
                } else {
                    // TODO: switch to the inherent `BlobFile::append_native`
                    // once the specialized remote ops land and `BlobFile`
                    // loses its `UniversalAppend` impl.
                    match self.remote.append(offset, data.as_ref()) {
                        Ok(()) => self.native_appends += 1,
                        Err(err) if err_requires_rewrite(&err) => self.rewrite(offset, &data)?,
                        Err(err) => return Err(err),
                    }
                }
            }
            AppendMode::Rewrite => self.rewrite(offset, &data)?,
        }

        let new_len = offset + data.len() as u64;
        self.cache.schedule_reopen(|_| {
            Some(FileInfo {
                size: new_len,
                last_modified: None,
                etag: None,
            })
        })?;
        self.cache.reopen()
    }

    /// Replace the whole remote object with `[0, offset) + data`.
    ///
    /// The remote offers no compare-and-swap here, so `offset` is validated
    /// against the mirror length — this handle's view of the remote EOF,
    /// kept in step after every append (single-writer contract).
    fn rewrite(&mut self, offset: ByteOffset, data: &[u8]) -> UioResult<()> {
        let local_len = self.cache.len::<u8>()?;
        if offset != local_len {
            return Err(UniversalIoError::AppendOffsetConflict {
                path: self.remote.path().to_path_buf(),
                offset,
            });
        }

        if offset == 0 {
            self.save_whole(Bytes::copy_from_slice(data))?;
        } else if offset >= MIN_COPY_PREFIX {
            // Server-side copy of the remote prefix, `data` as the final part.
            todo!("multipart rewrite with UploadPartCopy (AsyncRewrite backend capability)")
        } else {
            // The prefix is small by construction; read it through the cache
            // (served locally once mirrored).
            let prefix = self.cache.read_bytes(0..offset, Sequential, 1)?;
            let mut whole = Vec::with_capacity(prefix.len() + data.len());
            whole.extend_from_slice(&prefix);
            whole.extend_from_slice(data);
            self.save_whole(Bytes::from(whole))?;
        }

        self.native_appends = 0;
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

/// Whether a native-append failure means the store demands a full rewrite
/// (append-block limit exceeded, or write-offset appends unsupported).
// TODO: classify once `UniversalIoError::AppendRewriteRequired` exists; until
// then only the proactive `soft_limit` triggers rewrites in `Native` mode.
fn err_requires_rewrite(_err: &UniversalIoError) -> bool {
    false
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

    fn reopen(&mut self) -> UioResult<()> {
        self.cache.reopen()
    }

    fn schedule_reopen<F: FnOnce(&Path) -> Option<FileInfo>>(
        &self,
        get_file_info: F,
    ) -> UioResult<()> {
        self.cache.schedule_reopen(get_file_info)
    }

    fn read_bytes<P: AccessPattern>(
        &self,
        range: Range<u64>,
        access_pattern: P,
        align: usize,
    ) -> UioResult<ACow<'_>> {
        self.cache.read_bytes(range, access_pattern, align)
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
