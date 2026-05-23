use std::borrow::Cow;
use std::ops::Range;
use std::path::{Path, PathBuf};

use fs_err as fs;

use std::sync::Arc;

use crate::generic_consts::AccessPattern;
use crate::universal_io::{
    Item, OpenOptions, OpenOptionsExtra, ReadRange, Result, ShardStorageContext, UniversalIoError,
    UniversalRead, UniversalReadFileOps, UserData, local_file_ops,
};

mod cached_slice;
mod controller;
mod pipeline;
#[cfg(test)]
mod tests;

pub use cached_slice::CachedSlice;
pub use controller::CacheController;
use controller::CacheRead;
use pipeline::{BorrowedDiskCacheReadPipeline, OwnedDiskCacheReadPipeline};

use super::UniversalKind;

/// We cache data in blocks of this size.
/// Should be multiple of filesystem block size (usually 4 KiB).
const BLOCK_SIZE: usize = 16 * 1024;

/// Internal identifier of a cold file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct FileId(u32);

/// Offset within a file, in blocks.
///
/// `u32` with 16 KiB blocks covers up to 70 TiB worth of data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct BlockOffset(u32);

impl BlockOffset {
    /// The same offset but in bytes instead of blocks.
    fn bytes(self) -> usize {
        self.0 as usize * BLOCK_SIZE
    }
}

/// This pair uniquely identifies a block in a cold file.
/// Acts as a cache key.
#[derive(Copy, Hash, PartialEq, Eq, Clone, Debug)]
struct BlockId {
    file_id: FileId,
    offset: BlockOffset,
}

/// A request for a range of bytes inside of a block
struct BlockRequest {
    key: BlockId,
    range: Range<usize>,
}

/// Per-shard configuration consumed by [`CachedSlice::open_with_extras`].
#[derive(Debug, Clone)]
pub struct BlockCacheExtras {
    /// Controller owning the cache file and block lifecycle.
    pub controller: Arc<CacheController>,
}

impl UniversalReadFileOps for CachedSlice {
    fn list_files(prefix_path: &Path) -> Result<Vec<PathBuf>> {
        local_file_ops::local_list_files(prefix_path)
    }

    fn exists(path: &Path) -> Result<bool> {
        fs::exists(path).map_err(UniversalIoError::from)
    }
}

impl UniversalRead for CachedSlice {
    type BorrowedReadPipeline<'a, T, U>
        = BorrowedDiskCacheReadPipeline<'a, T, U>
    where
        Self: 'a,
        T: Item,
        U: UserData;

    type OwnedReadPipeline<T, U>
        = OwnedDiskCacheReadPipeline<T, U>
    where
        T: Item,
        U: UserData;

    type OpenExtras = BlockCacheExtras;

    fn extras_from_context(ctx: &ShardStorageContext) -> Result<BlockCacheExtras> {
        let cfg = ctx.block_cache.as_ref().ok_or_else(|| {
            UniversalIoError::uninitialized(
                "ShardStorageContext::block_cache is not configured for this shard",
            )
        })?;
        Ok(BlockCacheExtras {
            controller: Arc::clone(&cfg.controller),
        })
    }

    fn open(_path: impl AsRef<Path>, _options: OpenOptions) -> Result<Self> {
        Err(UniversalIoError::uninitialized(
            "block-cache backend requires per-shard configuration; \
             open via `UniversalRead::open_with_extras` with extras from the shard context",
        ))
    }

    fn open_with_extras(
        path: impl AsRef<Path>,
        options: OpenOptions,
        extras: BlockCacheExtras,
    ) -> Result<Self> {
        let OpenOptions {
            writeable,
            populate: _,
            access_hint: _,
            need_sequential: _,
            extra: OpenOptionsExtra { cache_hint: _ }, // This is cached in disk, backed by a mmap
        } = options;
        debug_assert!(!writeable);

        Ok(CachedSlice::open(&extras.controller, path.as_ref())?)
    }

    fn reopen(&mut self) -> Result<()> {
        // TODO: revise if this is the best way to reopen
        *self = CachedSlice::open(&self.controller, &self.path)?;
        Ok(())
    }

    fn read<P: AccessPattern, T: Item>(&self, range: ReadRange) -> Result<Cow<'_, [T]>> {
        let elem_start = usize::try_from(range.byte_offset).expect("range.start is within usize")
            / size_of::<T>();
        let elem_length = usize::try_from(range.length).expect("range.length is within usize");

        let range = elem_start..elem_start + elem_length;

        Ok(self.get_range(range)?)
    }

    fn len<T>(&self) -> Result<u64> {
        Ok(Self::len::<T>(self) as u64)
    }

    fn populate(&self) -> Result<()> {
        Ok(self.populate()?)
    }

    fn clear_ram_cache(&self) -> Result<()> {
        // TODO: issue fadvise DONTNEED on the cache file's backing mmap region.
        Ok(())
    }

    fn kind() -> UniversalKind {
        UniversalKind::DiskCache
    }
}
