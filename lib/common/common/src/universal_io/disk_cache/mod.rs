use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use fs_err as fs;

use crate::ext::aligned_vec::ACow;
use crate::generic_consts::AccessPattern;
use crate::universal_io::{
    ListedFile, OpenOptions, Result, UniversalIoError, UniversalRead, UniversalReadFileOps,
    UniversalReadFs, UniversalWriteFileOps, UserData, local_file_ops,
};

mod cached_slice;
mod controller;
mod pipeline;
#[cfg(test)]
mod tests;

pub use cached_slice::CachedSlice;
use controller::{CacheController, CacheRead};
use pipeline::DiskCacheReadPipeline;

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

/// Construction context for [`BlockCacheFs`]: carries the shared cache
/// controller. Defaults to the global controller when one has been
/// installed via `CacheController::initialize_global`; for tests and
/// multi-tenant code, pass an explicit `Arc<CacheController>`.
#[derive(Debug, Clone)]
pub struct BlockCacheConfigContext {
    pub controller: Arc<CacheController>,
}

impl Default for BlockCacheConfigContext {
    fn default() -> Self {
        let controller = CacheController::global()
            .expect("CacheController::initialize_global must be called before BlockCacheConfigContext::default()")
            .clone();
        BlockCacheConfigContext { controller }
    }
}

/// Filesystem handle for the block-based disk cache.
#[derive(Debug, Clone)]
pub struct BlockCacheFs {
    controller: Arc<CacheController>,
}

impl UniversalReadFileOps for BlockCacheFs {
    type ContextConfig = BlockCacheConfigContext;

    fn from_context(ctx: BlockCacheConfigContext) -> Result<Self> {
        Ok(Self {
            controller: ctx.controller,
        })
    }

    fn list_files(&self, prefix_path: &Path) -> Result<Vec<ListedFile>> {
        local_file_ops::local_list_files(prefix_path)
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        fs::exists(path).map_err(UniversalIoError::from)
    }
}

impl UniversalWriteFileOps for BlockCacheFs {
    fn create(&self, path: &Path, expected_length: usize) -> Result<()> {
        local_file_ops::local_create(path, expected_length)
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        local_file_ops::local_create_dir(path)
    }

    fn remove(&self, path: &Path) -> Result<()> {
        local_file_ops::local_remove(path)
    }

    fn remove_dir(&self, path: &Path) -> Result<()> {
        local_file_ops::local_remove_dir(path)
    }

    fn atomic_save(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        local_file_ops::local_atomic_save(path, bytes)
    }
}

impl UniversalReadFs for BlockCacheFs {
    type File = CachedSlice;
    type OpenExtra = ();

    fn open(
        &self,
        path: impl AsRef<Path>,
        options: OpenOptions,
        _extra: (),
    ) -> Result<CachedSlice> {
        let OpenOptions {
            writeable,
            need_sequential: _,
            populate: _,
            advice: _,
        } = options;
        debug_assert!(!writeable);

        Ok(CachedSlice::open(&self.controller, path.as_ref())?)
    }
}

impl UniversalRead for CachedSlice {
    type Fs = BlockCacheFs;

    type ReadPipeline<'a, U>
        = DiskCacheReadPipeline<'a, U>
    where
        Self: 'a,
        U: UserData;

    fn reopen(&mut self) -> Result<()> {
        // TODO: revise if this is the best way to reopen
        *self = CachedSlice::open(&self.controller, &self.path)?;
        Ok(())
    }

    fn read_bytes<P: AccessPattern>(&self, range: Range<u64>, align: usize) -> Result<ACow<'_>> {
        let start = usize::try_from(range.start).expect("range.start is within usize");
        let end = usize::try_from(range.end).expect("range.end is within usize");
        Ok(self.get_range_bytes(start..end, align)?)
    }

    fn len<T>(&self) -> Result<u64> {
        Ok(Self::len::<T>(self) as u64)
    }

    fn populate(&self) -> Result<()> {
        Ok(self.populate()?)
    }

    fn populate_auto() -> bool {
        false
    }

    fn clear_ram_cache(&self) -> Result<()> {
        // TODO: issue fadvise DONTNEED on the cache file's backing mmap region.
        Ok(())
    }

    fn kind() -> UniversalKind {
        UniversalKind::DiskCache
    }
}
