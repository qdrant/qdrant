use std::ops::Range;
use std::path::{Path, PathBuf};

use fs_err as fs;

use crate::aligned_buf::AlignedCow;
use crate::generic_consts::AccessPattern;
use crate::universal_io::read::UniversalReadPipeline;
use crate::universal_io::{
    OpenOptions, Result, UniversalIoError, UniversalRead, UniversalReadFileOps, UserData,
    local_file_ops,
};

mod cached_slice;
mod controller;
#[cfg(test)]
mod tests;

pub use cached_slice::CachedSlice;
use controller::{CacheController, CacheRead};

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

impl UniversalReadFileOps for CachedSlice {
    fn list_files(prefix_path: &Path) -> Result<Vec<PathBuf>> {
        local_file_ops::local_list_files(prefix_path)
    }

    fn exists(path: &Path) -> Result<bool> {
        fs::exists(path).map_err(UniversalIoError::from)
    }
}

impl UniversalRead for CachedSlice {
    type ReadPipeline<'a, U>
        = DiskCacheReadPipeline<'a, U>
    where
        Self: 'a,
        U: UserData;

    fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let Some(controller) = CacheController::global() else {
            return Err(UniversalIoError::uninitialized(
                "Disk cache was not initialized when trying to register a file",
            ));
        };

        // Disk-cache is backed by a single file
        let OpenOptions {
            writeable,
            need_sequential: _,
            disk_parallel: _,
            populate: _,
            advice: _,
            prevent_caching: _, // This is cached in disk, backed by a mmap
        } = options;

        debug_assert!(!writeable);

        Ok(CachedSlice::open(controller, path.as_ref())?)
    }

    fn read_bytes<P: AccessPattern>(
        &self,
        range: Range<u64>,
        align: usize,
    ) -> Result<AlignedCow<'_>> {
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

    fn clear_ram_cache(&self) -> Result<()> {
        // TODO: issue fadvise DONTNEED on the cache file's backing mmap region.
        Ok(())
    }

    fn kind() -> UniversalKind {
        UniversalKind::DiskCache
    }
}

pub struct DiskCacheReadPipeline<'file, U>
where
    U: UserData,
{
    result: Option<(U, AlignedCow<'file>)>,
}

impl<'file, U> UniversalReadPipeline<'file, U> for DiskCacheReadPipeline<'file, U>
where
    U: UserData,
{
    type File = CachedSlice;

    fn new() -> Result<Self> {
        Ok(Self { result: None })
    }

    fn can_schedule(&mut self) -> bool {
        self.result.is_none()
    }

    fn schedule<P>(
        &mut self,
        user_data: U,
        file: &'file CachedSlice,
        range: Range<u64>,
        align: usize,
    ) -> Result<()>
    where
        P: AccessPattern,
    {
        if self.result.is_some() {
            return Err(UniversalIoError::QueueIsFull);
        }

        let byte_range = range.start as usize..range.end as usize;
        self.result = Some((user_data, file.get_range_bytes(byte_range, align)?));
        Ok(())
    }

    fn wait(&mut self) -> Result<Option<(U, AlignedCow<'file>)>> {
        Ok(self.result.take())
    }
}
