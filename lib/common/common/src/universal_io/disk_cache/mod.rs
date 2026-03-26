use std::borrow::Cow;
use std::ops::Range;
use std::path::{Path, PathBuf};

use fs_err as fs;

use crate::generic_consts::AccessPattern;
use crate::universal_io::{
    OpenOptions, ReadRange, Result, UniversalIoError, UniversalRead, UniversalReadFileOps,
    local_file_ops,
};

mod cached_slice;
mod controller;
#[cfg(test)]
mod tests;

pub use cached_slice::CachedSlice;
use controller::{CacheController, CacheRead};

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

impl<T> UniversalReadFileOps for CachedSlice<T> {
    fn list_files(prefix_path: &Path) -> Result<Vec<PathBuf>> {
        local_file_ops::local_list_files(prefix_path)
    }

    fn exists(path: &Path) -> Result<bool> {
        fs::exists(path).map_err(UniversalIoError::from)
    }
}

impl<T: bytemuck::Pod> UniversalRead<T> for CachedSlice<T> {
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

    fn read<P: AccessPattern>(&self, range: ReadRange) -> Result<Cow<'_, [T]>> {
        let elem_start = usize::try_from(range.byte_offset).expect("range.start is within usize")
            / size_of::<T>();
        let elem_length = usize::try_from(range.length).expect("range.length is within usize");

        let range = elem_start..elem_start + elem_length;

        Ok(self.get_range(range)?)
    }

    fn read_batch<P: AccessPattern>(
        &self,
        ranges: impl IntoIterator<Item = ReadRange>,
        mut callback: impl FnMut(usize, &[T]) -> Result<()>,
    ) -> Result<()> {
        for (i, range) in ranges.into_iter().enumerate() {
            let data = self.read::<P>(range)?;
            callback(i, &data)?;
        }

        Ok(())
    }

    fn len(&self) -> Result<u64> {
        Ok(Self::len(self) as u64)
    }

    fn populate(&self) -> Result<()> {
        Ok(self.populate()?)
    }

    fn clear_ram_cache(&self) -> Result<()> {
        // TODO: issue fadvise DONTNEED on the cache file's backing mmap region.
        Ok(())
    }
}
