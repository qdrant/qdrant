use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fs_err as fs;

use crate::disk_cache::{CacheController, CachedSlice};
use crate::generic_consts::AccessPattern;
use crate::universal_io::{
    OpenOptions, ReadRange, Result, UniversalIoError, UniversalRead, UniversalReadFileOps,
    local_file_ops,
};

pub fn with_global<U>(f: impl FnOnce(&Arc<CacheController>) -> Result<U>) -> Result<U> {
    let Some(global) = CacheController::global() else {
        return Err(UniversalIoError::uninitialized(
            "Disk cache was not initialized when trying to use it",
        ));
    };

    f(global)
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
            need_sequential: _,
            disk_parallel: _,
            populate: _,
            advice: _,
        } = options;

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
        // TODO: read all content of this file to make sure it is in the cache file.
        Ok(())
    }

    fn clear_ram_cache(&self) -> Result<()> {
        // TODO: issue fadvise DONTNEED on the cache file's backing mmap region.
        Ok(())
    }
}
