use std::borrow::Cow;
use std::sync::Arc;

use fs_err as fs;

use crate::disk_cache::{self, CachedFile};
use crate::universal_io::{
    OpenOptions, Result, UniversalIoError, UniversalRead, UniversalReadFileOps, local_file_ops,
};

pub fn with_global<U>(f: impl FnOnce(&Arc<disk_cache::CacheController>) -> Result<U>) -> Result<U> {
    let Some(global) = disk_cache::CacheController::global() else {
        return Err(UniversalIoError::uninitialized(
            "Disk cache was not initialized when trying to use it",
        ));
    };

    f(global)
}

impl UniversalReadFileOps for CachedFile {
    fn list_files(
        prefix_path: &std::path::Path,
    ) -> crate::universal_io::Result<Vec<std::path::PathBuf>> {
        local_file_ops::local_list_files(prefix_path)
    }

    fn exists(path: &std::path::Path) -> crate::universal_io::Result<bool> {
        fs::exists(path).map_err(UniversalIoError::from)
    }
}

impl<T: bytemuck::Pod> UniversalRead<T> for CachedFile {
    fn open(path: impl AsRef<std::path::Path>, options: OpenOptions) -> Result<Self>
    where
        Self: Sized,
    {
        let Some(controller) = disk_cache::CacheController::global() else {
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

        let file = controller.open_file(path.as_ref())?;

        Ok(file)
    }

    fn read<const SEQUENTIAL: bool>(
        &self,
        range: crate::universal_io::ElementsRange,
    ) -> Result<Cow<'_, [T]>> {
        let t_size = std::mem::size_of::<T>();

        let elem_start = usize::try_from(range.start).expect("range.start is within usize");
        let elem_length = usize::try_from(range.length).expect("range.length is within usize");

        let start = elem_start * t_size;
        let end = start + elem_length * t_size;
        let byte_range = start..end;

        let data = self.get_range(byte_range);

        match data {
            Cow::Borrowed(bytes) => {
                let slice = bytemuck::try_cast_slice(bytes)?;
                Ok(Cow::Borrowed(slice))
            }
            Cow::Owned(vec_u8) => {
                let vec_t = bytemuck::try_cast_vec(vec_u8).map_err(|(err, _vec)| err)?;
                Ok(Cow::Owned(vec_t))
            }
        }
    }

    fn read_batch<const SEQUENTIAL: bool>(
        &self,
        ranges: impl IntoIterator<Item = crate::universal_io::ElementsRange>,
        mut callback: impl FnMut(usize, &[T]) -> crate::universal_io::Result<()>,
    ) -> crate::universal_io::Result<()> {
        for (i, range) in ranges.into_iter().enumerate() {
            let data = self.read::<SEQUENTIAL>(range)?;
            callback(i, &data)?;
        }

        Ok(())
    }

    fn len(&self) -> crate::universal_io::Result<u64> {
        Ok(Self::len(self) as u64)
    }

    fn populate(&self) -> crate::universal_io::Result<()> {
        todo!("make sure the disk cache has this file")
    }

    fn clear_ram_cache(&self) -> crate::universal_io::Result<()> {
        todo!("WONTNEED for the range of this file")
    }
}
