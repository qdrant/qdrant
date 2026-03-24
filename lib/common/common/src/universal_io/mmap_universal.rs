use std::borrow::Cow;
use std::marker::PhantomData;
use std::path::PathBuf;

use super::*;
use crate::generic_consts::AccessPattern;

/// Read-write mmap
pub type MmapUniversalRw<T> = MmapUniversal<T>;

/// Read-only mmap
pub type MmapUniversalRo<T> = ReadOnly<MmapUniversal<T>>;

#[derive(Debug)]
pub struct MmapUniversal<T> {
    mmap: MmapFile,
    _phantom: PhantomData<T>,
}

impl<T> UniversalReadFileOps for MmapUniversal<T> {
    #[inline]
    fn list_files(prefix_path: &Path) -> Result<Vec<PathBuf>> {
        MmapFile::list_files(prefix_path)
    }

    #[inline]
    fn exists(path: &Path) -> Result<bool> {
        MmapFile::exists(path)
    }
}

impl<T> UniversalRead<T> for MmapUniversal<T>
where
    T: bytemuck::Pod,
{
    #[inline]
    fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let mmap = Self {
            mmap: UniversalRead::<T>::open(path, options)?,
            _phantom: PhantomData,
        };

        Ok(mmap)
    }

    #[inline]
    fn read<P: AccessPattern>(&self, range: ReadRange) -> Result<Cow<'_, [T]>> {
        self.mmap.read::<P>(range)
    }

    #[inline]
    fn read_batch<P: AccessPattern>(
        &self,
        ranges: impl IntoIterator<Item = ReadRange>,
        callback: impl FnMut(usize, &[T]) -> Result<()>,
    ) -> Result<()> {
        self.mmap.read_batch::<P>(ranges, callback)
    }

    #[inline]
    fn len(&self) -> Result<u64> {
        UniversalRead::<T>::len(&self.mmap)
    }

    #[inline]
    fn populate(&self) -> Result<()> {
        UniversalRead::<T>::populate(&self.mmap)
    }

    #[inline]
    fn clear_ram_cache(&self) -> Result<()> {
        UniversalRead::<T>::clear_ram_cache(&self.mmap)
    }
}

impl<T> UniversalWrite<T> for MmapUniversal<T>
where
    T: bytemuck::Pod,
{
    #[inline]
    fn write(&mut self, offset: ByteOffset, data: &[T]) -> Result<()> {
        self.mmap.write(offset, data)
    }

    #[inline]
    fn write_batch<'a>(
        &mut self,
        offset_data: impl IntoIterator<Item = (ByteOffset, &'a [T])>,
    ) -> Result<()> {
        self.mmap.write_batch(offset_data)
    }

    #[inline]
    fn flusher(&self) -> Flusher {
        UniversalWrite::<T>::flusher(&self.mmap)
    }
}
