use std::borrow::Cow;
use std::path::{Path, PathBuf};

use bytemuck::TransparentWrapper;

use super::super::{
    FileIndex, OpenOptions, ReadRange, Result, UniversalRead, UniversalReadFileOps,
};
use crate::generic_consts::AccessPattern;

#[derive(Debug, TransparentWrapper)]
#[repr(transparent)]
#[transparent(S)]
pub struct ReadOnly<S>(S);

impl<S> UniversalReadFileOps for ReadOnly<S>
where
    S: UniversalReadFileOps,
{
    #[inline]
    fn list_files(prefix_path: &Path) -> Result<Vec<PathBuf>> {
        S::list_files(prefix_path)
    }

    #[inline]
    fn exists(path: &Path) -> Result<bool> {
        S::exists(path)
    }
}

impl<S, T> UniversalRead<T> for ReadOnly<S>
where
    S: UniversalRead<T>,
    T: Copy + 'static,
{
    #[inline]
    fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        debug_assert!(!options.writeable);
        let io = S::open(path, options)?;
        Ok(Self(io))
    }

    #[inline]
    fn read<P: AccessPattern>(&self, range: ReadRange) -> Result<Cow<'_, [T]>> {
        self.0.read::<P>(range)
    }

    #[inline]
    fn read_whole(&self) -> Result<Cow<'_, [T]>> {
        self.0.read_whole()
    }

    #[inline]
    fn read_batch<P: AccessPattern>(
        &self,
        ranges: impl IntoIterator<Item = ReadRange>,
        callback: impl FnMut(usize, &[T]) -> Result<()>,
    ) -> Result<()> {
        self.0.read_batch::<P>(ranges, callback)
    }

    #[inline]
    fn len(&self) -> Result<u64> {
        self.0.len()
    }

    #[inline]
    fn populate(&self) -> Result<()> {
        self.0.populate()
    }

    #[inline]
    fn clear_ram_cache(&self) -> Result<()> {
        self.0.clear_ram_cache()
    }

    #[inline]
    fn read_multi<P: AccessPattern>(
        files: &[Self],
        reads: impl IntoIterator<Item = (FileIndex, ReadRange)>,
        callback: impl FnMut(usize, FileIndex, &[T]) -> Result<()>,
    ) -> Result<()> {
        S::read_multi::<P>(Self::peel_slice(files), reads, callback)
    }
}
