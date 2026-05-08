use std::borrow::Cow;
use std::path::{Path, PathBuf};

use bytemuck::TransparentWrapper;

use super::super::{
    OpenOptions, ReadRange, Result, UniversalKind, UniversalRead, UniversalReadFileOps,
};
use super::WrappedReadPipeline;
use crate::generic_consts::AccessPattern;

#[derive(Debug, TransparentWrapper)]
#[repr(transparent)]
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

impl<S> UniversalRead for ReadOnly<S>
where
    S: UniversalRead,
{
    type ReadPipeline<'file, T, Meta>
        = WrappedReadPipeline<'file, Self, S::ReadPipeline<'file, T, Meta>>
    where
        Self: 'file,
        T: bytemuck::Pod;

    #[inline]
    fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        debug_assert!(!options.writeable);
        let io = S::open(path, options)?;
        Ok(Self(io))
    }

    #[inline]
    fn read<P: AccessPattern, T: bytemuck::Pod>(&self, range: ReadRange) -> Result<Cow<'_, [T]>> {
        self.0.read::<P, T>(range)
    }

    #[inline]
    fn read_whole<T: bytemuck::Pod>(&self) -> Result<Cow<'_, [T]>> {
        self.0.read_whole()
    }

    #[inline]
    fn read_batch<P, T, Meta>(
        &self,
        ranges: impl IntoIterator<Item = (Meta, ReadRange)>,
        callback: impl FnMut(Meta, &[T]) -> Result<()>,
    ) -> Result<()>
    where
        P: AccessPattern,
        T: bytemuck::Pod,
    {
        self.0.read_batch::<P, T, Meta>(ranges, callback)
    }

    #[inline]
    fn read_iter<P, T, Meta>(
        &self,
        ranges: impl IntoIterator<Item = (Meta, ReadRange)>,
    ) -> Result<impl Iterator<Item = Result<(Meta, Cow<'_, [T]>)>>>
    where
        P: AccessPattern,
        T: bytemuck::Pod,
    {
        self.0.read_iter::<P, T, Meta>(ranges)
    }

    #[inline]
    fn len<T>(&self) -> Result<u64> {
        self.0.len::<T>()
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
    fn read_multi<'a, P, T, Meta>(
        reads: impl IntoIterator<Item = (Meta, &'a Self, ReadRange)>,
        callback: impl FnMut(Meta, &[T]) -> Result<()>,
    ) -> Result<()>
    where
        P: AccessPattern,
        T: bytemuck::Pod,
        Self: 'a,
    {
        let reads = reads
            .into_iter()
            .map(|(meta, file, range)| (meta, &file.0, range));

        S::read_multi::<P, T, _>(reads, callback)
    }

    #[inline]
    fn read_multi_iter<'a, P, T, Meta>(
        reads: impl IntoIterator<Item = (Meta, &'a Self, ReadRange)>,
    ) -> Result<impl Iterator<Item = Result<(Meta, Cow<'a, [T]>)>>>
    where
        P: AccessPattern,
        T: bytemuck::Pod,
        Self: 'a,
    {
        let it = reads
            .into_iter()
            .map(|(meta, file, range)| (meta, &file.0, range));

        S::read_multi_iter::<P, T, _>(it)
    }

    fn kind() -> UniversalKind {
        S::kind()
    }
}
