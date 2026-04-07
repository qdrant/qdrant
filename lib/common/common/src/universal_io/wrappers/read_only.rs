use std::borrow::Cow;
use std::path::{Path, PathBuf};

use super::super::{OpenOptions, ReadRange, Result, UniversalRead, UniversalReadFileOps};
use crate::generic_consts::AccessPattern;

#[derive(Debug)]
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
    fn read_batch<'a, P: AccessPattern, Meta: 'a>(
        &'a self,
        ranges: impl IntoIterator<Item = (Meta, ReadRange)>,
        callback: impl FnMut(Meta, &[T]) -> Result<()>,
    ) -> Result<()> {
        self.0.read_batch::<P, Meta>(ranges, callback)
    }

    #[inline]
    fn read_iter<'a, P: AccessPattern, Meta>(
        &'a self,
        ranges: impl IntoIterator<Item = (Meta, ReadRange)>,
    ) -> impl Iterator<Item = Result<(Meta, Cow<'a, [T]>)>> {
        self.0.read_iter::<P, Meta>(ranges)
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
    fn read_multi<'a, P: AccessPattern, Meta: 'a>(
        reads: impl IntoIterator<Item = (Meta, &'a Self, ReadRange)>,
        callback: impl FnMut(Meta, &[T]) -> Result<()>,
    ) -> Result<()>
    where
        Self: 'a,
    {
        let reads = reads
            .into_iter()
            .map(|(meta, file, range)| (meta, &file.0, range));
        S::read_multi::<P, _>(reads, callback)
    }

    #[inline]
    fn read_multi_iter<'a, P: AccessPattern, Meta>(
        reads: impl IntoIterator<Item = (Meta, &'a Self, ReadRange)>,
    ) -> impl Iterator<Item = Result<(Meta, Cow<'a, [T]>)>>
    where
        Self: 'a,
    {
        let it = reads
            .into_iter()
            .map(|(meta, file, range)| (meta, &file.0, range));
        S::read_multi_iter::<P, _>(it)
    }
}
