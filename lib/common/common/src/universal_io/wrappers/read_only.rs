use std::borrow::Cow;
use std::path::Path;

use bytemuck::TransparentWrapper;

use super::{BorrowedWrappedReadPipeline, OwnedWrappedReadPipeline};
use crate::generic_consts::AccessPattern;
use crate::universal_io::{
    Item, OpenOptions, ReadRange, Result, UniversalKind, UniversalRead, UniversalReadFs, UserData,
};

#[derive(Debug, TransparentWrapper)]
#[repr(transparent)]
pub struct ReadOnly<S>(S);

impl<S> ReadOnly<S>
where
    S: UniversalRead,
{
    /// Open a read-only file through the given filesystem handle.
    ///
    /// Asserts the request is read-only (panics in debug builds on a writeable
    /// `OpenOptions`); the wrapper itself does not enforce write protection
    /// beyond not exposing `UniversalWrite`.
    #[inline]
    pub fn open<Fs>(fs: &Fs, path: impl AsRef<Path>, options: OpenOptions) -> Result<Self>
    where
        Fs: UniversalReadFs<File = S>,
    {
        debug_assert!(!options.writeable);
        let io = fs.open(path, options)?;
        Ok(Self(io))
    }
}

impl<S> UniversalRead for ReadOnly<S>
where
    S: UniversalRead,
{
    type BorrowedReadPipeline<'file, T, U>
        = BorrowedWrappedReadPipeline<'file, Self, S::BorrowedReadPipeline<'file, T, U>>
    where
        Self: 'file,
        T: Item,
        U: UserData;

    type OwnedReadPipeline<T, U>
        = OwnedWrappedReadPipeline<Self, S::OwnedReadPipeline<T, U>>
    where
        T: Item,
        U: UserData;

    #[inline]
    fn reopen(&mut self) -> Result<()> {
        self.0.reopen()
    }

    #[inline]
    fn read<P: AccessPattern, T: Item>(&self, range: ReadRange) -> Result<Cow<'_, [T]>> {
        self.0.read::<P, T>(range)
    }

    #[inline]
    fn read_whole<T: Item>(&self) -> Result<Cow<'_, [T]>> {
        self.0.read_whole()
    }

    #[inline]
    fn read_batch<P, T, U>(
        &self,
        ranges: impl IntoIterator<Item = (U, ReadRange)>,
        callback: impl FnMut(U, &[T]) -> Result<()>,
    ) -> Result<()>
    where
        P: AccessPattern,
        T: Item,
        U: UserData,
    {
        self.0.read_batch::<P, T, U>(ranges, callback)
    }

    #[inline]
    fn read_iter<P, T, U>(
        &self,
        ranges: impl IntoIterator<Item = (U, ReadRange)>,
    ) -> Result<impl Iterator<Item = Result<(U, Cow<'_, [T]>)>>>
    where
        P: AccessPattern,
        T: Item,
        U: UserData,
    {
        self.0.read_iter::<P, T, U>(ranges)
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
    fn read_multi<'a, P, T, U>(
        reads: impl IntoIterator<Item = (U, &'a Self, ReadRange)>,
        callback: impl FnMut(U, &[T]) -> Result<()>,
    ) -> Result<()>
    where
        P: AccessPattern,
        T: Item,
        U: UserData,
        Self: 'a,
    {
        let reads = reads
            .into_iter()
            .map(|(user_data, file, range)| (user_data, &file.0, range));

        S::read_multi::<P, T, _>(reads, callback)
    }

    #[inline]
    fn read_multi_iter<'a, P, T, U>(
        reads: impl IntoIterator<Item = (U, &'a Self, ReadRange)>,
    ) -> Result<impl Iterator<Item = Result<(U, Cow<'a, [T]>)>>>
    where
        P: AccessPattern,
        T: Item,
        U: UserData,
        Self: 'a,
    {
        let it = reads
            .into_iter()
            .map(|(user_data, file, range)| (user_data, &file.0, range));

        S::read_multi_iter::<P, T, _>(it)
    }

    fn kind() -> UniversalKind {
        S::kind()
    }
}
