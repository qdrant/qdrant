use std::borrow::Cow;
use std::fmt;
use std::ops::Range;
use std::path::{Path, PathBuf};

use bytemuck::TransparentWrapper;

use super::{BorrowedWrappedReadPipeline, OwnedWrappedReadPipeline};
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::AccessPattern;
use crate::universal_io::traits::UniversalReadFileOps;
use crate::universal_io::{
    Item, OpenOptions, ReadBytesItem, ReadRange, Result, UniversalIoError, UniversalKind,
    UniversalRead, UniversalReadFs, UserData,
};

#[derive(Debug, TransparentWrapper)]
#[repr(transparent)]
pub struct ReadOnly<S>(S);

/// Phantom filesystem handle whose `File` type is `ReadOnly<F::File>`.
///
/// Exists purely to satisfy the bidirectional
/// `UniversalReadFs<File = Self>` constraint on `UniversalRead::Fs` for
/// the `ReadOnly<S>` wrapper. It wraps an inner `F: UniversalReadFs`
/// and asserts read-only semantics on `open`. In practice this Fs is
/// rarely instantiated; callers use [`ReadOnly::open`] with the
/// underlying `&S::Fs` directly.
#[derive(Clone)]
pub struct ReadOnlyFs<F>(F);

impl<F: fmt::Debug> fmt::Debug for ReadOnlyFs<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ReadOnlyFs").field(&self.0).finish()
    }
}

impl<F: UniversalReadFileOps> UniversalReadFileOps for ReadOnlyFs<F> {
    type ContextConfig = ReadOnlyConfigContext<F::ContextConfig>;

    fn from_context(ctx: Self::ContextConfig) -> Result<Self> {
        Ok(ReadOnlyFs(F::from_context(ctx.0)?))
    }

    fn list_files(&self, prefix_path: &Path) -> Result<Vec<PathBuf>> {
        self.0.list_files(prefix_path)
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        self.0.exists(path)
    }

    fn create(&self, _path: &Path, _expected_length: usize) -> Result<()> {
        Err(UniversalIoError::uninitialized(
            "ReadOnlyFs does not support creating files",
        ))
    }

    fn create_dir(&self, _path: &Path) -> Result<()> {
        Err(UniversalIoError::uninitialized(
            "ReadOnlyFs does not support creating directories",
        ))
    }

    fn remove(&self, _path: &Path) -> Result<()> {
        Err(UniversalIoError::uninitialized(
            "ReadOnlyFs does not support removing files",
        ))
    }

    fn remove_dir(&self, _path: &Path) -> Result<()> {
        Err(UniversalIoError::uninitialized(
            "ReadOnlyFs does not support removing directories",
        ))
    }

    fn atomic_save(&self, _path: &Path, _bytes: &[u8]) -> Result<()> {
        Err(UniversalIoError::uninitialized(
            "ReadOnlyFs does not support atomic saves",
        ))
    }
}

impl<F: UniversalReadFs> UniversalReadFs for ReadOnlyFs<F> {
    type File = ReadOnly<F::File>;
    type OpenExtra = F::OpenExtra;

    fn open(
        &self,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: F::OpenExtra,
    ) -> Result<Self::File> {
        debug_assert!(!options.writeable);
        Ok(ReadOnly(self.0.open(path, options, extra)?))
    }
}

/// Construction context for [`ReadOnlyFs`], forwarding to the inner Fs's
/// context.
#[derive(Debug, Clone, Default)]
pub struct ReadOnlyConfigContext<C>(pub C);

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
    pub fn open(
        fs: &S::Fs,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: <S::Fs as UniversalReadFs>::OpenExtra,
    ) -> Result<Self> {
        debug_assert!(!options.writeable);
        let io = fs.open(path, options, extra)?;
        Ok(Self(io))
    }
}

impl<S> UniversalRead for ReadOnly<S>
where
    S: UniversalRead,
{
    type Fs = ReadOnlyFs<S::Fs>;

    type BorrowedReadPipeline<'file, U>
        = BorrowedWrappedReadPipeline<'file, Self, S::BorrowedReadPipeline<'file, U>>
    where
        Self: 'file,
        U: UserData;

    type OwnedReadPipeline<U>
        = OwnedWrappedReadPipeline<Self, S::OwnedReadPipeline<U>>
    where
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
    fn read_bytes<P: AccessPattern>(&self, range: Range<u64>, align: usize) -> Result<ACow<'_>> {
        self.0.read_bytes::<P>(range, align)
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
    fn read_bytes_iter<P, U>(
        &self,
        ranges: impl IntoIterator<Item = ReadBytesItem<U>>,
    ) -> Result<impl Iterator<Item = Result<(U, ACow<'_>)>>>
    where
        P: AccessPattern,
        U: UserData,
    {
        self.0.read_bytes_iter::<P, U>(ranges)
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
    fn populate_auto() -> bool {
        S::populate_auto()
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
