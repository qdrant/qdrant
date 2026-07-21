use std::borrow::Cow;
use std::fmt;
use std::ops::Range;
use std::path::Path;

use bytemuck::TransparentWrapper;

use super::WrappedReadPipeline;
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::AccessPattern;
use crate::universal_io::traits::UniversalReadFileOps;
use crate::universal_io::{
    Item, ListedFile, OpenOptions, ReadBytesItem, ReadRange, UioResult, UniversalIoError,
    UniversalKind, UniversalRead, UniversalReadFs, UserData,
};

#[derive(Debug, TransparentWrapper)]
#[repr(transparent)]
pub struct ReadOnly<S>(S);

/// Phantom filesystem handle whose `File` type is `ReadOnly<F::File>`.
///
/// Exists purely to satisfy the `UniversalReadFs<File = Self>` constraint
/// on `UniversalRead::Fs` for the `ReadOnly<S>` wrapper. It wraps an inner `F: UniversalReadFs`
/// and asserts read-only semantics on `open`. In practice this Fs is
/// rarely instantiated; callers use [`ReadOnly::open`] with the
/// underlying `&S::Fs` directly.
#[derive(Clone)]
pub struct ReadOnlyFs<F>(F);

impl<F: fmt::Debug> fmt::Debug for ReadOnlyFs<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(inner) = self;
        f.debug_tuple("ReadOnlyFs").field(inner).finish()
    }
}

impl<F: UniversalReadFileOps> UniversalReadFileOps for ReadOnlyFs<F> {
    type ContextConfig = ReadOnlyConfigContext<F::ContextConfig>;

    fn from_context(ctx: Self::ContextConfig) -> UioResult<Self> {
        Ok(ReadOnlyFs(F::from_context(ctx.0)?))
    }

    fn list_files(&self, prefix_path: &Path) -> UioResult<Vec<ListedFile>> {
        self.0.list_files(prefix_path)
    }

    fn exists(&self, path: &Path) -> UioResult<bool> {
        self.0.exists(path)
    }

    // Deliberately no `UniversalWriteFileOps` impl: read-only is a
    // compile-time property of this wrapper.
}

impl<F: UniversalReadFs> UniversalReadFs for ReadOnlyFs<F> {
    type File = ReadOnly<F::File>;
    type OpenExtra = F::OpenExtra;

    fn open(
        &self,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: F::OpenExtra,
    ) -> UioResult<Self::File> {
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
    /// Open a read-only file through the given filesystem handle — the
    /// canonical `S::Fs` or any other filesystem producing `S`-typed
    /// handles (e.g. [`CachedReadFs`](crate::universal_io::CachedReadFs)).
    ///
    /// Asserts the request is read-only (panics in debug builds on a writeable
    /// `OpenOptions`); the wrapper itself does not enforce write protection
    /// beyond not exposing `UniversalWrite`.
    #[inline]
    pub fn open<Fs: UniversalReadFs<File = S>>(
        fs: &Fs,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: Fs::OpenExtra,
    ) -> UioResult<Self> {
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

    type ReadPipeline<'file, U>
        = WrappedReadPipeline<Self, S::ReadPipeline<'file, U>>
    where
        Self: 'file,
        U: UserData;

    #[inline]
    fn reopen(&mut self) -> UioResult<()> {
        self.0.reopen()
    }

    #[inline]
    fn read<P: AccessPattern, T: Item>(
        &self,
        range: ReadRange,
        access_pattern: P,
    ) -> UioResult<Cow<'_, [T]>> {
        self.0.read(range, access_pattern)
    }

    #[inline]
    fn read_bytes<P: AccessPattern>(&self, range: Range<u64>, align: usize) -> UioResult<ACow<'_>> {
        self.0.read_bytes::<P>(range, align)
    }

    #[inline]
    fn read_whole<T: Item>(&self) -> UioResult<Cow<'_, [T]>> {
        self.0.read_whole()
    }

    #[inline]
    fn read_batch<P, T, U, E>(
        &self,
        ranges: impl IntoIterator<Item = (U, ReadRange)>,
        access_pattern: P,
        callback: impl FnMut(U, &[T]) -> Result<(), E>,
    ) -> Result<(), E>
    where
        P: AccessPattern,
        T: Item,
        U: UserData,
        E: From<UniversalIoError>,
    {
        self.0.read_batch(ranges, access_pattern, callback)
    }

    #[inline]
    fn read_iter<P: AccessPattern, T: Item, U: UserData>(
        &self,
        ranges: impl IntoIterator<Item = (U, ReadRange)>,
        access_pattern: P,
    ) -> UioResult<impl Iterator<Item = UioResult<(U, Cow<'_, [T]>)>>> {
        self.0.read_iter(ranges, access_pattern)
    }

    #[inline]
    fn read_bytes_iter<P: AccessPattern, U: UserData>(
        &self,
        ranges: impl IntoIterator<Item = ReadBytesItem<U>>,
        access_pattern: P,
    ) -> UioResult<impl Iterator<Item = UioResult<(U, ACow<'_>)>>> {
        self.0.read_bytes_iter(ranges, access_pattern)
    }

    #[inline]
    fn len<T>(&self) -> UioResult<u64> {
        self.0.len::<T>()
    }

    #[inline]
    fn populate(&self) -> UioResult<()> {
        self.0.populate()
    }

    #[inline]
    fn populate_auto() -> bool {
        S::populate_auto()
    }

    #[inline]
    fn clear_ram_cache(&self) -> UioResult<()> {
        self.0.clear_ram_cache()
    }

    fn kind() -> UniversalKind {
        S::kind()
    }
}
