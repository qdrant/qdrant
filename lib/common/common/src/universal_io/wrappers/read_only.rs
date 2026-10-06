use std::borrow::Cow;
use std::fmt;
use std::ops::Range;
use std::path::Path;

use bytemuck::TransparentWrapper;

use super::WrappedReadPipeline;
use crate::counter::hardware_counter::HardwareCounterCell;
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::AccessPattern;
use crate::universal_io::cached_fs::FileInfo;
use crate::universal_io::{
    Item, ListedFile, OpenOptions, ReadBytesItem, ReadRange, UioResult, UniversalIoError,
    UniversalKind, UniversalRead, UniversalReadFs, UserData,
};
// Async impls (`UniversalReadAsync` / `UniversalReadFsAsync`) live in
// `super::async_io`, gated on the wrapped backend being async-capable.

#[derive(Debug, TransparentWrapper)]
#[repr(transparent)]
pub struct ReadOnly<S>(pub(super) S);

/// Phantom filesystem handle whose `File` type is `ReadOnly<F::File>`.
///
/// Exists purely to satisfy the `UniversalReadFs<File = Self>` constraint
/// on `UniversalRead::Fs` for the `ReadOnly<S>` wrapper. It wraps an inner `F: UniversalReadFs`
/// and asserts read-only semantics on `open`. In practice this Fs is
/// rarely instantiated; callers use [`ReadOnly::open`] with the
/// underlying `&S::Fs` directly.
#[derive(Clone)]
pub struct ReadOnlyFs<F>(pub(super) F);

impl<F: fmt::Debug> fmt::Debug for ReadOnlyFs<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(inner) = self;
        f.debug_tuple("ReadOnlyFs").field(inner).finish()
    }
}

impl<F: UniversalReadFs> UniversalReadFs for ReadOnlyFs<F> {
    type File = ReadOnly<F::File>;
    type OpenExtra = F::OpenExtra;
    type ContextConfig = ReadOnlyConfigContext<F::ContextConfig>;

    fn from_context(ctx: Self::ContextConfig) -> UioResult<Self> {
        Ok(ReadOnlyFs(F::from_context(ctx.0)?))
    }

    fn list_files(
        &self,
        prefix_path: &Path,
        hw_counter: &HardwareCounterCell,
    ) -> UioResult<Vec<ListedFile>> {
        self.0.list_files(prefix_path, hw_counter)
    }

    fn exists(&self, path: &Path, hw_counter: &HardwareCounterCell) -> UioResult<bool> {
        self.0.exists(path, hw_counter)
    }

    fn open(
        &self,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: F::OpenExtra,
        hw_counter: &HardwareCounterCell,
    ) -> UioResult<Self::File> {
        debug_assert!(!options.writeable);
        Ok(ReadOnly(self.0.open(path, options, extra, hw_counter)?))
    }
}

// Deliberately no `UniversalWriteFs` impl: read-only is a compile-time
// property of this wrapper.

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
        hw_counter: &HardwareCounterCell,
    ) -> UioResult<Self> {
        debug_assert!(!options.writeable);
        let io = fs.open(path, options, extra, hw_counter)?;
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
    fn live_reload(&mut self, hw_counter: &HardwareCounterCell) -> UioResult<()> {
        self.0.live_reload(hw_counter)
    }

    #[inline]
    fn live_preload<F: FnOnce(&Path) -> Option<FileInfo>>(
        &self,
        get_file_info: F,
        hw_counter: &HardwareCounterCell,
    ) -> UioResult<impl Future<Output = ()> + Send + 'static> {
        self.0.live_preload(get_file_info, hw_counter)
    }

    #[inline]
    fn read<P: AccessPattern, T: Item>(
        &self,
        range: ReadRange,
        access_pattern: P,
        hw_counter: &HardwareCounterCell,
    ) -> UioResult<Cow<'_, [T]>> {
        self.0.read(range, access_pattern, hw_counter)
    }

    #[inline]
    fn read_bytes<P: AccessPattern>(
        &self,
        range: Range<u64>,
        access_pattern: P,
        align: usize,
        hw_counter: &HardwareCounterCell,
    ) -> UioResult<ACow<'_>> {
        self.0.read_bytes(range, access_pattern, align, hw_counter)
    }

    #[inline]
    fn read_whole<T: Item>(&self, hw_counter: &HardwareCounterCell) -> UioResult<Cow<'_, [T]>> {
        self.0.read_whole(hw_counter)
    }

    #[inline]
    fn read_batch<P, T, U, E>(
        &self,
        ranges: impl IntoIterator<Item = (U, ReadRange)>,
        access_pattern: P,
        callback: impl FnMut(U, &[T]) -> Result<(), E>,
        hw_counter: &HardwareCounterCell,
    ) -> Result<(), E>
    where
        P: AccessPattern,
        T: Item,
        U: UserData,
        E: From<UniversalIoError>,
    {
        self.0
            .read_batch(ranges, access_pattern, callback, hw_counter)
    }

    #[inline]
    fn read_iter<P: AccessPattern, T: Item, U: UserData>(
        &self,
        ranges: impl IntoIterator<Item = (U, ReadRange)>,
        access_pattern: P,
        hw_counter: &HardwareCounterCell,
    ) -> UioResult<impl Iterator<Item = UioResult<(U, Cow<'_, [T]>)>>> {
        self.0.read_iter(ranges, access_pattern, hw_counter)
    }

    #[inline]
    fn read_bytes_iter<P: AccessPattern, U: UserData>(
        &self,
        ranges: impl IntoIterator<Item = ReadBytesItem<U>>,
        access_pattern: P,
        hw_counter: &HardwareCounterCell,
    ) -> UioResult<impl Iterator<Item = UioResult<(U, ACow<'_>)>>> {
        self.0.read_bytes_iter(ranges, access_pattern, hw_counter)
    }

    #[inline]
    fn len<T>(&self, hw_counter: &HardwareCounterCell) -> UioResult<u64> {
        self.0.len::<T>(hw_counter)
    }

    #[inline]
    fn populate(&self, hw_counter: &HardwareCounterCell) -> UioResult<()> {
        self.0.populate(hw_counter)
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
