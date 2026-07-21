use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;
use std::path::Path;

use bytemuck::TransparentWrapper;

use crate::generic_consts::AccessPattern;
use crate::universal_io::{
    ByteOffset, FileIndex, Flusher, Item, OpenOptions, ReadRange, UioResult, UniversalAppend,
    UniversalFlush, UniversalIoError, UniversalKind, UniversalRead, UniversalReadFs,
    UniversalWrite, UserData,
};

/// A wrapper around [`UniversalRead`]/[`UniversalWrite`] that binds the element
/// type to a specific `T`.
///
/// The underlying read/write methods are generic over `T` per call. This
/// wrapper forwards them with `T` fixed, acting as a fail-safe against
/// accidentally reading or writing the wrong type from a generic storage.
#[derive(TransparentWrapper)]
#[repr(transparent)]
#[transparent(S)]
pub struct TypedStorage<S, T> {
    pub inner: S,
    _phantom: PhantomData<T>,
}

impl<S: fmt::Debug, T> fmt::Debug for TypedStorage<S, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { inner, _phantom: _ } = self;
        f.debug_struct("TypedStorage")
            .field("inner", inner)
            .finish()
    }
}

impl<S, T> TypedStorage<S, T> {
    /// Approximate RAM usage in bytes. IO-backed storage has no significant
    /// heap allocations; on-disk data is accounted via `files()`.
    pub fn ram_usage_bytes(&self) -> usize {
        0
    }
}

#[expect(clippy::len_without_is_empty)]
impl<S, T> TypedStorage<S, T>
where
    S: UniversalRead,
    T: Item,
{
    pub fn new(inner: S) -> Self {
        TypedStorage {
            inner,
            _phantom: PhantomData,
        }
    }

    /// Open through the provided filesystem handle and wrap the result.
    #[inline]
    pub fn open<Fs: UniversalReadFs<File = S>>(
        fs: &Fs,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: Fs::OpenExtra,
    ) -> UioResult<Self> {
        fs.open(path, options, extra).map(Self::new)
    }

    pub fn reopen(&mut self) -> UioResult<()> {
        self.inner.reopen()
    }

    #[inline]
    pub fn read<P: AccessPattern>(&self, range: ReadRange) -> UioResult<Cow<'_, [T]>> {
        self.inner.read::<P, T>(range)
    }

    #[inline]
    pub fn read_whole(&self) -> UioResult<Cow<'_, [T]>> {
        self.inner.read_whole::<T>()
    }

    #[inline]
    pub fn read_batch<P: AccessPattern, U: UserData, E: From<UniversalIoError>>(
        &self,
        ranges: impl IntoIterator<Item = (U, ReadRange)>,
        access_pattern: P,
        callback: impl FnMut(U, &[T]) -> Result<(), E>,
    ) -> Result<(), E> {
        self.inner.read_batch(ranges, access_pattern, callback)
    }

    #[inline]
    pub fn read_iter<P, U>(
        &self,
        ranges: impl IntoIterator<Item = (U, ReadRange)>,
        access_pattern: P,
    ) -> UioResult<impl Iterator<Item = UioResult<(U, Cow<'_, [T]>)>>>
    where
        P: AccessPattern,
        U: UserData,
    {
        self.inner.read_iter(ranges, access_pattern)
    }

    #[inline]
    pub fn len(&self) -> UioResult<u64> {
        self.inner.len::<T>()
    }

    #[inline]
    pub fn populate(&self) -> UioResult<()> {
        self.inner.populate()
    }

    #[inline]
    pub fn clear_ram_cache(&self) -> UioResult<()> {
        self.inner.clear_ram_cache()
    }

    pub fn kind() -> UniversalKind {
        S::kind()
    }
}

impl<S, T> TypedStorage<S, T>
where
    S: UniversalWrite,
    T: bytemuck::Pod,
{
    #[inline]
    pub fn write(&mut self, byte_offset: ByteOffset, data: &[T]) -> UioResult<()> {
        self.inner.write::<T>(byte_offset, data)
    }

    #[inline]
    pub fn write_batch<'a>(
        &mut self,
        offset_data: impl IntoIterator<Item = (ByteOffset, &'a [T])>,
    ) -> UioResult<()>
    where
        T: 'a,
    {
        self.inner.write_batch::<T>(offset_data)
    }

    #[inline]
    pub fn write_multi<'a>(
        files: &mut [Self],
        writes: impl IntoIterator<Item = (FileIndex, ByteOffset, &'a [T])>,
    ) -> UioResult<()>
    where
        T: 'a,
    {
        S::write_multi::<T>(Self::peel_slice_mut(files), writes)
    }
}

// On `UniversalFlush` rather than `UniversalWrite` so append-only storages
// can run their durability flusher through the wrapper too.
impl<S, T> TypedStorage<S, T>
where
    S: UniversalFlush,
{
    #[inline]
    pub fn flusher(&self) -> Flusher {
        self.inner.flusher()
    }
}

impl<S, T> TypedStorage<S, T>
where
    S: UniversalAppend,
    T: bytemuck::Pod,
{
    /// Append `data` at exactly byte offset `offset`, which must equal the
    /// current end of file.
    #[inline]
    pub fn append(&mut self, offset: ByteOffset, data: &[T]) -> UioResult<()> {
        self.inner.append::<T>(offset, data)
    }

    /// Append several buffers contiguously, starting at exactly byte offset
    /// `offset`, which must equal the current end of file.
    #[inline]
    pub fn append_batch<'a>(
        &mut self,
        offset: ByteOffset,
        items: impl IntoIterator<Item = &'a [T]>,
    ) -> UioResult<()>
    where
        T: 'a,
    {
        self.inner.append_batch::<T>(offset, items)
    }
}
