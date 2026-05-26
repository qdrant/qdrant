use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;
use std::path::Path;

use bytemuck::TransparentWrapper;

use crate::generic_consts::AccessPattern;
use crate::universal_io::{
    ByteOffset, FileIndex, Flusher, Item, OpenOptions, ReadRange, Result, UniversalKind,
    UniversalRead, UniversalReadFs, UniversalWrite, UserData,
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
        f.debug_struct("TypedStorage")
            .field("inner", &self.inner)
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
    /// Open through the provided filesystem handle and wrap the result.
    #[inline]
    pub fn open(
        fs: &S::Fs,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: <S::Fs as UniversalReadFs>::OpenExtra,
    ) -> Result<Self> {
        fs.open(path, options, extra).map(|inner| TypedStorage {
            inner,
            _phantom: PhantomData,
        })
    }

    pub fn reopen(&mut self) -> Result<()> {
        self.inner.reopen()
    }

    #[inline]
    pub fn read<P: AccessPattern>(&self, range: ReadRange) -> Result<Cow<'_, [T]>> {
        self.inner.read::<P, T>(range)
    }

    #[inline]
    pub fn read_whole(&self) -> Result<Cow<'_, [T]>> {
        self.inner.read_whole::<T>()
    }

    #[inline]
    pub fn read_batch<P, U>(
        &self,
        ranges: impl IntoIterator<Item = (U, ReadRange)>,
        callback: impl FnMut(U, &[T]) -> Result<()>,
    ) -> Result<()>
    where
        P: AccessPattern,
        U: UserData,
    {
        self.inner.read_batch::<P, T, U>(ranges, callback)
    }

    #[inline]
    pub fn read_iter<P, U>(
        &self,
        ranges: impl IntoIterator<Item = (U, ReadRange)>,
    ) -> Result<impl Iterator<Item = Result<(U, Cow<'_, [T]>)>>>
    where
        P: AccessPattern,
        U: UserData,
    {
        self.inner.read_iter::<P, T, U>(ranges)
    }

    #[inline]
    pub fn len(&self) -> Result<u64> {
        self.inner.len::<T>()
    }

    #[inline]
    pub fn populate(&self) -> Result<()> {
        self.inner.populate()
    }

    #[inline]
    pub fn clear_ram_cache(&self) -> Result<()> {
        self.inner.clear_ram_cache()
    }

    #[inline]
    pub fn read_multi<'a, P, U>(
        reads: impl IntoIterator<Item = (U, &'a Self, ReadRange)>,
        callback: impl FnMut(U, &[T]) -> Result<()>,
    ) -> Result<()>
    where
        P: AccessPattern,
        U: UserData,
        Self: 'a,
    {
        let reads = reads
            .into_iter()
            .map(|(user_data, file, range)| (user_data, &file.inner, range));
        S::read_multi::<P, T, U>(reads, callback)
    }

    #[inline]
    pub fn read_multi_iter<'a, P, U>(
        reads: impl IntoIterator<Item = (U, &'a Self, ReadRange)>,
    ) -> Result<impl Iterator<Item = Result<(U, Cow<'a, [T]>)>>>
    where
        P: AccessPattern,
        U: UserData,
        Self: 'a,
    {
        let reads = reads
            .into_iter()
            .map(|(user_data, file, range)| (user_data, &file.inner, range));
        S::read_multi_iter::<P, T, _>(reads)
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
    pub fn write(&mut self, byte_offset: ByteOffset, data: &[T]) -> Result<()> {
        self.inner.write::<T>(byte_offset, data)
    }

    #[inline]
    pub fn write_batch<'a>(
        &mut self,
        offset_data: impl IntoIterator<Item = (ByteOffset, &'a [T])>,
    ) -> Result<()>
    where
        T: 'a,
    {
        self.inner.write_batch::<T>(offset_data)
    }

    #[inline]
    pub fn flusher(&self) -> Flusher {
        self.inner.flusher()
    }

    #[inline]
    pub fn write_multi<'a>(
        files: &mut [Self],
        writes: impl IntoIterator<Item = (FileIndex, ByteOffset, &'a [T])>,
    ) -> Result<()>
    where
        T: 'a,
    {
        S::write_multi::<T>(Self::peel_slice_mut(files), writes)
    }
}
