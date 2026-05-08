use std::borrow::Cow;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use bytemuck::TransparentWrapper;

use super::super::{
    ByteOffset, FileIndex, Flusher, OpenOptions, ReadRange, Result, UniversalKind, UniversalRead,
    UniversalReadFileOps, UniversalWrite,
};
use crate::generic_consts::AccessPattern;

/// A wrapper around [`UniversalRead`]/[`UniversalWrite`] that binds the element
/// type to a specific `T`.
///
/// The underlying read/write methods are generic over `T` per call. This
/// wrapper forwards them with `T` fixed, acting as a fail-safe against
/// accidentally reading or writing the wrong type from a generic storage.
#[derive(Debug, TransparentWrapper)]
#[repr(transparent)]
#[transparent(S)]
pub struct TypedStorage<S, T> {
    pub inner: S,
    _phantom: PhantomData<T>,
}

impl<S, T> TypedStorage<S, T> {
    /// Approximate RAM usage in bytes. IO-backed storage has no significant
    /// heap allocations; on-disk data is accounted via `files()`.
    pub fn ram_usage_bytes(&self) -> usize {
        0
    }
}

impl<S, T> UniversalReadFileOps for TypedStorage<S, T>
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

#[expect(clippy::len_without_is_empty)]
impl<S, T> TypedStorage<S, T>
where
    S: UniversalRead,
    T: bytemuck::Pod,
{
    #[inline]
    pub fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        S::open(path, options).map(|inner| TypedStorage {
            inner,
            _phantom: PhantomData,
        })
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
    pub fn read_batch<P, Meta>(
        &self,
        ranges: impl IntoIterator<Item = (Meta, ReadRange)>,
        callback: impl FnMut(Meta, &[T]) -> Result<()>,
    ) -> Result<()>
    where
        P: AccessPattern,
    {
        self.inner.read_batch::<P, T, Meta>(ranges, callback)
    }

    #[inline]
    pub fn read_iter<P, Meta>(
        &self,
        ranges: impl IntoIterator<Item = (Meta, ReadRange)>,
    ) -> Result<impl Iterator<Item = Result<(Meta, Cow<'_, [T]>)>>>
    where
        P: AccessPattern,
    {
        self.inner.read_iter::<P, T, Meta>(ranges)
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
    pub fn read_multi<'a, P, Meta>(
        reads: impl IntoIterator<Item = (Meta, &'a Self, ReadRange)>,
        callback: impl FnMut(Meta, &[T]) -> Result<()>,
    ) -> Result<()>
    where
        P: AccessPattern,
        Self: 'a,
    {
        let reads = reads
            .into_iter()
            .map(|(meta, file, range)| (meta, &file.inner, range));
        S::read_multi::<P, T, Meta>(reads, callback)
    }

    #[inline]
    pub fn read_multi_iter<'a, P, Meta>(
        reads: impl IntoIterator<Item = (Meta, &'a Self, ReadRange)>,
    ) -> Result<impl Iterator<Item = Result<(Meta, Cow<'a, [T]>)>>>
    where
        P: AccessPattern,
        Self: 'a,
    {
        let reads = reads
            .into_iter()
            .map(|(meta, file, range)| (meta, &file.inner, range));
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
