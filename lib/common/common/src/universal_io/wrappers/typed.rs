use std::borrow::Cow;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use bytemuck::TransparentWrapper;

use super::super::{
    ByteOffset, FileIndex, Flusher, OpenOptions, ReadRange, Result, UniversalRead,
    UniversalReadFileOps, UniversalWrite,
};
use crate::generic_consts::AccessPattern;

/// A wrapper around [`UniversalRead`]/[`UniversalWrite`] that binds `T` to a
/// specific type.
///
/// This wrapper is not needed for code with a single universal io trait bound,
/// (e.g. `where S: UniversalRead<f32>`), but it helps the compiler to
/// distinguish when more than one bound is used, e.g.
/// `where S: UniversalRead<f32> + UniversalRead<PointOffsetType> + …`.
#[derive(Debug, TransparentWrapper)]
#[repr(transparent)]
#[transparent(S)]
pub struct TypedStorage<S, T> {
    pub inner: S,
    _phantom: PhantomData<T>,
}

impl<S: UniversalRead<T>, T: Copy + 'static> UniversalReadFileOps for TypedStorage<S, T> {
    #[inline]
    fn list_files(prefix_path: &Path) -> Result<Vec<PathBuf>> {
        S::list_files(prefix_path)
    }

    #[inline]
    fn exists(path: &Path) -> Result<bool> {
        S::exists(path)
    }
}

impl<S: UniversalRead<T>, T: Copy + 'static> UniversalRead<T> for TypedStorage<S, T> {
    #[inline]
    fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        S::open(path, options).map(|inner| TypedStorage {
            inner,
            _phantom: PhantomData,
        })
    }

    #[inline]
    fn read<P: AccessPattern>(&self, range: ReadRange) -> Result<Cow<'_, [T]>> {
        self.inner.read::<P>(range)
    }

    #[inline]
    fn read_whole(&self) -> Result<Cow<'_, [T]>> {
        self.inner.read_whole()
    }

    #[inline]
    fn read_batch<'a, P: AccessPattern, RequestId: 'a>(
        &'a self,
        ranges: impl IntoIterator<Item = (RequestId, ReadRange)>,
        callback: impl FnMut(RequestId, &[T]) -> Result<()>,
    ) -> Result<()> {
        self.inner.read_batch::<P, RequestId>(ranges, callback)
    }

    #[inline]
    fn read_iter<P: AccessPattern, RequestId>(
        &self,
        ranges: impl IntoIterator<Item = (RequestId, ReadRange)>,
    ) -> impl Iterator<Item = Result<(RequestId, Cow<'_, [T]>)>> {
        self.inner.read_iter::<P, RequestId>(ranges)
    }

    #[inline]
    fn len(&self) -> Result<u64> {
        self.inner.len()
    }

    #[inline]
    fn populate(&self) -> Result<()> {
        self.inner.populate()
    }

    #[inline]
    fn clear_ram_cache(&self) -> Result<()> {
        self.inner.clear_ram_cache()
    }

    #[inline]
    fn read_multi<'a, P: AccessPattern, RequestId: 'a>(
        reads: impl IntoIterator<Item = (RequestId, &'a Self, ReadRange)>,
        callback: impl FnMut(RequestId, &[T]) -> Result<()>,
    ) -> Result<()>
    where
        Self: 'a,
    {
        S::read_multi::<'a, P, RequestId>(
            reads
                .into_iter()
                .map(|(id, file, range)| (id, &file.inner, range)),
            callback,
        )
    }

    #[inline]
    fn read_multi_iter<'a, P: AccessPattern, RequestId>(
        reads: impl IntoIterator<Item = (RequestId, &'a Self, ReadRange)>,
    ) -> impl Iterator<Item = Result<(RequestId, Cow<'a, [T]>)>>
    where
        Self: 'a,
    {
        let reads = reads
            .into_iter()
            .map(|(id, file, range)| (id, &file.inner, range));
        S::read_multi_iter::<P, _>(reads)
    }
}

impl<S: UniversalWrite<T>, T: Copy + 'static> UniversalWrite<T> for TypedStorage<S, T> {
    #[inline]
    fn write(&mut self, byte_offset: ByteOffset, data: &[T]) -> Result<()> {
        self.inner.write(byte_offset, data)
    }

    #[inline]
    fn write_batch<'a>(
        &mut self,
        offset_data: impl IntoIterator<Item = (ByteOffset, &'a [T])>,
    ) -> Result<()> {
        self.inner.write_batch(offset_data)
    }

    #[inline]
    fn flusher(&self) -> Flusher {
        self.inner.flusher()
    }

    #[inline]
    fn write_multi<'a>(
        files: &mut [Self],
        writes: impl IntoIterator<Item = (FileIndex, ByteOffset, &'a [T])>,
    ) -> Result<()> {
        S::write_multi(Self::peel_slice_mut(files), writes)
    }
}
