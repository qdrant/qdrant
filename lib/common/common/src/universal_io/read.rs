use std::borrow::Cow;
use std::ops::Range;
use std::path::Path;

use super::*;
use crate::aligned_buf::AlignedCow;
use crate::generic_consts::{AccessPattern, Sequential};
use crate::universal_io::file_ops::UniversalReadFileOps;

/// Interface for accessing files in a universal way, abstracting away possible
/// implementations, such as memory map, io_uring, DIRECTIO, S3, etc.
#[expect(clippy::len_without_is_empty)]
pub trait UniversalRead: UniversalReadFileOps {
    type ReadPipeline<'file, U>: UniversalReadPipeline<'file, U, File = Self>
    where
        Self: 'file,
        U: UserData;

    fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self>;

    /// Prefer [`read_batch`] if you need high performance.
    fn read<P: AccessPattern, T: bytemuck::Pod>(&self, range: ReadRange) -> Result<Cow<'_, [T]>> {
        self.read_bytes::<P>(range.as_byte_range::<T>(), align_of::<T>())?
            .try_cast_bytemuck()
            .map_err(UniversalIoError::AlignedBufCast)
    }

    fn read_bytes<P: AccessPattern>(
        &self,
        range: Range<u64>,
        align: usize,
    ) -> Result<AlignedCow<'_>>;

    /// Read the entire file in one logical access.
    ///
    /// Implementations may override this to avoid the two accesses that would result from
    /// `len()` followed by `read(0..len())`. Default implementation does exactly that.
    fn read_whole<T: bytemuck::Pod>(&self) -> Result<Cow<'_, [T]>> {
        let range = ReadRange {
            byte_offset: 0,
            length: self.len::<T>()?,
        };

        self.read::<Sequential, T>(range)
    }

    fn read_batch<P, T, U>(
        &self,
        ranges: impl IntoIterator<Item = (U, ReadRange)>,
        mut callback: impl FnMut(U, &[T]) -> Result<()>,
    ) -> Result<()>
    where
        P: AccessPattern,
        T: bytemuck::Pod,
        U: UserData,
    {
        for record in self.read_iter::<P, T, U>(ranges)? {
            let (user_data, data) = record?;
            callback(user_data, &data)?;
        }

        Ok(())
    }

    /// Like [`read_batch`](Self::read_batch), but returns a fallible iterator instead of
    /// accepting a callback.
    fn read_iter<P, T, U>(
        &self,
        ranges: impl IntoIterator<Item = (U, ReadRange)>,
    ) -> Result<impl Iterator<Item = Result<(U, Cow<'_, [T]>)>>>
    where
        P: AccessPattern,
        T: bytemuck::Pod,
        U: UserData,
    {
        let reads = ranges
            .into_iter()
            .map(move |(user_data, range)| (user_data, self, range));

        Self::read_multi_iter::<P, T, U>(reads)
    }

    fn len<T>(&self) -> Result<u64>;

    /// Fill RAM cache with related data, if applicable for this implementation.
    ///
    /// For example in MMAP-based files we do `madvise` with `MADV_POPULATE_READ`.
    fn populate(&self) -> Result<()>;

    /// Ask to evict related data from RAM cache, if applicable for this implementation.
    ///
    /// For example in MMAP-based files we do `madvise` with `MADV_PAGEOUT`.
    fn clear_ram_cache(&self) -> Result<()>;

    /// Read from multiple files in a single operation.
    fn read_multi<'a, P, T, U>(
        reads: impl IntoIterator<Item = (U, &'a Self, ReadRange)>,
        mut callback: impl FnMut(U, &[T]) -> Result<()>,
    ) -> Result<()>
    where
        P: AccessPattern,
        T: bytemuck::Pod,
        U: UserData,
        Self: 'a,
    {
        for record in Self::read_multi_iter::<P, T, U>(reads)? {
            let (user_data, items) = record?;
            callback(user_data, &items)?;
        }

        Ok(())
    }

    /// Like [`read_multi`](Self::read_multi), but returns a fallible iterator instead of
    /// accepting a callback.
    fn read_multi_iter<'a, P, T, U>(
        reads: impl IntoIterator<Item = (U, &'a Self, ReadRange)>,
    ) -> Result<impl Iterator<Item = Result<(U, Cow<'a, [T]>)>>>
    where
        P: AccessPattern,
        T: bytemuck::Pod,
        U: UserData,
        Self: 'a,
    {
        let mut pipeline = Self::ReadPipeline::<'a, U>::new()?;
        let mut reads = reads.into_iter();

        let iter = std::iter::from_fn(move || {
            while pipeline.can_schedule()
                && let Some(read) = reads.next()
            {
                let (user_data, file, range) = read;
                let range = range.as_byte_range::<T>();
                if let Err(err) = pipeline.schedule::<P>(user_data, file, range, align_of::<T>()) {
                    return Some(Err(err));
                }
            }

            pipeline.wait_bytemuck().transpose()
        });

        Ok(iter)
    }

    fn kind() -> UniversalKind;

    // When adding provided methods, don't forget to update impls in crate::universal_io::wrappers::*.
}

pub trait UniversalReadPipeline<'file, U>: Sized
where
    U: UserData,
{
    type File: 'file;

    fn new() -> Result<Self>;

    fn can_schedule(&mut self) -> bool;

    /// Schedule read operation.
    ///
    /// Note: an implementation might add it to internal queue, but not actually
    /// execute it until [`UniversalReadPipeline::wait()`] is called.
    ///
    /// Should be called only when [`UniversalReadPipeline::can_schedule()`] is
    /// `true`. Returns [`UniversalIoError::QueueIsFull`] otherwise.
    fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        file: &'file Self::File,
        range: Range<u64>,
        align: usize,
    ) -> Result<()>;

    /// Block until any of the scheduled operations is completed and consume its
    /// result.
    fn wait(&mut self) -> Result<Option<(U, AlignedCow<'file>)>>;

    fn wait_bytemuck<T: bytemuck::Pod>(&mut self) -> Result<Option<(U, Cow<'file, [T]>)>> {
        let Some((user_data, bytes)) = self.wait()? else {
            return Ok(None);
        };
        let cow = bytes
            .try_cast_bytemuck()
            .map_err(UniversalIoError::AlignedBufCast)?;
        Ok(Some((user_data, cow)))
    }
}

/// An arbitrary value to distinguish requests.
///
/// Batched universal I/O methods let callers to add an arbitrary user-provided
/// value to each request. This value will be passed back to the caller/callback
/// when the request completes.
///
/// Similar to `user_data` in `io_uring`, but allows arbitrary type, not just
/// `u64`.
///
/// This trait exists for documentation/code navigation purposes only.
pub trait UserData {}
impl<T> UserData for T {}
