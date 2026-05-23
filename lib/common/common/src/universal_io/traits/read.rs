use std::borrow::Cow;
use std::path::Path;

use super::{BorrowedReadPipeline, Item, OwnedReadPipeline, UniversalReadFileOps, UserData};
use crate::generic_consts::{AccessPattern, Sequential};
use crate::universal_io::{OpenOptions, ReadRange, Result, UniversalKind};

/// Interface for accessing files in a universal way, abstracting away possible
/// implementations, such as memory map, io_uring, DIRECTIO, S3, etc.
#[expect(clippy::len_without_is_empty)]
pub trait UniversalRead: UniversalReadFileOps {
    type BorrowedReadPipeline<'file, T, U>: BorrowedReadPipeline<'file, T, U, File = Self>
    where
        Self: 'file,
        T: Item,
        U: UserData;

    type OwnedReadPipeline<T, U>: OwnedReadPipeline<T, U, File = Self>
    where
        T: Item,
        U: UserData;

    fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self>;

    /// Enables live-reloading of files. Append-only files can make the underlying file larger,
    /// so reopening can account for this growth.
    ///
    /// This may be a no-op in some implementations.
    fn reopen(&mut self) -> Result<()>;

    /// Prefer [`read_batch`] if you need high performance.
    fn read<P: AccessPattern, T: Item>(&self, range: ReadRange) -> Result<Cow<'_, [T]>>;

    /// Read the entire file in one logical access.
    ///
    /// Implementations may override this to avoid the two accesses that would result from
    /// `len()` followed by `read(0..len())`. Default implementation does exactly that.
    fn read_whole<T: Item>(&self) -> Result<Cow<'_, [T]>> {
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
        T: Item,
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
        T: Item,
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
        T: Item,
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
        T: Item,
        U: UserData,
        Self: 'a,
    {
        let mut pipeline = Self::BorrowedReadPipeline::<'a, T, U>::new()?;
        let mut reads = reads.into_iter();

        let iter = std::iter::from_fn(move || {
            while pipeline.can_schedule()
                && let Some(read) = reads.next()
            {
                let (user_data, file, range) = read;

                if let Err(err) = pipeline.schedule::<P>(user_data, file, range) {
                    return Some(Err(err));
                }
            }

            pipeline.wait().transpose()
        });

        Ok(iter)
    }

    fn kind() -> UniversalKind;

    // When adding provided methods, don't forget to update impls in crate::universal_io::wrappers::*.
}
