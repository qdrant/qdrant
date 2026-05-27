use std::borrow::Cow;
use std::fmt::Debug;
use std::ops::Range;

use super::{BorrowedReadPipeline, Item, OwnedReadPipeline, UniversalReadFs, UserData};
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::{AccessPattern, Sequential};
use crate::universal_io::{ReadRange, Result, UniversalKind};

/// Per-file handle for universal read access.
///
/// Concrete file handles (`MmapFile`, `IoUringFile`, `CachedSlice`, ...)
/// implement this trait. Instances are produced by a corresponding
/// [`UniversalReadFs`](super::UniversalReadFs) backend via
/// [`UniversalReadFs::open`](super::UniversalReadFs::open).
///
/// This trait deliberately does *not* extend
/// [`UniversalReadFileOps`](super::UniversalReadFileOps): a file handle is
/// not a filesystem, and not every filesystem-level backend produces
/// `UniversalRead` handles (e.g. a metadata-only listing service). The
/// link to the producing filesystem is exposed via the [`Self::Fs`]
/// associated type so generic-over-`<S: UniversalRead>` code can refer
/// to the matching filesystem handle as `S::Fs` without an extra generic
/// parameter.
///
/// # Alignment
///
/// Some methods accept an `align` parameter.
/// - When returning [`ACow::Owned`], an implementation should honor that
///   alignment, meaning that the result can be casted to [`Vec<T>`].
/// - When returning [`ACow::Borrowed`], an implementation will ignore the
///   alignment. Practically mmaps are aligned by 4 KiB, which is more than
///   enough for the majority of types.
#[expect(clippy::len_without_is_empty)]
pub trait UniversalRead: Sized + Debug + Send + Sync {
    /// Filesystem handle type that opens `Self`-typed file handles via
    /// [`UniversalReadFs::open`](UniversalReadFs::open).
    ///
    /// Bidirectionally pinned: `Self::Fs::File = Self`. Wrappers such as
    /// `ReadOnly<S>` declare a phantom `ReadOnlyFs<S::Fs>` to satisfy this
    /// constraint at the type level, while their inherent `open`
    /// constructor still accepts the unwrapped inner `S::Fs`.
    type Fs: UniversalReadFs<File = Self>;

    type BorrowedReadPipeline<'file, U>: BorrowedReadPipeline<'file, U, File = Self>
    where
        Self: 'file,
        U: UserData;

    type OwnedReadPipeline<U>: OwnedReadPipeline<U, File = Self>
    where
        U: UserData;

    /// Enables live-reloading of files. Append-only files can make the
    /// underlying file larger, so reopening can account for this growth.
    ///
    /// This may be a no-op in some implementations.
    fn reopen(&mut self) -> Result<()>;

    /// Prefer [`read_batch`] if you need high performance.
    #[inline]
    fn read<P: AccessPattern, T: Item>(&self, range: ReadRange) -> Result<Cow<'_, [T]>> {
        let bytes = self.read_bytes::<P>(range.into_byte_range::<T>(), align_of::<T>())?;
        Ok(bytes.try_cast_bytemuck().unwrap())
    }

    fn read_bytes<P: AccessPattern>(&self, range: Range<u64>, align: usize) -> Result<ACow<'_>>;

    /// Read the entire file in one logical access.
    ///
    /// Implementations may override this to avoid the two accesses that would
    /// result from `len()` followed by `read(0..len())`. Default implementation
    /// does exactly that.
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

    /// Like [`read_batch`](Self::read_batch), but returns a fallible iterator
    /// instead of accepting a callback.
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

    /// Like [`read_multi`](Self::read_multi), but returns a fallible iterator
    /// instead of accepting a callback.
    fn read_multi_iter<'a, P, T, U>(
        reads: impl IntoIterator<Item = (U, &'a Self, ReadRange)>,
    ) -> Result<impl Iterator<Item = Result<(U, Cow<'a, [T]>)>>>
    where
        P: AccessPattern,
        T: Item,
        U: UserData,
        Self: 'a,
    {
        let mut pipeline = Self::BorrowedReadPipeline::<'a, U>::new()?;
        let mut reads = reads.into_iter();

        let iter = std::iter::from_fn(move || {
            while pipeline.can_schedule()
                && let Some(read) = reads.next()
            {
                let (user_data, file, range) = read;
                let range = range.into_byte_range::<T>();
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
