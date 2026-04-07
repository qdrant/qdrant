use std::borrow::Cow;
use std::path::Path;

use super::*;
use crate::generic_consts::{AccessPattern, Sequential};
use crate::universal_io::file_ops::UniversalReadFileOps;

/// Interface for accessing files in a universal way, abstracting away possible
/// implementations, such as memory map, io_uring, DIRECTIO, S3, etc.
#[expect(clippy::len_without_is_empty)]
pub trait UniversalRead<T: Copy + 'static>: UniversalReadFileOps {
    fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self>;

    /// Prefer [`read_batch`] if you need high performance.
    fn read<P: AccessPattern>(&self, range: ReadRange) -> Result<Cow<'_, [T]>>;

    /// Read the entire file in one logical access.
    ///
    /// Implementations may override this to avoid the two accesses that would result from
    /// `len()` followed by `read(0..len())`. Default implementation does exactly that.
    fn read_whole(&self) -> Result<Cow<'_, [T]>> {
        let n = self.len()?;
        self.read::<Sequential>(ReadRange {
            byte_offset: 0,
            length: n,
        })
    }

    fn read_batch<'a, P: AccessPattern, Meta: 'a>(
        &'a self,
        ranges: impl IntoIterator<Item = (Meta, ReadRange)>,
        callback: impl FnMut(Meta, &[T]) -> Result<()>,
    ) -> Result<()>;

    /// Like [`read_batch`](Self::read_batch), but returns a fallible iterator instead of
    /// accepting a callback.
    fn read_iter<P: AccessPattern, Meta>(
        &self,
        ranges: impl IntoIterator<Item = (Meta, ReadRange)>,
    ) -> impl Iterator<Item = Result<(Meta, Cow<'_, [T]>)>> {
        ranges
            .into_iter()
            .map(move |(meta, range)| self.read::<P>(range).map(|data| (meta, data)))
    }

    fn len(&self) -> Result<u64>;

    /// Fill RAM cache with related data, if applicable for this implementation.
    ///
    /// For example in MMAP-based files we do `madvise` with `MADV_POPULATE_READ`.
    fn populate(&self) -> Result<()>;

    /// Ask to evict related data from RAM cache, if applicable for this implementation.
    ///
    /// For example in MMAP-based files we do `fadvise` with `POSIX_FADV_DONTNEED`.
    fn clear_ram_cache(&self) -> Result<()>;

    /// Read from multiple files in a single operation.
    fn read_multi<'a, P: AccessPattern, Meta: 'a>(
        reads: impl IntoIterator<Item = (Meta, &'a Self, ReadRange)>,
        mut callback: impl FnMut(Meta, &[T]) -> Result<()>,
    ) -> Result<()>
    where
        Self: 'a,
    {
        for (meta, file, range) in reads {
            let data = file.read::<P>(range)?;
            callback(meta, &data)?;
        }

        Ok(())
    }

    /// Like [`read_multi`](Self::read_multi), but returns a fallible iterator instead of
    /// accepting a callback.
    fn read_multi_iter<'a, P: AccessPattern, Meta>(
        reads: impl IntoIterator<Item = (Meta, &'a Self, ReadRange)>,
    ) -> impl Iterator<Item = Result<(Meta, Cow<'a, [T]>)>>
    where
        Self: 'a,
    {
        reads.into_iter().map(move |(meta, file, range)| {
            let data = file.read::<P>(range)?;
            Ok((meta, data))
        })
    }

    // When adding provided methods, don't forget to update impls in crate::universal_io::wrappers::*.
}
