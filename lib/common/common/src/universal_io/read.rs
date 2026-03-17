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

    fn read_batch<P: AccessPattern>(
        &self,
        ranges: impl IntoIterator<Item = ReadRange>,
        callback: impl FnMut(usize, &[T]) -> Result<()>,
    ) -> Result<()>;

    /// Like [`read_batch`](Self::read_batch), but returns a fallible iterator instead of
    /// accepting a callback.
    fn read_iter<P: AccessPattern>(
        &self,
        ranges: impl IntoIterator<Item = ReadRange>,
    ) -> impl Iterator<Item = Result<(usize, Cow<'_, [T]>)>> {
        ranges
            .into_iter()
            .enumerate()
            .map(move |(idx, range)| self.read::<P>(range).map(|data| (idx, data)))
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
    fn read_multi<P: AccessPattern>(
        files: &[Self],
        reads: impl IntoIterator<Item = (FileIndex, ReadRange)>,
        mut callback: impl FnMut(usize, FileIndex, &[T]) -> Result<()>,
    ) -> Result<()> {
        for (operation_index, (file_index, range)) in reads.into_iter().enumerate() {
            let file = files
                .get(file_index)
                .ok_or(UniversalIoError::InvalidFileIndex {
                    file_index,
                    files: files.len(),
                })?;

            let data = file.read::<P>(range)?;
            callback(operation_index, file_index, &data)?;
        }

        Ok(())
    }

    /// Like [`read_multi`](Self::read_multi), but returns a fallible iterator instead of
    /// accepting a callback.
    fn read_multi_iter<P: AccessPattern>(
        files: &[Self],
        reads: impl IntoIterator<Item = (FileIndex, ReadRange)>,
    ) -> impl Iterator<Item = Result<(usize, FileIndex, Cow<'_, [T]>)>> {
        reads
            .into_iter()
            .enumerate()
            .map(move |(idx, (file_index, range))| {
                let file = files
                    .get(file_index)
                    .ok_or(UniversalIoError::InvalidFileIndex {
                        file_index,
                        files: files.len(),
                    })?;

                let data = file.read::<P>(range)?;
                Ok((idx, file_index, data))
            })
    }

    /// Similar to [`UniversalRead::read_batch`], but automatically splits
    /// ranges into chunks. Calls callback per single item.
    fn read_batch_autochunks(
        &self,
        ranges: impl IntoIterator<Item = ReadRange>,
        mut callback: impl FnMut(T),
    ) -> Result<()> {
        self.read_batch::<Sequential>(
            ranges.into_iter().flat_map(|r| r.iter_autochunks::<T>().0),
            |_, chunk| {
                for &item in chunk {
                    callback(item);
                }
                Ok(())
            },
        )
    }

    /// Read the entire file and call callback for each item with its index.
    fn for_each(&self, mut callback: impl FnMut(u64, T)) -> Result<()> {
        let (iter, chunk_len) = ReadRange {
            byte_offset: 0,
            length: self.len()?,
        }
        .iter_autochunks::<T>();
        self.read_batch::<Sequential>(iter, |chunk_idx, chunk| {
            for (item_idx, &item) in chunk.iter().enumerate() {
                callback(chunk_idx as u64 * chunk_len + item_idx as u64, item);
            }
            Ok(())
        })
    }
    // When adding provided methods, don't forget to update impls in crate::universal_io::wrappers::*.
}
