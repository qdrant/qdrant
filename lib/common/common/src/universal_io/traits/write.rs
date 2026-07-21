use super::{UniversalRead, UniversalWriteFileOps};
use crate::universal_io::{ByteOffset, FileIndex, Flusher, UioResult, UniversalIoError};

/// Durability control for mutating file handles.
///
/// Extracted from [`UniversalWrite`] so capabilities that mutate a file
/// without random-offset writes (e.g. [`UniversalAppend`]) can require it
/// without duplicating `flusher` — a duplicate would make plain
/// `handle.flusher()` calls ambiguous on handles implementing both.
///
/// [`UniversalAppend`]: super::UniversalAppend
pub trait UniversalFlush {
    /// Flusher to persist all mutations performed so far.
    fn flusher(&self) -> Flusher;
}

/// A writeable file handle.
///
/// Requires the backing filesystem to support mutating file operations
/// ([`UniversalWriteFileOps`]): a backend that can open files for writing
/// must also be able to create and remove them.
pub trait UniversalWrite: UniversalRead<Fs: UniversalWriteFileOps> + UniversalFlush {
    fn write<T: bytemuck::Pod>(&mut self, byte_offset: ByteOffset, data: &[T]) -> UioResult<()>;

    fn write_batch<'a, T: bytemuck::Pod>(
        &mut self,
        offset_data: impl IntoIterator<Item = (ByteOffset, &'a [T])>,
    ) -> UioResult<()>;

    /// Write to multiple files in a single operation.
    fn write_multi<'a, T: bytemuck::Pod>(
        files: &mut [Self],
        writes: impl IntoIterator<Item = (FileIndex, ByteOffset, &'a [T])>,
    ) -> UioResult<()> {
        let files_len = files.len();

        for (file_index, offset, data) in writes {
            let file = files
                .get_mut(file_index)
                .ok_or(UniversalIoError::InvalidFileIndex {
                    file_index,
                    files: files_len,
                })?;

            file.write(offset, data)?;
        }

        Ok(())
    }

    // When adding provided methods, don't forget to update impls in crate::universal_io::wrappers::*.
}
