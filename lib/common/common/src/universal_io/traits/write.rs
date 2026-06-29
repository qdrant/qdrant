use super::UniversalRead;
use crate::universal_io::{ByteOffset, FileIndex, Flusher, Result, UniversalIoError};

pub trait UniversalWrite: UniversalRead {
    fn write<T: bytemuck::Pod>(&mut self, byte_offset: ByteOffset, data: &[T]) -> Result<()>;

    /// Write `data` at `byte_offset`, extending the file if the write reaches
    /// past its current end.
    ///
    /// Unlike [`Self::write`] — which requires the file to already be large
    /// enough — growing the file and writing the data happen as a single append
    /// operation. This is what append-only backends (e.g. object stores) need:
    /// they cannot resize a file separately from writing to it. Any gap between
    /// the old end of the file and `byte_offset` reads back as zeros.
    fn write_grow<T: bytemuck::Pod>(&mut self, byte_offset: ByteOffset, data: &[T]) -> Result<()>;

    fn write_batch<'a, T: bytemuck::Pod>(
        &mut self,
        offset_data: impl IntoIterator<Item = (ByteOffset, &'a [T])>,
    ) -> Result<()>;

    fn flusher(&self) -> Flusher;

    /// Write to multiple files in a single operation.
    fn write_multi<'a, T: bytemuck::Pod>(
        files: &mut [Self],
        writes: impl IntoIterator<Item = (FileIndex, ByteOffset, &'a [T])>,
    ) -> Result<()> {
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

    /// Like [`Self::write_multi`], but each write may extend its target file as
    /// a single append operation (see [`Self::write_grow`]).
    fn write_multi_grow<'a, T: bytemuck::Pod>(
        files: &mut [Self],
        writes: impl IntoIterator<Item = (FileIndex, ByteOffset, &'a [T])>,
    ) -> Result<()> {
        let files_len = files.len();

        for (file_index, offset, data) in writes {
            let file = files
                .get_mut(file_index)
                .ok_or(UniversalIoError::InvalidFileIndex {
                    file_index,
                    files: files_len,
                })?;

            file.write_grow(offset, data)?;
        }

        Ok(())
    }

    // When adding provided methods, don't forget to update impls in crate::universal_io::wrappers::*.
}
