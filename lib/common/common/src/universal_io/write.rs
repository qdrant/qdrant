use super::read::UniversalRead;
use super::*;

pub trait UniversalWrite<T: Copy + 'static>: UniversalRead<T> {
    fn write(&mut self, byte_offset: ByteOffset, data: &[T]) -> Result<()>;

    fn write_batch<'a>(
        &mut self,
        offset_data: impl IntoIterator<Item = (ByteOffset, &'a [T])>,
    ) -> Result<()>;

    fn flusher(&self) -> Flusher;

    /// Write to multiple files in a single operation.
    fn write_multi<'a>(
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

    // When adding provided methods, don't forget to update impls in crate::universal_io::wrappers::*.
}
