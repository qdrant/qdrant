use super::*;

pub type FileIndex = usize;

pub trait UniversalReadMulti<T: Copy + 'static>: UniversalRead<T> + Sized {
    fn read_multi<const SEQUENTIAL: bool>(
        files: &[Self],
        reads: impl IntoIterator<Item = (FileIndex, ElementsRange)>,
        mut callback: impl FnMut(usize, FileIndex, &[T]) -> Result<()>,
    ) -> Result<()> {
        for (operation_index, (file_index, range)) in reads.into_iter().enumerate() {
            let file = files
                .get(file_index)
                .ok_or(UniversalIoError::InvalidFileIndex {
                    file_index,
                    num_files: files.len(),
                })?;

            let data = file.read::<SEQUENTIAL>(range)?;
            callback(operation_index, file_index, &data)?;
        }

        Ok(())
    }
}

pub trait UniversalWriteMulti<T: Copy + 'static>: UniversalWrite<T> + Sized {
    fn write_multi<'a>(
        files: &mut [Self],
        writes: impl IntoIterator<Item = (FileIndex, ElementOffset, &'a [T])>,
    ) -> Result<()> {
        let num_files = files.len();

        for (file_index, offset, data) in writes {
            let file = files
                .get_mut(file_index)
                .ok_or(UniversalIoError::InvalidFileIndex {
                    file_index,
                    num_files,
                })?;

            file.write(offset, data)?;
        }

        Ok(())
    }
}
