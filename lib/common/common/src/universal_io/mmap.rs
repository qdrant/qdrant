use std::borrow::Cow;
use std::path::{Path, PathBuf};

use crate::mmap::{
    Advice, AdviceSetting, MULTI_MMAP_IS_SUPPORTED, MmapSlice, MmapSliceReadOnly, open_read_mmap,
    open_write_mmap,
};
use crate::universal_io::{
    ByteOffset, BytesRange, Flusher, OpenOptions, UniversalIoError, UniversalRead, UniversalWrite,
};

#[derive(Debug)]
pub struct MmapUniversal<T: Copy + 'static> {
    path: PathBuf,
    /// Main data mmap slice for read/write
    ///
    /// Best suited for random reads.
    mmap: MmapSlice<T>,
    /// Read-only mmap slice best suited for sequential reads
    ///
    /// `None` on platforms that do not support multiple memory maps to the same file.
    mmap_seq: Option<MmapSliceReadOnly<T>>,
}

impl<T> UniversalRead<T> for MmapUniversal<T>
where
    T: Copy + 'static,
{
    fn open(path: impl AsRef<Path>, options: OpenOptions) -> crate::universal_io::Result<Self>
    where
        Self: Sized,
    {
        let OpenOptions {
            need_sequential,
            disk_parallel: _,
            populate,
        } = options;

        let mmap_file = path.as_ref();
        let advice = AdviceSetting::Global;

        let mmap = open_write_mmap(mmap_file, advice, populate.unwrap_or_default())?;
        let mmap = unsafe { MmapSlice::try_from(mmap) }?;

        let mmap_seq = if *MULTI_MMAP_IS_SUPPORTED && need_sequential {
            let mmap_seq =
                open_read_mmap(mmap_file, AdviceSetting::Advice(Advice::Sequential), false)?;
            Some(unsafe { MmapSliceReadOnly::try_from(mmap_seq) }?)
        } else {
            None
        };

        Ok(MmapUniversal {
            path: mmap_file.to_path_buf(),
            mmap,
            mmap_seq,
        })
    }

    fn read<const SEQUENTIAL: bool>(
        &self,
        range: BytesRange,
    ) -> crate::universal_io::Result<Cow<'_, [T]>> {
        let data_slice = self.as_slice::<SEQUENTIAL>();
        let start = range.start as usize;
        let end = start + range.length as usize;

        let data_range = data_slice
            .get(start..end)
            .ok_or(UniversalIoError::OutOfBounds {
                start: start as u64,
                end: end as u64,
                data_length: data_slice.len(),
            })?;

        Ok(Cow::Borrowed(data_range))
    }

    fn read_batch<const SEQUENTIAL: bool>(
        &self,
        ranges: impl IntoIterator<Item = BytesRange>,
        mut callback: impl FnMut(usize, &[T]) -> crate::universal_io::Result<()>,
    ) -> crate::universal_io::Result<()> {
        let data_slice = self.as_slice::<SEQUENTIAL>();
        let data_length = data_slice.len();

        for (idx, range) in ranges.into_iter().enumerate() {
            let start = range.start as usize;
            let end = start + range.length as usize;

            let data_range = data_slice
                .get(start..end)
                .ok_or(UniversalIoError::OutOfBounds {
                    start: start as u64,
                    end: end as u64,
                    data_length,
                })?;

            callback(idx, data_range)?;
        }

        Ok(())
    }

    fn populate(&self) -> crate::universal_io::Result<()> {
        if let Some(mmap_seq) = &self.mmap_seq {
            mmap_seq.populate()?;
        } else {
            // If we don't have a separate sequential mmap,
            // populating regular mmap is a fallback
            self.mmap.populate()?;
        }
        Ok(())
    }

    fn clear_ram_cache(&self) -> crate::universal_io::Result<()> {
        crate::fs::clear_disk_cache(&self.path)?;
        Ok(())
    }
}

impl<T> UniversalWrite<T> for MmapUniversal<T>
where
    T: Copy + 'static,
{
    fn write(&mut self, offset: ByteOffset, data: &[T]) -> crate::universal_io::Result<()> {
        let mmap_slice: &mut [T] = &mut self.mmap;
        let data_length = mmap_slice.len();
        let start = offset as usize;
        let end = start + data.len();

        let target = mmap_slice
            .get_mut(start..end)
            .ok_or(UniversalIoError::OutOfBounds {
                start: offset,
                end: offset + data.len() as u64,
                data_length,
            })?;

        target.copy_from_slice(data);
        Ok(())
    }

    fn write_batch<'a>(
        &mut self,
        offset_data: impl IntoIterator<Item = (ByteOffset, &'a [T])>,
    ) -> crate::universal_io::Result<()> {
        for (offset, data) in offset_data {
            self.write(offset, data)?;
        }
        Ok(())
    }

    fn flusher(&self) -> Flusher {
        let inner = self.mmap.flusher();
        Box::new(move || Ok(inner()?)) // Converts error type to UniversalIoError
    }
}

impl<T: Copy + 'static> MmapUniversal<T> {
    fn as_seq_slice(&self) -> &[T] {
        self.mmap_seq
            .as_ref()
            .map(|m| m.as_ref())
            .unwrap_or(self.mmap.as_ref())
    }

    fn as_slice<const SEQUENTIAL: bool>(&self) -> &[T] {
        if SEQUENTIAL {
            self.as_seq_slice()
        } else {
            self.mmap.as_ref()
        }
    }
}
