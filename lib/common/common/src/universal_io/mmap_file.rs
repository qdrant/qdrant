use std::borrow::Cow;
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs, slice};

use memmap2::MmapRaw;

use super::*;
use crate::generic_consts::AccessPattern;
use crate::mmap::{MULTI_MMAP_IS_SUPPORTED, Madviseable as _};

#[derive(Debug)]
pub struct MmapFile {
    path: PathBuf,
    mmap: Arc<MmapRaw>,
    mmap_seq: Option<MmapRaw>,
}

impl UniversalReadFileOps for MmapFile {
    fn list_files(prefix_path: &Path) -> Result<Vec<PathBuf>> {
        local_file_ops::local_list_files(prefix_path)
    }

    fn exists(path: &Path) -> crate::universal_io::Result<bool> {
        fs_err::exists(path).map_err(UniversalIoError::from)
    }
}

impl<T> UniversalRead<T> for MmapFile
where
    T: bytemuck::Pod,
{
    fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let OpenOptions {
            need_sequential,
            disk_parallel: _,
            populate,
            advice,
        } = options;

        let mmap = open_mmap(
            path.as_ref(),
            true,
            populate.unwrap_or_default(),
            advice.unwrap_or(AdviceSetting::Global),
        )?;

        let mmap_seq = if need_sequential && *MULTI_MMAP_IS_SUPPORTED {
            let mmap_seq = open_mmap(
                path.as_ref(),
                false,
                false,
                AdviceSetting::Advice(Advice::Sequential),
            )?;

            Some(mmap_seq)
        } else {
            None
        };

        let mmap = Self {
            path: path.as_ref().into(),
            mmap: Arc::new(mmap),
            mmap_seq,
        };

        Ok(mmap)
    }

    fn read<P: AccessPattern>(&self, range: ReadRange) -> Result<Cow<'_, [T]>> {
        let items = self.read_impl::<_, P>(range)?;
        Ok(Cow::Borrowed(items))
    }

    fn read_batch<P: AccessPattern>(
        &self,
        ranges: impl IntoIterator<Item = ReadRange>,
        mut callback: impl FnMut(usize, &[T]) -> Result<()>,
    ) -> Result<()> {
        for (idx, range) in ranges.into_iter().enumerate() {
            let items = self.read_impl::<_, P>(range)?;
            callback(idx, items)?;
        }

        Ok(())
    }

    fn len(&self) -> Result<u64> {
        let len = self.mmap.len() / size_of::<T>();
        Ok(len as u64)
    }

    fn populate(&self) -> Result<()> {
        self.mmap.populate();
        Ok(())
    }

    fn clear_ram_cache(&self) -> Result<()> {
        crate::fs::clear_disk_cache(&self.path)?;
        Ok(())
    }
}

impl<T> UniversalWrite<T> for MmapFile
where
    T: bytemuck::Pod,
{
    fn write(&mut self, byte_offset: ByteOffset, items: &[T]) -> Result<()> {
        self.write_impl(byte_offset, items)?;
        Ok(())
    }

    fn write_batch<'a>(
        &mut self,
        offset_data: impl IntoIterator<Item = (ByteOffset, &'a [T])>,
    ) -> Result<()> {
        for (byte_offset, items) in offset_data {
            self.write_impl(byte_offset, items)?;
        }

        Ok(())
    }

    fn flusher(&self) -> Flusher {
        let mmap = self.mmap.clone();
        let flusher = move || {
            // flushing empty mmap returns error on some platforms
            if mmap.len() > 0 {
                mmap.flush()?;
            }

            Ok(())
        };

        Box::new(flusher)
    }
}

fn open_mmap(path: &Path, write: bool, populate: bool, advice: AdviceSetting) -> Result<MmapRaw> {
    // TODO: `fs_err` can cause panic when run on a single-threaded Tokio runtime
    #[expect(clippy::disallowed_types)]
    let file = fs::OpenOptions::new()
        .read(true)
        .write(write)
        .open(path)
        .map_err(|err| UniversalIoError::extract_not_found(err, path))?;

    let mmap = if write {
        memmap2::MmapOptions::new().map_raw(&file)?
    } else {
        memmap2::MmapOptions::new().map_raw_read_only(&file)?
    };

    if populate {
        mmap.populate();
    }

    mmap.madvise(advice.resolve())?;

    Ok(mmap)
}

impl MmapFile {
    fn as_bytes<P: AccessPattern>(&self) -> &[u8] {
        let mmap = if P::IS_SEQUENTIAL {
            self.mmap_seq.as_ref().unwrap_or(&self.mmap)
        } else {
            &self.mmap
        };

        unsafe { slice::from_raw_parts(mmap.as_ptr(), mmap.len()) }
    }

    fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.mmap.as_mut_ptr(), self.mmap.len()) }
    }

    fn read_impl<T, P>(&self, range: ReadRange) -> Result<&[T]>
    where
        T: bytemuck::Pod,
        P: AccessPattern,
    {
        let ReadRange {
            byte_offset,
            length: items,
        } = range;

        let start = byte_offset as usize;
        let end = start + size_of::<T>() * items as usize;

        let bytes =
            self.as_bytes::<P>()
                .get(start..end)
                .ok_or_else(|| UniversalIoError::OutOfBounds {
                    start: start as _,
                    end: end as _,
                    elements: self.mmap.len() / size_of::<T>(),
                })?;

        // `bytemuck::cast_slice` checks that `bytes` size and alignment match `T` requirements
        let items = bytemuck::cast_slice(bytes);
        Ok(items)
    }

    fn write_impl<T>(&mut self, byte_offset: ByteOffset, items: &[T]) -> Result<()>
    where
        T: bytemuck::Pod,
    {
        let start = byte_offset as usize;
        let end = start + size_of_val(items);

        let mmap_len_bytes = self.mmap.len();

        let mmap = self.as_bytes_mut().get_mut(start..end).ok_or_else(|| {
            UniversalIoError::OutOfBounds {
                start: start as _,
                end: end as _,
                elements: mmap_len_bytes / size_of::<T>(),
            }
        })?;

        let bytes = bytemuck::cast_slice(items);
        mmap.copy_from_slice(bytes);

        Ok(())
    }
}
