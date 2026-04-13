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
            writeable,
            need_sequential,
            disk_parallel: _,
            populate,
            advice,
            prevent_caching: _, // Whole point of mmap is to cache
        } = options;

        let mmap = open_mmap(
            path.as_ref(),
            writeable,
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
        let mmap = self.as_bytes::<P>();
        let items = read(mmap, range)?;
        Ok(Cow::Borrowed(items))
    }

    fn read_batch<'a, P: AccessPattern, Meta: 'a>(
        &'a self,
        ranges: impl IntoIterator<Item = (Meta, ReadRange)>,
        mut callback: impl FnMut(Meta, &[T]) -> Result<()>,
    ) -> Result<()> {
        let mmap = self.as_bytes::<P>();

        for (meta, range) in ranges {
            let items = read(mmap, range)?;
            callback(meta, items)?;
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
        let Self {
            path: _,
            mmap,
            mmap_seq: _,
        } = self;
        mmap.clear_cache();
        Ok(())
    }
}

impl<T> UniversalWrite<T> for MmapFile
where
    T: bytemuck::Pod,
{
    fn write(&mut self, byte_offset: ByteOffset, items: &[T]) -> Result<()> {
        let mmap = self.as_bytes_mut();
        write(mmap, byte_offset, items)?;
        Ok(())
    }

    fn write_batch<'a>(
        &mut self,
        offset_data: impl IntoIterator<Item = (ByteOffset, &'a [T])>,
    ) -> Result<()> {
        let mmap = self.as_bytes_mut();

        for (byte_offset, items) in offset_data {
            write(mmap, byte_offset, items)?;
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
    /// Returns the path of the underlying file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns total file size on disk in bytes.
    pub fn disk_bytes(&self) -> std::io::Result<u64> {
        Ok(fs_err::metadata(&self.path)?.len())
    }

    /// Returns the number of bytes currently resident in RAM (page cache),
    /// measured via `mincore`. This is a point-in-time approximation.
    #[cfg(unix)]
    pub fn resident_bytes(&self) -> std::io::Result<u64> {
        let len = self.mmap.len();
        if len == 0 {
            return Ok(0);
        }

        let page_size = crate::mmap::advice::page_size()
            .ok_or_else(|| std::io::Error::other("failed to determine page size"))?;
        let num_pages = len.div_ceil(page_size);
        let mut vec = vec![0u8; num_pages];

        // SAFETY: `self.mmap.as_ptr()` is a valid page-aligned pointer for `len` bytes
        // (guaranteed by memmap2). `vec` is correctly sized for `num_pages` entries.
        let ret = unsafe {
            nix::libc::mincore(
                self.mmap.as_ptr() as *mut nix::libc::c_void,
                len,
                vec.as_mut_ptr().cast(),
            )
        };
        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }

        // `mincore` writes one byte per page. The least significant bit indicates
        // whether the page is currently resident in RAM (page cache).
        // See: https://man7.org/linux/man-pages/man2/mincore.2.html
        let resident_pages = vec.iter().filter(|&&b| b & 1 != 0).count();
        let resident_bytes = (resident_pages * page_size).min(len) as u64;
        Ok(resident_bytes)
    }

    /// Opens a file as a read-only `MmapFile` and returns its memory stats.
    ///
    /// This creates a temporary `MmapFile` to measure `mincore` residency,
    /// ensuring all measurements go through the same mmap path.
    #[cfg(unix)]
    pub fn probe_memory_stats(path: impl AsRef<Path>) -> std::io::Result<(u64, u64)> {
        let file: Self = <Self as UniversalRead<u8>>::open(
            path,
            OpenOptions {
                writeable: false,
                need_sequential: false,
                disk_parallel: None,
                populate: Some(false),
                advice: Some(AdviceSetting::Advice(Advice::Normal)),
                prevent_caching: None,
            },
        )
        .map_err(|e| std::io::Error::other(e.to_string()))?;
        let disk_bytes = file.disk_bytes()?;
        let resident_bytes = file.resident_bytes()?;
        Ok((disk_bytes, resident_bytes))
    }

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
}

#[inline]
fn read<T>(bytes: &[u8], range: ReadRange) -> Result<&[T]>
where
    T: bytemuck::Pod,
{
    let ReadRange {
        byte_offset,
        length: items,
    } = range;

    let start = byte_offset as usize;
    let end = start + size_of::<T>() * items as usize;

    let bytes = bytes
        .get(start..end)
        .ok_or_else(|| UniversalIoError::OutOfBounds {
            start: start as _,
            end: end as _,
            elements: bytes.len() / size_of::<T>(),
        })?;

    // `bytemuck::cast_slice` checks that `bytes` size and alignment match `T` requirements
    let items = bytemuck::cast_slice(bytes);
    Ok(items)
}

#[inline]
fn write<T>(mmap: &mut [u8], byte_offset: ByteOffset, items: &[T]) -> Result<()>
where
    T: bytemuck::Pod,
{
    let start = byte_offset as usize;
    let end = start + size_of_val(items);

    let mmap_len_bytes = mmap.len();

    let mmap = mmap
        .get_mut(start..end)
        .ok_or_else(|| UniversalIoError::OutOfBounds {
            start: start as _,
            end: end as _,
            elements: mmap_len_bytes / size_of::<T>(),
        })?;

    let bytes = bytemuck::cast_slice(items);
    mmap.copy_from_slice(bytes);

    Ok(())
}
