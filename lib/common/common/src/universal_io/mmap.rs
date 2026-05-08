use std::borrow::Cow;
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs, slice};

use memmap2::MmapRaw;

use super::*;
use crate::generic_consts::AccessPattern;
use crate::mmap::{MULTI_MMAP_IS_SUPPORTED, Madviseable as _};
use crate::universal_io::read::UniversalReadPipeline;

#[derive(Debug)]
pub struct MmapFile {
    path: PathBuf,

    // `mmap` and `mmap_seq` own the mmaps.
    mmap: Arc<MmapRaw>,
    mmap_seq: Option<MmapRaw>,

    // `len`, `ptr`, `ptr_seq` contain the same values as `mmap`, `mmap_seq`,
    // but duplicated here to avoid `Arc`/`Option` overhead in hot loops.
    /// Length of [`Self::mmap`].
    len: usize,
    /// Points to [`Self::mmap`].
    ptr: SendSyncPtr,
    /// Points to [`Self::mmap_seq`] (if present) or fallbacks to [`Self::mmap`].
    ptr_seq: SendSyncPtr,
}

#[derive(Debug, Clone, Copy)]
struct SendSyncPtr(*mut u8);
unsafe impl Send for SendSyncPtr {}
unsafe impl Sync for SendSyncPtr {}

impl UniversalReadFileOps for MmapFile {
    fn list_files(prefix_path: &Path) -> Result<Vec<PathBuf>> {
        local_file_ops::local_list_files(prefix_path)
    }

    fn exists(path: &Path) -> crate::universal_io::Result<bool> {
        fs_err::exists(path).map_err(UniversalIoError::from)
    }
}

impl UniversalRead for MmapFile {
    type ReadPipeline<'a, T, Meta>
        = MmapReadPipeline<'a, T, Meta>
    where
        T: bytemuck::Pod;

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
        let ptr = SendSyncPtr(mmap.as_mut_ptr());

        let mmap_seq;
        let len;
        let ptr_seq;

        if need_sequential && *MULTI_MMAP_IS_SUPPORTED {
            let mmap_seq_ = open_mmap(
                path.as_ref(),
                false,
                false,
                AdviceSetting::Advice(Advice::Sequential),
            )?;

            len = std::cmp::min(mmap.len(), mmap_seq_.len());
            ptr_seq = SendSyncPtr(mmap_seq_.as_mut_ptr());
            mmap_seq = Some(mmap_seq_);
        } else {
            len = mmap.len();
            ptr_seq = ptr;
            mmap_seq = None;
        };

        let mmap = Self {
            path: path.as_ref().into(),
            mmap: Arc::new(mmap),
            mmap_seq,
            len,
            ptr,
            ptr_seq,
        };

        Ok(mmap)
    }

    fn read<P: AccessPattern, T: bytemuck::Pod>(&self, range: ReadRange) -> Result<Cow<'_, [T]>> {
        let mmap = self.as_bytes::<P>();
        let items = read(mmap, range)?;
        Ok(Cow::Borrowed(items))
    }

    fn len<T>(&self) -> Result<u64> {
        let len = self.len / size_of::<T>();
        Ok(len as u64)
    }

    fn populate(&self) -> Result<()> {
        self.mmap.populate();
        Ok(())
    }

    fn clear_ram_cache(&self) -> Result<()> {
        self.mmap.clear_cache();
        if let Some(mmap_seq) = &self.mmap_seq {
            mmap_seq.clear_cache();
        }
        Ok(())
    }

    fn kind() -> UniversalKind {
        UniversalKind::Mmap
    }
}

pub struct MmapReadPipeline<'file, T, Meta> {
    result: Option<(Meta, &'file [T])>,
}

impl<'file, T, Meta> UniversalReadPipeline<'file, T, Meta> for MmapReadPipeline<'file, T, Meta>
where
    T: bytemuck::Pod,
{
    type File = MmapFile;

    fn new() -> Result<Self> {
        Ok(Self { result: None })
    }

    fn can_schedule(&mut self) -> bool {
        self.result.is_none()
    }

    fn schedule<P>(&mut self, meta: Meta, file: &'file MmapFile, range: ReadRange) -> Result<()>
    where
        P: AccessPattern,
    {
        if self.result.is_some() {
            return Err(UniversalIoError::QueueIsFull);
        }

        self.result = Some((meta, read(file.as_bytes::<P>(), range)?));
        Ok(())
    }

    fn wait(&mut self) -> Result<Option<(Meta, Cow<'file, [T]>)>> {
        let result = self.result.take();
        Ok(result.map(|(meta, items)| (meta, Cow::Borrowed(items))))
    }
}

impl UniversalWrite for MmapFile {
    fn write<T: bytemuck::Pod>(&mut self, byte_offset: ByteOffset, items: &[T]) -> Result<()> {
        let mmap = self.as_bytes_mut();
        write(mmap, byte_offset, items)?;
        Ok(())
    }

    fn write_batch<'a, T: bytemuck::Pod>(
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
        let len = self.len;
        if len == 0 {
            return Ok(0);
        }

        let page_size = crate::mmap::advice::page_size()
            .ok_or_else(|| std::io::Error::other("failed to determine page size"))?;
        let num_pages = len.div_ceil(page_size);
        let mut vec = vec![0u8; num_pages];

        // SAFETY: `self.ptr.as_ptr()` is a valid page-aligned pointer for `len` bytes
        // (guaranteed by memmap2). `vec` is correctly sized for `num_pages` entries.
        let ret = unsafe { nix::libc::mincore(self.ptr.0.cast(), len, vec.as_mut_ptr().cast()) };
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
        let file: Self = MmapFile::open(
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
        let ptr = if P::IS_SEQUENTIAL {
            self.ptr_seq
        } else {
            self.ptr
        };
        unsafe { slice::from_raw_parts(ptr.0, self.len) }
    }

    fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.ptr.0, self.len) }
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
