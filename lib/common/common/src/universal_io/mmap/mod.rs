mod pipeline;

use std::borrow::Cow;
use std::io::ErrorKind;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io, slice};

use memmap2::MmapRaw;
use parking_lot::Mutex;

use self::pipeline::MmapReadPipeline;
use super::traits::{UniversalReadFileOps, UniversalReadFs};
use super::*;
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::AccessPattern;
use crate::mmap::{Advice, AdviceSetting, MULTI_MMAP_IS_SUPPORTED, Madviseable as _};

/// Filesystem handle for local mmap-backed files. Stateless.
#[derive(Debug, Default, Clone, Copy)]
pub struct MmapFs;

impl UniversalReadFileOps for MmapFs {
    type ContextConfig = ();

    fn from_context(_: ()) -> Result<Self> {
        Ok(MmapFs)
    }

    fn list_files(&self, prefix_path: &Path) -> Result<Vec<PathBuf>> {
        local_file_ops::local_list_files(prefix_path)
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        fs_err::exists(path).map_err(UniversalIoError::from)
    }

    fn create(&self, path: &Path, expected_length: usize) -> Result<()> {
        local_file_ops::local_create(path, expected_length)
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        local_file_ops::local_create_dir(path)
    }

    fn remove(&self, path: &Path) -> Result<()> {
        local_file_ops::local_remove(path)
    }

    fn remove_dir(&self, path: &Path) -> Result<()> {
        local_file_ops::local_remove_dir(path)
    }

    fn atomic_save(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        local_file_ops::local_atomic_save(path, bytes)
    }
}

impl UniversalReadFs for MmapFs {
    type File = MmapFile;
    type OpenExtra = ();

    fn open(&self, path: impl AsRef<Path>, options: OpenOptions, _extra: ()) -> Result<MmapFile> {
        MmapFile::open_inner(path, options)
    }
}

#[derive(Debug, Clone)]
pub struct MmapFile {
    path: PathBuf,

    #[cfg_attr(target_os = "linux", expect(dead_code))]
    writeable: bool,
    #[cfg_attr(target_os = "linux", expect(dead_code))]
    populate: bool,
    #[cfg_attr(target_os = "linux", expect(dead_code))]
    advice: AdviceSetting,

    // `mmap` and `mmap_seq` own the mmaps.
    mmap: Arc<Mutex<MmapRaw>>,
    mmap_seq: Option<Arc<Mutex<MmapRaw>>>,

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

impl MmapFile {
    /// Internal open helper, used by `MmapFs::open`.
    pub(super) fn open_inner(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let OpenOptions {
            writeable,
            need_sequential,
            populate,
            advice,
        } = options;

        let populate = match populate {
            Populate::Auto => Self::populate_auto(),
            Populate::No => false,
            Populate::PreferBackground | // mmap does not yet implement background populate
            Populate::Blocking => true,
        };

        let mmap = open_mmap(path.as_ref(), writeable, populate, advice)?;
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
            writeable,
            populate,
            advice,
            mmap: Arc::new(Mutex::new(mmap)),
            mmap_seq: mmap_seq.map(|mmap_seq_| Arc::new(Mutex::new(mmap_seq_))),
            len,
            ptr,
            ptr_seq,
        };

        Ok(mmap)
    }
}

impl UniversalRead for MmapFile {
    type Fs = MmapFs;

    type ReadPipeline<'a, U>
        = MmapReadPipeline<'a, U>
    where
        Self: 'a,
        U: UserData;

    fn reopen(&mut self) -> Result<()> {
        let old_len = self.len as u64;
        let new_len = fs_err::File::open(self.path())?.metadata()?.len();
        if new_len < old_len {
            return Err(UniversalIoError::Io(io::Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "Reopen encountered a smaller file than expected; old_len: {old_len}, new_len: {new_len}"
                ),
            )));
        }
        if new_len == old_len {
            return Ok(());
        }

        let mut mmap = self.mmap.lock();
        let mut mmap_seq = self.mmap_seq.as_ref().map(|m| m.lock());
        cfg_select! {
            // in linux, we can use `MmapRaw::remap`
            target_os = "linux" => {
                // SAFETY:
                // We use may_move = true, since `remap` can fail if we don't allow it.
                // It is safe to allow moving since we are holding `&mut self`
                let remap_options = memmap2::RemapOptions::new().may_move(true);
                unsafe {
                    mmap.remap(new_len as usize, remap_options)?;
                    mmap_seq.as_mut().map(|m| m.remap(new_len as usize, remap_options)).transpose()?;
                };

                // Whether or not `remap` moved the memory region let's update the pointers
                let ptr = SendSyncPtr(mmap.as_mut_ptr());
                let ptr_seq = mmap_seq.as_ref().map(|m| SendSyncPtr(m.as_mut_ptr())).unwrap_or(ptr);
                let len = new_len as usize;
            }
            // otherwise, let's open again
            _ => {
                *mmap = open_mmap(
                    self.path.as_ref(),
                    self.writeable,
                    self.populate,
                    self.advice,
                )?;
                let ptr = SendSyncPtr(mmap.as_mut_ptr());

                let ptr_seq;
                let len;
                if let Some(mmap_seq) = mmap_seq.as_mut() {
                    let mmap_seq_ = open_mmap(
                        self.path(),
                        false,
                        false,
                        AdviceSetting::Advice(Advice::Sequential),
                    )?;
                    **mmap_seq = mmap_seq_;

                    len = std::cmp::min(mmap.len(), mmap_seq.len());
                    ptr_seq = SendSyncPtr(mmap_seq.as_mut_ptr());
                } else {
                    len = mmap.len();
                    ptr_seq = ptr;
                }
            }
        }

        self.ptr = ptr;
        self.ptr_seq = ptr_seq;
        self.len = len;

        Ok(())
    }

    fn read_bytes<P: AccessPattern>(&self, range: Range<u64>, _align: usize) -> Result<ACow<'_>> {
        let mmap = self.as_bytes::<P>();
        let bytes = read_bytes(mmap, range)?;
        Ok(ACow::Borrowed(bytes))
    }

    /// Override the default pipeline-based for better performance.
    fn read_iter<P: AccessPattern, T: Item, U: UserData>(
        &self,
        ranges: impl IntoIterator<Item = (U, ReadRange)>,
    ) -> Result<impl Iterator<Item = Result<(U, Cow<'_, [T]>)>>> {
        let bytes = self.as_bytes::<P>();
        Ok(ranges.into_iter().map(move |(user_data, range)| {
            let items = read_bytemuck::<T>(bytes, range)?;
            Ok((user_data, Cow::Borrowed(items)))
        }))
    }

    /// Override the default pipeline-based for better performance.
    fn read_batch<P: AccessPattern, T: Item, U: UserData>(
        &self,
        ranges: impl IntoIterator<Item = (U, ReadRange)>,
        mut callback: impl FnMut(U, &[T]) -> Result<()>,
    ) -> Result<()> {
        let bytes = self.as_bytes::<P>();
        for (user_data, range) in ranges {
            let items = read_bytemuck::<T>(bytes, range)?;
            callback(user_data, items)?;
        }
        Ok(())
    }

    fn len<T>(&self) -> Result<u64> {
        let len = self.len / size_of::<T>();
        Ok(len as u64)
    }

    fn populate(&self) -> Result<()> {
        self.mmap.lock().populate();
        Ok(())
    }

    fn populate_auto() -> bool {
        false
    }

    fn clear_ram_cache(&self) -> Result<()> {
        self.mmap.lock().clear_cache();
        if let Some(mmap_seq) = &self.mmap_seq {
            mmap_seq.lock().clear_cache();
        }
        Ok(())
    }

    fn kind() -> UniversalKind {
        UniversalKind::Mmap
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
            let mmap = mmap.lock();
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
        let fs = MmapFs;
        let file = fs
            .open(
                path,
                OpenOptions {
                    writeable: false,
                    need_sequential: false,
                    populate: Populate::No,
                    advice: AdviceSetting::Advice(Advice::Normal),
                },
                (),
            )
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let disk_bytes = file.disk_bytes()?;
        let resident_bytes = file.resident_bytes()?;
        Ok((disk_bytes, resident_bytes))
    }
}

impl MmapFile {
    pub(crate) fn as_bytes<P: AccessPattern>(&self) -> &[u8] {
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
pub(crate) fn read_bytes(bytes: &[u8], range: Range<u64>) -> Result<&[u8]> {
    bytes
        .get(range.start as usize..range.end as usize)
        .ok_or_else(|| UniversalIoError::OutOfBounds {
            start: range.start,
            end: range.end,
            elements: bytes.len(),
        })
}

#[inline]
pub(crate) fn read_bytemuck<T: Item>(bytes: &[u8], range: ReadRange) -> Result<&[T]> {
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
