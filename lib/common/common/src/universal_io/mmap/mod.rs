mod pipeline;

use std::borrow::Cow;
use std::io::ErrorKind;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::{fs, io, slice};

use memmap2::MmapRaw;
use parking_lot::Mutex;

use self::pipeline::MmapReadPipeline;
use super::traits::{UniversalReadFileOps, UniversalReadFs, UniversalWriteFileOps};
use super::*;
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::AccessPattern;
use crate::mmap::{Advice, AdviceSetting, MULTI_MMAP_IS_SUPPORTED, Madviseable as _};

/// Filesystem handle for local mmap-backed files. Stateless.
#[derive(Debug, Default, Clone, Copy)]
pub struct MmapFs;

impl UniversalReadFileOps for MmapFs {
    type ContextConfig = ();

    fn from_context(_: ()) -> UioResult<Self> {
        Ok(MmapFs)
    }

    fn list_files(&self, prefix_path: &Path) -> UioResult<Vec<ListedFile>> {
        local_file_ops::local_list_files(prefix_path)
    }

    fn exists(&self, path: &Path) -> UioResult<bool> {
        fs_err::exists(path).map_err(UniversalIoError::from)
    }
}

impl UniversalWriteFileOps for MmapFs {
    fn create(&self, path: &Path, expected_length: usize) -> UioResult<()> {
        local_file_ops::local_create(path, expected_length)
    }

    fn create_dir(&self, path: &Path) -> UioResult<()> {
        local_file_ops::local_create_dir(path)
    }

    fn remove(&self, path: &Path) -> UioResult<()> {
        local_file_ops::local_remove(path)
    }

    fn remove_dir(&self, path: &Path) -> UioResult<()> {
        local_file_ops::local_remove_dir(path)
    }

    fn atomic_save(&self, path: &Path, bytes: &[u8]) -> UioResult<()> {
        local_file_ops::local_atomic_save(path, bytes)
    }
}

impl UniversalReadFs for MmapFs {
    type File = MmapFile;
    type OpenExtra = ();

    fn open(
        &self,
        path: impl AsRef<Path>,
        options: OpenOptions,
        _extra: (),
    ) -> UioResult<MmapFile> {
        MmapFile::open_inner(path, options)
    }
}

/// A memory-mapped local file handle.
///
/// # Clones share the mapping
///
/// Clones share the underlying mapping but keep their own raw `ptr`/`len`
/// copies. Growing the file through one handle ([`UniversalAppend::append`],
/// or [`UniversalRead::reopen`] after external growth) remaps the shared
/// mapping, which may move or replace it — the sibling clones' cached
/// pointers then dangle. Reading through such a clone is undefined behavior
/// (not merely a stale view), even long after the growing call returned,
/// until that clone calls [`UniversalRead::reopen`] itself.
#[derive(Debug, Clone)]
pub struct MmapFile {
    path: PathBuf,

    writeable: bool,
    populate: bool,
    #[cfg_attr(target_os = "linux", expect(dead_code))]
    advice: AdviceSetting,

    /// Dedicated `O_APPEND` fd, lazily opened on the first
    /// [`UniversalAppend::append`]. Kept separate from the mmap so appends
    /// cannot disturb the mapping, and shared across clones so every
    /// handle's flusher fdatasyncs appended growth — regardless of which
    /// clone appended, or in which order clones and flushers were created.
    append_file: Arc<OnceLock<fs_err::File>>,

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
    pub(super) fn open_inner(path: impl AsRef<Path>, options: OpenOptions) -> UioResult<Self> {
        let OpenOptions {
            writeable,
            need_sequential,
            populate,
            advice,
        } = options;

        let populate = match populate {
            Populate::Auto => Self::populate_auto(),
            Populate::Partial(_) | // mmap does not support partial populate
            Populate::No => false,
            Populate::PreferBackground | // mmap does not yet implement background populate
            Populate::Blocking => true,
        };

        let mmap = open_mmap(path.as_ref(), writeable, populate, advice)?;
        let ptr = SendSyncPtr(mmap.as_mut_ptr());

        let (mmap_seq, len, ptr_seq) = if need_sequential && *MULTI_MMAP_IS_SUPPORTED {
            let mmap_seq = open_mmap(
                path.as_ref(),
                false,
                false,
                AdviceSetting::Advice(Advice::Sequential),
            )?;

            let len = std::cmp::min(mmap.len(), mmap_seq.len());
            let ptr_seq = SendSyncPtr(mmap_seq.as_mut_ptr());
            (Some(mmap_seq), len, ptr_seq)
        } else {
            (None, mmap.len(), ptr)
        };

        let mmap = Self {
            path: path.as_ref().into(),
            writeable,
            populate,
            advice,
            append_file: Arc::new(OnceLock::new()),
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

    fn reopen(&mut self) -> UioResult<()> {
        let old_len = self.len as u64;
        let new_len = fs_err::File::open(self.path())
            .map_err(|err| UniversalIoError::extract_not_found(err, self.path()))?
            .metadata()?
            .len();
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

        self.remap_to(new_len as usize, self.populate)
    }

    fn read_bytes<P: AccessPattern>(
        &self,
        range: Range<u64>,
        _align: usize,
    ) -> UioResult<ACow<'_>> {
        let mmap = self.as_bytes::<P>();
        let bytes = read_bytes(mmap, range)?;
        Ok(ACow::Borrowed(bytes))
    }

    /// Override the default pipeline-based for better performance.
    fn read_iter<P: AccessPattern, T: Item, U: UserData>(
        &self,
        ranges: impl IntoIterator<Item = (U, ReadRange)>,
        _access_pattern: P,
    ) -> UioResult<impl Iterator<Item = UioResult<(U, Cow<'_, [T]>)>>> {
        let bytes = self.as_bytes::<P>();
        Ok(ranges.into_iter().map(move |(user_data, range)| {
            let items = read_bytemuck::<T>(bytes, range)?;
            Ok((user_data, Cow::Borrowed(items)))
        }))
    }

    /// Override the default pipeline-based for better performance.
    fn read_batch<P: AccessPattern, T: Item, U: UserData, E: From<UniversalIoError>>(
        &self,
        ranges: impl IntoIterator<Item = (U, ReadRange)>,
        _access_pattern: P,
        mut callback: impl FnMut(U, &[T]) -> Result<(), E>,
    ) -> Result<(), E> {
        let bytes = self.as_bytes::<P>();
        for (user_data, range) in ranges {
            let items = read_bytemuck::<T>(bytes, range)?;
            callback(user_data, items)?;
        }
        Ok(())
    }

    fn len<T>(&self) -> UioResult<u64> {
        let len = self.len / size_of::<T>();
        Ok(len as u64)
    }

    fn populate(&self) -> UioResult<()> {
        self.mmap.lock().populate();
        Ok(())
    }

    fn populate_auto() -> bool {
        false
    }

    fn clear_ram_cache(&self) -> UioResult<()> {
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
    fn write<T: bytemuck::Pod>(&mut self, byte_offset: ByteOffset, items: &[T]) -> UioResult<()> {
        let mmap = self.as_bytes_mut();
        write(mmap, byte_offset, items)?;
        Ok(())
    }

    fn write_batch<'a, T: bytemuck::Pod>(
        &mut self,
        offset_data: impl IntoIterator<Item = (ByteOffset, &'a [T])>,
    ) -> UioResult<()> {
        let mmap = self.as_bytes_mut();

        for (byte_offset, items) in offset_data {
            write(mmap, byte_offset, items)?;
        }

        Ok(())
    }
}

impl UniversalFlush for MmapFile {
    fn flusher(&self) -> Flusher {
        let mmap = self.mmap.clone();
        let append_file = self.append_file.clone();
        let flusher = move || {
            {
                // flushing empty mmap returns error on some platforms
                let mmap = mmap.lock();
                if mmap.len() > 0 {
                    mmap.flush()?;
                }
            }

            // Appends change the file size, and `msync` alone does not
            // persist size metadata — also fdatasync when any handle of
            // this file has appended. The shared cell is read at flush
            // time, so appends through clones, or appends made after this
            // flusher was created, are covered too.
            if let Some(file) = append_file.get() {
                file.sync_data()?;
            }

            Ok(())
        };

        Box::new(flusher)
    }
}

impl UniversalAppend for MmapFile {
    fn append<T: bytemuck::Pod>(&mut self, offset: ByteOffset, data: &[T]) -> UioResult<()> {
        let bytes: &[u8] = bytemuck::cast_slice(data);
        if bytes.is_empty() {
            return Ok(());
        }

        {
            let mut fd = self.append_fd()?;
            self.check_append_offset(fd, offset)?;
            io::Write::write_all(&mut fd, bytes)?;
        }

        // Remap so reads and `len` through this handle see the growth.
        self.grow_mapping(offset + bytes.len() as u64)?;

        Ok(())
    }

    fn append_batch<'a, T: bytemuck::Pod>(
        &mut self,
        offset: ByteOffset,
        items: impl IntoIterator<Item = &'a [T]>,
    ) -> UioResult<()> {
        let (mut slices, total) = local_file_ops::collect_append_slices(items);
        if total == 0 {
            return Ok(());
        }

        {
            let fd = self.append_fd()?;
            self.check_append_offset(fd, offset)?;
            local_file_ops::write_all_vectored(fd, &mut slices)?;
        }

        self.grow_mapping(offset + total as u64)?;

        Ok(())
    }
}

impl MmapFile {
    /// Grow the mapping to `new_len` without consulting the filesystem: the
    /// caller already knows the file's new length (an append it just made,
    /// or a `set_len` it just issued), so the `open`+`fstat`+`close` of a
    /// full [`reopen`](UniversalRead::reopen) is skipped. Unlike a reopen,
    /// the non-Linux re-mmap never re-populates: growth is mapping
    /// maintenance, and re-faulting the whole file would make each append
    /// O(file size) for handles opened with [`Populate::Blocking`].
    pub(crate) fn grow_mapping(&mut self, new_len: u64) -> UioResult<()> {
        debug_assert!(new_len as usize >= self.len, "grow_mapping cannot shrink");
        if new_len as usize == self.len {
            return Ok(());
        }

        self.remap_to(new_len as usize, false)
    }

    /// Remap both mmaps to `new_len` and refresh the cached pointers.
    ///
    /// `populate` only applies to the non-Linux path, which rebuilds the
    /// mapping from scratch; the Linux `mremap` keeps residency as is.
    fn remap_to(&mut self, new_len: usize, populate: bool) -> UioResult<()> {
        let mut mmap = self.mmap.lock();
        let mut mmap_seq = self.mmap_seq.as_ref().map(|m| m.lock());
        cfg_select! {
            // in linux, we can use `MmapRaw::remap`
            target_os = "linux" => {
                let _ = populate;

                // SAFETY:
                // We use may_move = true, since `remap` can fail if we don't allow it.
                // Moving is sound for *this* handle: `&mut self` keeps its reads out
                // while the cached pointers below are refreshed. Clones share this
                // mapping but keep their own pointer copies — per the contract
                // documented on `MmapFile` they must `reopen()` before reading after
                // any growth, so no live reference into the old location exists.
                let remap_options = memmap2::RemapOptions::new().may_move(true);
                unsafe {
                    mmap.remap(new_len, remap_options)?;
                    mmap_seq.as_mut().map(|m| m.remap(new_len, remap_options)).transpose()?;
                };

                // Whether or not `remap` moved the memory region let's update the pointers
                let ptr = SendSyncPtr(mmap.as_mut_ptr());
                let ptr_seq = mmap_seq.as_ref().map(|m| SendSyncPtr(m.as_mut_ptr())).unwrap_or(ptr);
                let len = new_len;
            }
            // otherwise, let's open again
            _ => {
                let _ = new_len; // suppress unused variable lint.
                *mmap = open_mmap(
                    self.path.as_ref(),
                    self.writeable,
                    populate,
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

    /// The append precondition: the file must currently end at `offset`.
    /// The fd is statted — not the mapping length, which can be stale when
    /// the file grew externally — so a stale handle gets a clean conflict.
    fn check_append_offset(&self, fd: &fs_err::File, offset: ByteOffset) -> UioResult<()> {
        let file_len = fd.metadata()?.len();
        if file_len != offset {
            return Err(UniversalIoError::AppendOffsetConflict {
                path: self.path.clone(),
                offset,
            });
        }

        Ok(())
    }

    /// Lazily open (and cache) a dedicated `O_APPEND` fd used exclusively for
    /// appends. Every `write(2)`/`writev(2)` through it is an atomic
    /// grow+write at the file's current end, so growth cannot leave a
    /// zero-filled window behind.
    fn append_fd(&self) -> UioResult<&fs_err::File> {
        if !self.writeable {
            return Err(UniversalIoError::Io(io::Error::new(
                ErrorKind::PermissionDenied,
                "append requires a handle opened with writeable=true",
            )));
        }

        if let Some(file) = self.append_file.get() {
            return Ok(file);
        }

        let file = fs_err::OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(|err| UniversalIoError::extract_not_found(err, &self.path))?;

        // A clone may have raced the initialization (concurrent appenders
        // are out of contract, but stay sound): exactly one fd is kept, a
        // losing one is dropped.
        Ok(self.append_file.get_or_init(|| file))
    }
}

fn open_mmap(
    path: &Path,
    write: bool,
    populate: bool,
    advice: AdviceSetting,
) -> UioResult<MmapRaw> {
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
        // SAFETY: `ptr`/`len` match the shared mapping unless this handle
        // missed a growth through a sibling clone — excluded by the
        // reopen-before-read contract documented on `MmapFile`.
        unsafe { slice::from_raw_parts(ptr.0, self.len) }
    }

    fn as_bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: see `as_bytes`.
        unsafe { slice::from_raw_parts_mut(self.ptr.0, self.len) }
    }
}

#[inline]
pub(crate) fn read_bytes(bytes: &[u8], range: Range<u64>) -> UioResult<&[u8]> {
    bytes
        .get(range.start as usize..range.end as usize)
        .ok_or_else(|| UniversalIoError::OutOfBounds {
            start: range.start,
            end: range.end,
            elements: bytes.len(),
        })
}

#[inline]
pub(crate) fn read_bytemuck<T: Item>(bytes: &[u8], range: ReadRange) -> UioResult<&[T]> {
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
fn write<T>(mmap: &mut [u8], byte_offset: ByteOffset, items: &[T]) -> UioResult<()>
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The append fd is shared across clones, so a flusher from any clone —
    /// even one created before the first append — fdatasyncs the growth.
    #[test]
    fn clone_flusher_sees_appends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("append.dat");
        MmapFs.create(&path, 0).unwrap();

        let options = OpenOptions {
            writeable: true,
            need_sequential: false,
            populate: Populate::No,
            advice: AdviceSetting::Global,
        };
        let mut file = MmapFs.open(&path, options, ()).unwrap();
        let clone = file.clone();
        let flusher = clone.flusher();

        file.append(0, b"tail".as_slice()).unwrap();

        // The append fd is visible through the sibling clone, and the
        // pre-created flusher takes the fdatasync path.
        assert!(clone.append_file.get().is_some());
        flusher().unwrap();
    }
}
