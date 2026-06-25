mod error;
mod pipeline;
mod pool;
mod runtime;

#[cfg(test)]
mod tests;

use std::io::{self, Read as _, Seek as _};
use std::ops::Range;
use std::os::fd::AsRawFd as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ::io_uring::types::Fd;
use aligned_vec::avec_rt;
use fs_err as fs;
use fs_err::os::unix::fs::{FileExt as _, OpenOptionsExt as _};

use self::error::*;
use self::pipeline::{BorrowedIoUringPipeline, OwnedIoUringPipeline};
use self::pool::*;
use self::runtime::*;
use super::traits::{OpenExtra, UniversalReadFileOps, UniversalReadFs};
use super::*;
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::AccessPattern;

/// Required alignment for `O_DIRECT` reads (both file offset and buffer).
pub const KERNEL_PAGE_SIZE: usize = 4096; // 4 KB

#[derive(Debug, Clone)]
pub struct IoUringFile {
    file: Arc<fs::File>,
    /// Whether the file was opened with `O_DIRECT` flag. This allows reads to be shorter
    /// than requested.
    ///
    /// This is because `O_DIRECT` can only read in aligned blocks of data, so reads at EOF might not
    /// be aligned with O_DIRECT alignment, but it is not possible to request less than one block.
    direct_io: bool,
}

impl IoUringFile {
    fn fd(&self) -> Fd {
        Fd(self.file.as_raw_fd())
    }
}

/// Filesystem handle for `io_uring`-backed files. No per-instance state
/// today; the per-call `prevent_caching` knob lives on
/// [`OpenOptions`](super::OpenOptions).
#[derive(Debug, Clone, Copy, Default)]
pub struct IoUringFs;

#[derive(Debug, Clone, Copy, Default)]
pub struct IoUringContextConfig;

impl UniversalReadFileOps for IoUringFs {
    type ContextConfig = IoUringContextConfig;

    fn from_context(_ctx: Self::ContextConfig) -> Result<Self> {
        Ok(Self)
    }

    fn list_files(&self, prefix_path: &Path) -> Result<Vec<PathBuf>> {
        local_file_ops::local_list_files(prefix_path)
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        fs::exists(path).map_err(UniversalIoError::from)
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

/// Per-open backend extras for [`IoUringFs::open`].
#[derive(Debug, Clone, Copy, Default)]
pub struct IoUringOpenExtra {
    /// Open with `O_DIRECT` to bypass the OS page cache. Requires
    /// block-aligned reads at runtime.
    pub prevent_caching: bool,
}

impl OpenExtra for IoUringOpenExtra {
    fn with_prevent_caching(self, prevent_caching: bool) -> Self {
        let Self { prevent_caching: _ } = self;
        Self { prevent_caching }
    }
}

impl UniversalReadFs for IoUringFs {
    type File = IoUringFile;
    type OpenExtra = IoUringOpenExtra;

    fn open(
        &self,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: IoUringOpenExtra,
    ) -> Result<IoUringFile> {
        // Check that io_uring is supported on this system.
        pool::check_io_uring_support()?;

        let OpenOptions {
            writeable,
            need_sequential: _,
            populate: _,
            advice: _,
        } = options;
        let IoUringOpenExtra { prevent_caching } = extra;

        let direct_io = prevent_caching;
        let direct_io_flags = if direct_io { nix::libc::O_DIRECT } else { 0 };

        let file = fs::OpenOptions::new()
            .read(true)
            .write(writeable)
            .create(false)
            .custom_flags(direct_io_flags)
            .open(path.as_ref())
            .map_err(|err| UniversalIoError::extract_not_found(err, path.as_ref()))?;

        Ok(IoUringFile {
            file: Arc::new(file),
            direct_io,
        })
    }
}

impl UniversalRead for IoUringFile {
    type Fs = IoUringFs;

    type BorrowedReadPipeline<'a, U>
        = BorrowedIoUringPipeline<'a, U>
    where
        Self: 'a,
        U: UserData;

    type OwnedReadPipeline<U>
        = OwnedIoUringPipeline<U>
    where
        U: UserData;

    fn reopen(&mut self) -> Result<()> {
        Ok(())
    }

    fn read_bytes<P: AccessPattern>(&self, range: Range<u64>, align: usize) -> Result<ACow<'_>> {
        if self.direct_io {
            // direct_io needs special handling
            let mut pipeline = BorrowedIoUringPipeline::<()>::new()?;
            pipeline.schedule::<P>((), self, range, align)?;
            let (_, bytes) = pipeline.wait()?.expect("there's exactly one read");
            return Ok(bytes);
        }

        let len = (range.end - range.start) as usize;
        let mut bytes = avec_rt!([align] | 0u8; len);
        self.file.read_exact_at(&mut bytes, range.start)?;
        Ok(ACow::Owned(bytes))
    }

    fn len<T>(&self) -> Result<u64> {
        let byte_len = self.file.metadata()?.len();

        let items_len = byte_len / size_of::<T>() as u64;
        debug_assert_eq!(byte_len % size_of::<T>() as u64, 0);

        Ok(items_len)
    }

    fn populate(&self) -> Result<()> {
        if crate::low_memory::low_memory_mode().skip_populate() {
            return Ok(());
        }

        if self.direct_io {
            // O_DIRECT bypasses the page cache, so reading the file
            // would not warm it — skip.
            return Ok(());
        }

        let mut file = self.file.as_ref();
        file.seek(io::SeekFrom::Start(0))?;

        let mut buffer = vec![0u8; 1024 * 1024];
        while file.read(&mut buffer)? > 0 {}

        Ok(())
    }

    fn populate_auto() -> bool {
        false
    }

    fn clear_ram_cache(&self) -> Result<()> {
        crate::fs::clear_disk_cache(self.file.path())?;
        Ok(())
    }

    fn kind() -> UniversalKind {
        UniversalKind::IoUring
    }
}
impl UniversalWrite for IoUringFile {
    fn write<T: bytemuck::Pod>(&mut self, byte_offset: ByteOffset, items: &[T]) -> Result<()> {
        let bytes = bytemuck::cast_slice(items);
        self.file.write_all_at(bytes, byte_offset)?;
        Ok(())
    }

    fn write_batch<'a, T: bytemuck::Pod>(
        &mut self,
        items: impl IntoIterator<Item = (ByteOffset, &'a [T])>,
    ) -> Result<()> {
        let mut rt = IoUringWriteRuntime::new()?;
        let mut items = items.into_iter().peekable();

        while items.peek().is_some() || rt.in_progress() > 0 {
            rt.enqueue_while(|state| {
                let Some((byte_offset, items)) = items.next() else {
                    return Ok(None);
                };

                let entry = state.write((), self.fd(), byte_offset, bytemuck::cast_slice(items));
                Ok(Some(entry))
            })?;

            rt.submit_and_wait(1)?;

            for result in rt.completed() {
                result?;
            }
        }

        Ok(())
    }

    fn write_multi<'a, T: bytemuck::Pod>(
        files: &mut [Self],
        writes: impl IntoIterator<Item = (FileIndex, ByteOffset, &'a [T])>,
    ) -> Result<()> {
        let mut rt = IoUringWriteRuntime::new()?;
        let mut writes = writes.into_iter().peekable();

        while writes.peek().is_some() || rt.in_progress() > 0 {
            rt.enqueue_while(|state| {
                let Some((file_index, byte_offset, items)) = writes.next() else {
                    return Ok(None);
                };

                let file = files.get(file_index).ok_or({
                    UniversalIoError::InvalidFileIndex {
                        file_index,
                        files: files.len(),
                    }
                })?;

                let entry = state.write((), file.fd(), byte_offset, bytemuck::cast_slice(items));
                Ok(Some(entry))
            })?;

            rt.submit_and_wait(1)?;

            for result in rt.completed() {
                result?;
            }
        }

        Ok(())
    }

    fn flusher(&self) -> Flusher {
        let file = self.file.clone();
        Box::new(move || Ok(file.sync_all()?))
    }
}
