mod error;
mod pipeline;
mod pool;
mod runtime;

#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::io::{self, Read as _, Seek as _};
use std::os::fd::AsRawFd as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ::io_uring::types::Fd;
use fs_err as fs;
use fs_err::os::unix::fs::{FileExt as _, OpenOptionsExt as _};

use self::error::*;
use self::pipeline::{BorrowedIoUringPipeline, OwnedIoUringPipeline};
use self::pool::*;
use self::runtime::*;
use super::traits::{Item, UniversalReadFileOps, UniversalReadFs};
use super::*;
use crate::generic_consts::AccessPattern;

#[derive(Debug, Clone)]
pub struct IoUringFile {
    file: Arc<fs::File>,
    /// Whether the file was opened with `O_DIRECT` flag. This allows reads to be shorter
    /// than requested.
    ///
    /// This is because `O_DIRECT` can only read in aligned blocks of data, so reads at EOF might not
    /// be aligned with O_DIRECT alignment, but it is not possible to request less than one block.
    pub(super) direct_io: bool,
}

impl IoUringFile {
    pub(super) fn fd(&self) -> Fd {
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

impl TConfigContext for IoUringContextConfig {}

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
}

/// Per-open backend extras for [`IoUringFs::open`].
#[derive(Debug, Clone, Copy, Default)]
pub struct IoUringOpenExtra {
    /// Open with `O_DIRECT` to bypass the OS page cache. Requires
    /// block-aligned reads at runtime.
    pub prevent_caching: bool,
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

    type BorrowedReadPipeline<'a, T, U>
        = BorrowedIoUringPipeline<'a, T, U>
    where
        T: Item,
        U: UserData;

    type OwnedReadPipeline<T, U>
        = OwnedIoUringPipeline<T, U>
    where
        T: Item,
        U: UserData;

    fn reopen(&mut self) -> Result<()> {
        Ok(())
    }

    fn read<P: AccessPattern, T: Item>(&self, range: ReadRange) -> Result<Cow<'_, [T]>> {
        if self.direct_io {
            // direct_io needs special handling
            return self
                .read_iter::<P, T, _>([((), range)])?
                .next()
                .expect("there's exactly one read")
                .map(|(_, data)| data);
        }

        let mut items = vec![T::zeroed(); range.length as usize];
        let bytes = bytemuck::cast_slice_mut(&mut items);
        self.file.read_exact_at(bytes, range.byte_offset)?;
        Ok(Cow::Owned(items))
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
        let mut rt = IoUringRuntime::new()?;
        let mut items = items.into_iter().peekable();

        while items.peek().is_some() || rt.in_progress > 0 {
            rt.enqueue_while(|state| {
                let Some((byte_offset, items)) = items.next() else {
                    return Ok(None);
                };

                let entry = state.write((), self.fd(), byte_offset, items);
                Ok(Some(entry))
            })?;

            rt.submit_and_wait(1)?;

            for result in rt.completed() {
                let (_, resp) = result?;
                resp.expect_write();
            }
        }

        Ok(())
    }

    fn write_multi<'a, T: bytemuck::Pod>(
        files: &mut [Self],
        writes: impl IntoIterator<Item = (FileIndex, ByteOffset, &'a [T])>,
    ) -> Result<()> {
        let mut rt = IoUringRuntime::new()?;
        let mut writes = writes.into_iter().peekable();

        while writes.peek().is_some() || rt.in_progress > 0 {
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

                let entry = state.write((), file.fd(), byte_offset, items);
                Ok(Some(entry))
            })?;

            rt.submit_and_wait(1)?;

            for result in rt.completed() {
                let (_, resp) = result?;
                resp.expect_write();
            }
        }

        Ok(())
    }

    fn flusher(&self) -> Flusher {
        let file = self.file.clone();
        Box::new(move || Ok(file.sync_all()?))
    }
}
