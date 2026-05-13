mod error;
mod pool;
mod runtime;

#[cfg(test)]
mod tests;

use std::io::{self, Read as _, Seek as _};
use std::ops::Range;
use std::os::fd::AsRawFd as _;
use std::path::PathBuf;
use std::sync::Arc;

use ::io_uring::types::Fd;
use fs_err as fs;
use fs_err::os::unix::fs::{FileExt as _, OpenOptionsExt as _};

use self::error::*;
use self::pool::*;
use self::runtime::*;
use super::*;
use crate::aligned_buf::{AlignedBuf, AlignedCow};
use crate::generic_consts::AccessPattern;
use crate::universal_io::read::UniversalReadPipeline;

#[derive(Debug)]
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

impl UniversalReadFileOps for IoUringFile {
    fn list_files(prefix_path: &Path) -> Result<Vec<PathBuf>> {
        local_file_ops::local_list_files(prefix_path)
    }

    fn exists(path: &Path) -> Result<bool> {
        fs::exists(path).map_err(UniversalIoError::from)
    }
}

impl UniversalRead for IoUringFile {
    type ReadPipeline<'a, U>
        = IoUringPipeline<'a, U>
    where
        Self: 'a,
        U: UserData;

    fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        // Check that io_uring is supported on this system.
        pool::check_io_uring_support()?;

        let OpenOptions {
            writeable,
            need_sequential: _,
            disk_parallel: _,
            populate: _,
            advice: _,
            prevent_caching,
        } = options;

        let direct_io = prevent_caching.unwrap_or(false);
        let direct_io_flags = if direct_io { nix::libc::O_DIRECT } else { 0 };

        let file = fs::OpenOptions::new()
            .read(true)
            .write(writeable)
            .create(false)
            .custom_flags(direct_io_flags)
            .open(path.as_ref())
            .map_err(|err| UniversalIoError::extract_not_found(err, path.as_ref()))?;

        let file = Self {
            file: Arc::new(file),
            direct_io,
        };

        Ok(file)
    }

    fn read_bytes<P: AccessPattern>(
        &self,
        range: Range<u64>,
        align: usize,
    ) -> Result<AlignedCow<'_>> {
        if self.direct_io {
            // direct_io needs special handling
            let mut pipeline = IoUringPipeline::<()>::new()?;
            pipeline.schedule::<P>((), self, range, align)?;
            let (_, bytes) = pipeline.wait()?.expect("there's exactly one read");
            return Ok(bytes);
        }

        let len = (range.end - range.start) as usize;
        let mut data = vec![0u8; len];
        self.file.read_exact_at(&mut data, range.start)?;
        let mut buf = AlignedBuf::with_capacity(0, align, len);
        buf.extend_from_slice(&data);
        Ok(AlignedCow::Owned(buf))
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

pub struct IoUringPipeline<'file, U>
where
    U: UserData,
{
    runtime: IoUringRuntime<'file, U>,
}

impl<'file, U> UniversalReadPipeline<'file, U> for IoUringPipeline<'file, U>
where
    U: UserData,
{
    type File = IoUringFile;

    fn new() -> Result<Self> {
        Ok(Self {
            runtime: IoUringRuntime::new()?,
        })
    }

    fn can_schedule(&mut self) -> bool {
        let squeue = self.runtime.io_uring.submission();
        self.runtime.in_progress + squeue.len() < IO_URING_QUEUE_LENGTH as _
    }

    fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        file: &'file IoUringFile,
        range: Range<u64>,
        align: usize,
    ) -> Result<()> {
        let mut squeue = self.runtime.io_uring.submission();

        if self.runtime.in_progress + squeue.len() >= IO_URING_QUEUE_LENGTH as _ {
            return Err(UniversalIoError::QueueIsFull);
        }

        let entry = self
            .runtime
            .state
            .read(user_data, file.fd(), range, align, file.direct_io);

        unsafe {
            squeue.push(&entry).expect("submission queue is not full");
        }

        Ok(())
    }

    fn wait(&mut self) -> Result<Option<(U, AlignedCow<'file>)>> {
        let next = self.runtime.completed().next();

        let enqueued = self.runtime.enqueued();

        if next.is_some() && enqueued > 0 {
            self.runtime.submit_and_wait(0)?;
        } else if next.is_none() && enqueued + self.runtime.in_progress > 0 {
            self.runtime.submit_and_wait(1)?;
        }

        let Some(result) = next.or_else(|| self.runtime.completed().next()) else {
            return Ok(None);
        };

        let (user_data, resp) = result?;
        Ok(Some((user_data, AlignedCow::Owned(resp.expect_read()))))
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

                let entry = state.write((), self.fd(), byte_offset, bytemuck::cast_slice(items));
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

                let entry = state.write((), file.fd(), byte_offset, bytemuck::cast_slice(items));
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
