mod pool;
mod read_iter;
mod runtime;
#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::io::{self, Read as _, Seek as _};
use std::os::fd::AsRawFd as _;
use std::path::PathBuf;
use std::sync::Arc;

use ::io_uring::types::Fd;
use fs_err as fs;
use fs_err::os::unix::fs::OpenOptionsExt;

use self::read_iter::{IoUringReadIter, IoUringReadMultiIter};
use self::runtime::with_uring_runtime;
use super::*;
use crate::generic_consts::AccessPattern;

#[derive(Debug)]
pub struct IoUringFile {
    file: Arc<fs::File>,
    /// Whether the file was opened with `O_DIRECT` flag. This allows reads to be shorter
    /// than requested.
    ///
    /// This is because `O_DIRECT` can only read in aligned blocks of data, so reads at EOF might not
    /// be aligned with O_DIRECT alignment, but it is not possible to request less than one block.
    uses_o_direct: bool,
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

impl<T: bytemuck::Pod + 'static> UniversalRead<T> for IoUringFile {
    fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        // Check that io_uring is supported on this system.
        pool::check_io_uring_supported().map_err(UniversalIoError::IoUringNotSupported)?;

        let OpenOptions {
            writeable,
            need_sequential: _,
            disk_parallel: _,
            populate: _,
            advice: _,
            prevent_caching,
        } = options;

        let mut opts = fs::OpenOptions::new();
        opts.read(true);
        opts.write(writeable);
        opts.create(false);
        if prevent_caching.unwrap_or_default() {
            opts.custom_flags(nix::libc::O_DIRECT);
        }

        let file = opts
            .open(path.as_ref())
            .map_err(|err| UniversalIoError::extract_not_found(err, path.as_ref()))?;

        let file = Self {
            file: Arc::new(file),
            uses_o_direct: prevent_caching.unwrap_or_default(),
        };

        Ok(file)
    }

    fn read<P: AccessPattern>(&self, range: ReadRange) -> Result<Cow<'_, [T]>> {
        with_uring_runtime(|mut rt| {
            let entry = rt.state.read(0, self.fd(), range, self.uses_o_direct)?;
            rt.enqueue_single(entry)?;
            // Loop because `submit_and_wait` may return before completions are
            // available on older kernels.
            while rt.completion_is_empty() {
                rt.submit_and_wait(1)?;
            }

            let (_, resp) = rt.completed().next().expect("read operation completed")?;
            let items = resp.expect_read();
            Ok(Cow::from(items))
        })?
    }

    fn read_batch<P: AccessPattern>(
        &self,
        ranges: impl IntoIterator<Item = ReadRange>,
        mut callback: impl FnMut(usize, &[T]) -> Result<()>,
    ) -> Result<()> {
        for record in self.read_iter::<P>(ranges) {
            let (idx, data) = record?;
            callback(idx, &data)?;
        }
        Ok(())
    }

    fn read_iter<P: AccessPattern>(
        &self,
        ranges: impl IntoIterator<Item = ReadRange>,
    ) -> impl Iterator<Item = Result<(usize, Cow<'_, [T]>)>> {
        match IoUringReadIter::new(self, ranges) {
            Ok(iter) => itertools::Either::Left(iter),
            Err(e) => itertools::Either::Right(std::iter::once(Err(e))),
        }
    }

    fn read_multi<P: AccessPattern>(
        files: &[Self],
        reads: impl IntoIterator<Item = (FileIndex, ReadRange)>,
        mut callback: impl FnMut(usize, FileIndex, &[T]) -> Result<()>,
    ) -> Result<()> {
        for record in Self::read_multi_iter::<P>(files, reads) {
            let (idx, file_idx, data) = record?;
            callback(idx, file_idx, &data)?;
        }
        Ok(())
    }

    fn read_multi_iter<P: AccessPattern>(
        files: &[Self],
        reads: impl IntoIterator<Item = (FileIndex, ReadRange)>,
    ) -> impl Iterator<Item = Result<(usize, FileIndex, Cow<'_, [T]>)>> {
        match IoUringReadMultiIter::new(files, reads) {
            Ok(iter) => itertools::Either::Left(iter),
            Err(e) => itertools::Either::Right(std::iter::once(Err(e))),
        }
    }

    fn len(&self) -> Result<u64> {
        let byte_len = self.file.metadata()?.len();

        let items_len = byte_len / size_of::<T>() as u64;
        debug_assert_eq!(byte_len % size_of::<T>() as u64, 0);

        Ok(items_len)
    }

    fn populate(&self) -> Result<()> {
        if self.uses_o_direct {
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
}

impl<T: bytemuck::Pod + 'static> UniversalWrite<T> for IoUringFile {
    fn write(&mut self, byte_offset: ByteOffset, items: &[T]) -> Result<()> {
        with_uring_runtime(|mut rt| {
            let entry = rt.state.write(0, self.fd(), byte_offset, items)?;
            rt.enqueue_single(entry)?;
            // Loop because `submit_and_wait` may return before completions are
            // available on older kernels.
            while rt.completion_is_empty() {
                rt.submit_and_wait(1)?;
            }

            let (_, resp) = rt.completed().next().expect("write operation completed")?;
            resp.expect_write();
            Ok(())
        })?
    }

    fn write_batch<'a>(
        &mut self,
        items: impl IntoIterator<Item = (ByteOffset, &'a [T])>,
    ) -> Result<()> {
        with_uring_runtime(|mut rt| {
            let mut items = items.into_iter().enumerate().peekable();

            while items.peek().is_some() || rt.in_progress > 0 {
                rt.enqueue(|state| {
                    let Some((id, (byte_offset, items))) = items.next() else {
                        return Ok(None);
                    };

                    let entry = state.write(id as _, self.fd(), byte_offset, items)?;
                    Ok(Some(entry))
                })?;

                rt.submit_and_wait(1)?;

                for result in rt.completed() {
                    let (_, resp) = result?;
                    resp.expect_write();
                }
            }

            Ok(())
        })?
    }

    fn write_multi<'a>(
        files: &mut [Self],
        writes: impl IntoIterator<Item = (FileIndex, ByteOffset, &'a [T])>,
    ) -> Result<()> {
        with_uring_runtime(|mut rt| {
            let mut writes = writes.into_iter().enumerate().peekable();

            while writes.peek().is_some() || rt.in_progress > 0 {
                rt.enqueue(|state| {
                    let Some((id, (file_index, byte_offset, items))) = writes.next() else {
                        return Ok(None);
                    };

                    let file = files.get(file_index).ok_or({
                        UniversalIoError::InvalidFileIndex {
                            file_index,
                            files: files.len(),
                        }
                    })?;

                    let entry = state.write(id as _, file.fd(), byte_offset, items)?;
                    Ok(Some(entry))
                })?;

                rt.submit_and_wait(1)?;

                for result in rt.completed() {
                    let (_, resp) = result?;
                    resp.expect_write();
                }
            }

            Ok(())
        })?
    }

    fn flusher(&self) -> Flusher {
        let file = self.file.clone();
        Box::new(move || Ok(file.sync_all()?))
    }
}
