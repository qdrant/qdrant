mod error;
mod pool;
mod read_iter;
mod runtime;

#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::io::{self, Read as _, Seek as _};
use std::iter;
use std::os::fd::AsRawFd as _;
use std::path::PathBuf;
use std::sync::Arc;

use ::io_uring::types::Fd;
use fs_err as fs;
use fs_err::os::unix::fs::{FileExt as _, OpenOptionsExt as _};

use self::error::*;
use self::pool::*;
use self::read_iter::*;
use self::runtime::*;
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

impl<T: bytemuck::Pod + 'static> UniversalRead<T> for IoUringFile {
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

    fn read<P: AccessPattern>(&self, range: ReadRange) -> Result<Cow<'_, [T]>> {
        let mut items = vec![T::zeroed(); range.length as usize];
        let bytes = bytemuck::cast_slice_mut(&mut items);
        self.file.read_exact_at(bytes, range.byte_offset)?;
        Ok(Cow::Owned(items))
    }

    fn read_batch<'a, P: AccessPattern, RequestId: 'a>(
        &'a self,
        ranges: impl IntoIterator<Item = (RequestId, ReadRange)>,
        mut callback: impl FnMut(RequestId, &[T]) -> Result<()>,
    ) -> Result<()> {
        for record in self.read_iter::<P, RequestId>(ranges) {
            let (id, items) = record?;
            callback(id, &items)?;
        }

        Ok(())
    }

    fn read_iter<P: AccessPattern, RequestId>(
        &self,
        ranges: impl IntoIterator<Item = (RequestId, ReadRange)>,
    ) -> impl Iterator<Item = Result<(RequestId, Cow<'_, [T]>)>> {
        let fd = self.fd();
        let direct_io = self.direct_io;
        let ranges = ranges
            .into_iter()
            .map(move |(id, range)| (id, fd, direct_io, range));
        match IoUringReadIter::new(ranges) {
            Ok(iter) => itertools::Either::Left(
                iter.map(|result| result.map(|(id, items)| (id, Cow::Owned(items)))),
            ),
            Err(err) => itertools::Either::Right(iter::once(Err(err))),
        }
    }

    fn read_multi<'a, P: AccessPattern, RequestId: 'a>(
        reads: impl IntoIterator<Item = (RequestId, &'a Self, ReadRange)>,
        mut callback: impl FnMut(RequestId, &[T]) -> Result<()>,
    ) -> Result<()>
    where
        Self: 'a,
    {
        for record in Self::read_multi_iter::<'a, P, RequestId>(reads) {
            let (id, items) = record?;
            callback(id, &items)?;
        }

        Ok(())
    }

    fn read_multi_iter<'a, P: AccessPattern, RequestId>(
        reads: impl IntoIterator<Item = (RequestId, &'a Self, ReadRange)>,
    ) -> impl Iterator<Item = Result<(RequestId, Cow<'a, [T]>)>> {
        let ranges = reads
            .into_iter()
            .map(|(id, file, range)| (id, file.fd(), file.direct_io, range));

        match IoUringReadIter::new(ranges) {
            Ok(iter) => itertools::Either::Left(
                iter.map(|result| result.map(|(id, items)| (id, Cow::Owned(items)))),
            ),
            Err(err) => itertools::Either::Right(iter::once(Err(err))),
        }
    }

    fn len(&self) -> Result<u64> {
        let byte_len = self.file.metadata()?.len();

        let items_len = byte_len / size_of::<T>() as u64;
        debug_assert_eq!(byte_len % size_of::<T>() as u64, 0);

        Ok(items_len)
    }

    fn populate(&self) -> Result<()> {
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
}

impl<T: bytemuck::Pod + 'static> UniversalWrite<T> for IoUringFile {
    fn write(&mut self, byte_offset: ByteOffset, items: &[T]) -> Result<()> {
        let bytes = bytemuck::cast_slice(items);
        self.file.write_all_at(bytes, byte_offset)?;
        Ok(())
    }

    fn write_batch<'a>(
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

                let entry = state.write((), self.fd(), byte_offset, items)?;
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

    fn write_multi<'a>(
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

                let entry = state.write((), file.fd(), byte_offset, items)?;
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
