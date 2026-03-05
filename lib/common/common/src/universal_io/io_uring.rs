use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::{HashMap, hash_map};
use std::io;
use std::os::fd::AsRawFd as _;
use std::sync::Arc;

use ::io_uring::types::Fd;
use ::io_uring::{IoUring, opcode, squeue};
use fs_err as fs;

use super::*;

thread_local! {
    static IO_URING: RefCell<IoUring> = RefCell::new(IoUring::new(IO_URING_QUEUE_LENGTH).expect("io_uring initialized"));
}

const IO_URING_QUEUE_LENGTH: u32 = 16;

#[derive(Debug)]
pub struct IoUringFile {
    file: Arc<fs::File>,
}

impl IoUringFile {
    fn fd(&self) -> Fd {
        Fd(self.file.as_raw_fd())
    }
}

impl UniversalReadFileOps for IoUringFile {
    fn list_files(prefix_path: &Path) -> crate::universal_io::Result<Vec<PathBuf>> {
        local_file_ops::local_list_files(prefix_path)
    }

    fn exists(path: &Path) -> crate::universal_io::Result<bool> {
        fs::exists(path).map_err(UniversalIoError::from)
    }
}

impl UniversalRead<u8> for IoUringFile {
    fn open(path: impl AsRef<Path>, _options: OpenOptions) -> Result<Self>
    where
        Self: Sized,
    {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path.as_ref())?;

        let file = Self {
            file: Arc::new(file),
        };

        Ok(file)
    }

    fn read<const SEQUENTIAL: bool>(&self, range: ElementsRange) -> Result<Cow<'_, [u8]>> {
        IO_URING.with_borrow_mut(|io_uring| {
            let mut rt = IoUringRuntime::new(io_uring);

            let entry = rt.buffers.read(0, self.fd(), range).expect("ID is unique");
            rt.enqueue_single(entry)?;

            rt.submit_and_wait(1).expect("read operation submitted");

            let (_, buffer) = rt.completed().next().expect("read operation completed")?;
            let buffer = buffer.expect("associated buffer returned");
            Ok(Cow::from(buffer))
        })
    }

    fn read_batch<const SEQUENTIAL: bool>(
        &self,
        ranges: impl IntoIterator<Item = ElementsRange>,
        mut callback: impl FnMut(usize, &[u8]) -> Result<()>,
    ) -> Result<()> {
        IO_URING.with_borrow_mut(|io_uring| {
            let mut rt = IoUringRuntime::new(io_uring);
            let mut ranges = ranges.into_iter().enumerate().peekable();

            while ranges.peek().is_some() || rt.in_progress > 0 {
                rt.enqueue(|buffers| {
                    let (id, range) = ranges.next()?;

                    let entry = buffers
                        .read(id as _, self.fd(), range)
                        .expect("ID is unique");

                    Some(entry)
                });

                rt.submit_and_wait(1)?;

                for result in rt.completed() {
                    let (id, buffer) = result?;
                    let buffer = buffer.expect("associated buffer returned");
                    callback(id as _, &buffer)?;
                }
            }

            Ok(())
        })
    }

    fn len(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn populate(&self) -> Result<()> {
        Ok(())
    }

    fn clear_ram_cache(&self) -> Result<()> {
        Ok(())
    }
}

impl UniversalWrite<u8> for IoUringFile {
    fn write(&mut self, offset: ElementOffset, data: &[u8]) -> Result<()> {
        IO_URING.with_borrow_mut(|io_uring| {
            let mut rt = IoUringRuntime::new(io_uring);

            let length = u32::try_from(data.len()).expect("data length is within u32");
            let entry = opcode::Write::new(self.fd(), data.as_ptr(), length)
                .offset(offset)
                .build()
                .user_data(0);

            rt.enqueue_single(entry)?;

            rt.submit_and_wait(1).expect("write operation submitted");
            rt.completed().next().expect("write operation completed")?;
            Ok(())
        })
    }

    fn write_batch<'a>(
        &mut self,
        offset_data: impl IntoIterator<Item = (ElementOffset, &'a [u8])>,
    ) -> Result<()> {
        IO_URING.with_borrow_mut(|io_uring| {
            let mut rt = IoUringRuntime::new(io_uring);
            let mut offset_data = offset_data.into_iter().enumerate().peekable();

            while offset_data.peek().is_some() || rt.in_progress > 0 {
                rt.enqueue(|_| {
                    let (id, (offset, data)) = offset_data.next()?;

                    let length = u32::try_from(data.len()).expect("data length is within u32");
                    let entry = opcode::Write::new(self.fd(), data.as_ptr(), length)
                        .offset(offset)
                        .build()
                        .user_data(id as _);

                    Some(entry)
                });

                rt.submit_and_wait(1)?;

                for result in rt.completed() {
                    let _ = result?;
                }
            }

            Ok(())
        })
    }

    fn flusher(&self) -> Flusher {
        let file = self.file.clone();
        Box::new(move || Ok(file.sync_all()?))
    }
}

struct IoUringRuntime<'a> {
    io_uring: &'a mut IoUring,
    buffers: BufferStore,
    in_progress: usize,
}

impl<'a> IoUringRuntime<'a> {
    pub fn new(io_uring: &'a mut IoUring) -> Self {
        Self {
            io_uring,
            buffers: BufferStore::new(),
            in_progress: 0,
        }
    }

    pub fn enqueue_single(&mut self, entry: squeue::Entry) -> io::Result<()> {
        unsafe {
            self.io_uring
                .submission()
                .push(&entry)
                .expect("SQE is not full");
        }

        Ok(())
    }

    pub fn enqueue(&mut self, mut entries: impl FnMut(&mut BufferStore) -> Option<squeue::Entry>) {
        let mut sqe = self.io_uring.submission();

        if self.in_progress + sqe.len() >= IO_URING_QUEUE_LENGTH as _ {
            return;
        }

        while let Some(entry) = entries(&mut self.buffers) {
            unsafe { sqe.push(&entry).expect("SQE is not full") };

            if self.in_progress + sqe.len() >= IO_URING_QUEUE_LENGTH as _ {
                break;
            }
        }
    }

    pub fn submit_and_wait(&mut self, want: usize) -> io::Result<()> {
        self.in_progress += self.io_uring.submit_and_wait(want)?;
        Ok(())
    }

    pub fn completed(&mut self) -> impl Iterator<Item = io::Result<(u64, Option<Vec<u8>>)>> {
        self.io_uring.completion().map(|entry| {
            self.in_progress -= 1;

            let id = entry.user_data();
            let result = entry.result();

            if result < 0 {
                self.buffers.drop(id);

                return Err(io::Error::other(format!(
                    "io_uring operation {id} failed ({result})"
                )));
            }

            let length = result as _;

            let buffer = self.buffers.take(id, length);
            Ok((id, buffer))
        })
    }
}

impl<'a> Drop for IoUringRuntime<'a> {
    fn drop(&mut self) {
        while self.in_progress > 0 {
            // TODO: Cancel operations with `io_uring::Submitter::register_sync_cancel`

            self.submit_and_wait(self.in_progress)
                .expect("operations submitted");

            for result in self.completed() {
                match result {
                    Ok(_) => (),
                    Err(err) => log::debug!("{err}"),
                }
            }
        }
    }
}

#[derive(Debug)]
struct BufferStore {
    buffers: HashMap<u64, Vec<u8>>,
}

impl BufferStore {
    pub fn new() -> Self {
        Self {
            buffers: HashMap::new(),
        }
    }

    pub fn read(&mut self, id: u64, fd: Fd, range: ElementsRange) -> Option<squeue::Entry> {
        let ElementsRange {
            start: offset,
            length,
        } = range;

        let length = u32::try_from(length).expect("range length is within u32");

        let buffer = self.create(id, length as _)?;
        let entry = opcode::Read::new(fd, buffer.as_mut_ptr(), length as _)
            .offset(offset)
            .build()
            .user_data(id);

        Some(entry)
    }

    pub fn create(&mut self, id: u64, length: usize) -> Option<&mut Vec<u8>> {
        let hash_map::Entry::Vacant(entry) = self.buffers.entry(id) else {
            return None;
        };

        let buffer = entry.insert(Vec::with_capacity(length));
        Some(buffer)
    }

    pub fn take(&mut self, id: u64, length: usize) -> Option<Vec<u8>> {
        let mut buffer = self.buffers.remove(&id)?;
        unsafe { buffer.set_len(length) };
        Some(buffer)
    }

    pub fn drop(&mut self, id: u64) -> bool {
        self.buffers.remove(&id).is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.buffers.is_empty()
    }
}

impl Drop for BufferStore {
    fn drop(&mut self) {
        debug_assert!(self.is_empty());
    }
}
