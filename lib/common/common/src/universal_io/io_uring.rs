use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::{HashMap, hash_map};
use std::io::{self, Read as _};
use std::mem::size_of;
use std::os::fd::AsRawFd as _;
use std::sync::Arc;

use ::io_uring::types::Fd;
use ::io_uring::{IoUring, Probe, opcode, squeue};
use fs_err as fs;

use super::*;

thread_local! {
    static IO_URING: io::Result<RefCell<IoUring>> = IoUring::new(IO_URING_QUEUE_LENGTH).map(RefCell::new);
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

    /// Convert element range to byte range (offset, length in bytes)
    fn element_range_to_bytes<T>(range: ElementsRange) -> (u64, u64) {
        let byte_offset = Self::element_to_byte_offset::<T>(range.start);
        let byte_length = range.length * size_of::<T>() as u64;
        (byte_offset, byte_length)
    }

    /// Convert element offset to byte offset
    fn element_to_byte_offset<T>(element_offset: ElementOffset) -> u64 {
        element_offset * size_of::<T>() as u64
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

impl<T: bytemuck::Pod + 'static> UniversalRead<T> for IoUringFile {
    fn open(path: impl AsRef<Path>, _options: OpenOptions) -> Result<Self>
    where
        Self: Sized,
    {
        // Check that `io_uring` was succesfully initialized and basic read/write operations are supported
        with_uring(|io_uring| {
            let mut probe = Probe::new();
            io_uring.submitter().register_probe(&mut probe)?;

            if probe.is_supported(opcode::Read::CODE) && probe.is_supported(opcode::Write::CODE) {
                Ok(())
            } else {
                Err(io::Error::other(
                    "io_uring does not support required operations",
                ))
            }
        })??;

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

    fn read<const SEQUENTIAL: bool>(&self, range: ElementsRange) -> Result<Cow<'_, [T]>> {
        with_uring(|io_uring| {
            let mut rt = IoUringRuntime::new(io_uring);

            let (byte_offset, byte_length) = Self::element_range_to_bytes::<T>(range);
            let entry = rt.state.read(0, self.fd(), byte_offset, byte_length)?;
            rt.enqueue_single(entry)?;
            rt.submit_and_wait(1)?;

            let (_, resp) = rt.completed().next().expect("read operation completed")?;
            let bytes = resp.expect_read();
            let items: Vec<T> = bytemuck::cast_vec(bytes);
            Ok(Cow::from(items))
        })?
    }

    fn read_batch<const SEQUENTIAL: bool>(
        &self,
        ranges: impl IntoIterator<Item = ElementsRange>,
        mut callback: impl FnMut(usize, &[T]) -> Result<()>,
    ) -> Result<()> {
        with_uring(|io_uring| {
            let mut rt = IoUringRuntime::new(io_uring);
            let mut ranges = ranges.into_iter().enumerate().peekable();

            while ranges.peek().is_some() || rt.in_progress > 0 {
                rt.enqueue(|state| {
                    let Some((id, range)) = ranges.next() else {
                        return Ok(None);
                    };

                    let (byte_offset, byte_length) = Self::element_range_to_bytes::<T>(range);
                    let entry = state.read(id as _, self.fd(), byte_offset, byte_length)?;
                    Ok(Some(entry))
                })?;

                rt.submit_and_wait(1)?;

                for result in rt.completed() {
                    let (id, resp) = result?;
                    let bytes = resp.expect_read();
                    let items = bytemuck::cast_slice(&bytes);
                    callback(id as _, items)?;
                }
            }

            Ok(())
        })?
    }

    fn read_multi<const SEQUENTIAL: bool>(
        files: &[Self],
        reads: impl IntoIterator<Item = (FileIndex, ElementsRange)>,
        mut callback: impl FnMut(usize, FileIndex, &[T]) -> Result<()>,
    ) -> Result<()>
    where
        Self: Sized,
    {
        with_uring(|io_uring| {
            let mut rt = IoUringRuntime::new(io_uring);
            let mut reads = reads.into_iter().enumerate().peekable();
            let mut file_indices = Vec::new();

            while reads.peek().is_some() || rt.in_progress > 0 {
                rt.enqueue(|state| {
                    let Some((id, (file_index, range))) = reads.next() else {
                        return Ok(None);
                    };

                    let file = files.get(file_index).ok_or_else(|| {
                        io::Error::other(format!("invalid file index {file_index}"))
                    })?;

                    file_indices.push(file_index);

                    let (byte_offset, byte_length) = Self::element_range_to_bytes::<T>(range);
                    let entry = state.read(id as _, file.fd(), byte_offset, byte_length)?;
                    Ok(Some(entry))
                })?;

                rt.submit_and_wait(1)?;

                for result in rt.completed() {
                    let (id, resp) = result?;

                    let file_idx = file_indices
                        .get(id as usize)
                        .copied()
                        .expect("file index is tracked");

                    let buffer = resp.expect_read();
                    let items = bytemuck::cast_slice(&buffer);

                    callback(id as _, file_idx, items)?;
                }
            }

            Ok(())
        })?
    }

    fn len(&self) -> Result<u64> {
        let byte_len = self.file.metadata()?.len();
        let items_len = byte_len / size_of::<T>() as u64;
        Ok(items_len)
    }

    fn populate(&self) -> Result<()> {
        let mut file = self.file.as_ref();
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
    fn write(&mut self, offset: ElementOffset, items: &[T]) -> Result<()> {
        with_uring(|io_uring| {
            let mut rt = IoUringRuntime::new(io_uring);

            let byte_offset = Self::element_to_byte_offset::<T>(offset);
            let bytes = bytemuck::cast_slice(items);
            let entry = rt.state.write(0, self.fd(), byte_offset, bytes)?;
            rt.enqueue_single(entry)?;
            rt.submit_and_wait(1)?;

            let (_, resp) = rt.completed().next().expect("write operation completed")?;
            resp.expect_write();
            Ok(())
        })?
    }

    fn write_batch<'a>(
        &mut self,
        items: impl IntoIterator<Item = (ElementOffset, &'a [T])>,
    ) -> Result<()> {
        with_uring(|io_uring| {
            let mut rt = IoUringRuntime::new(io_uring);
            let mut items = items.into_iter().enumerate().peekable();

            while items.peek().is_some() || rt.in_progress > 0 {
                rt.enqueue(|state| {
                    let Some((id, (offset, item))) = items.next() else {
                        return Ok(None);
                    };

                    let byte_offset = Self::element_to_byte_offset::<T>(offset);
                    let bytes = bytemuck::cast_slice(item);
                    let entry = state.write(id as _, self.fd(), byte_offset, bytes)?;
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
        writes: impl IntoIterator<Item = (FileIndex, ElementOffset, &'a [T])>,
    ) -> Result<()>
    where
        Self: Sized,
    {
        with_uring(|io_uring| {
            let mut rt = IoUringRuntime::new(io_uring);
            let mut writes = writes.into_iter().enumerate().peekable();

            while writes.peek().is_some() || rt.in_progress > 0 {
                rt.enqueue(|state| {
                    let Some((id, (file_index, offset, items))) = writes.next() else {
                        return Ok(None);
                    };

                    let file = files.get(file_index).ok_or_else(|| {
                        io::Error::other(format!("invalid file index {file_index}"))
                    })?;

                    let byte_offset = Self::element_to_byte_offset::<T>(offset);
                    let bytes = bytemuck::cast_slice(items);
                    let entry = state.write(id as _, file.fd(), byte_offset, bytes)?;
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

fn with_uring<T, F>(with_uring: F) -> io::Result<T>
where
    F: FnOnce(&mut IoUring) -> T,
{
    IO_URING.with(|io_uring| {
        let io_uring = match io_uring {
            Ok(io_uring) => io_uring,
            Err(err) => {
                return Err(io::Error::other(format!(
                    "failed to initialize io_uring: {err}"
                )));
            }
        };

        let mut io_uring = io_uring.borrow_mut();
        let output = with_uring(&mut io_uring);
        Ok(output)
    })
}

struct IoUringRuntime<'a> {
    io_uring: &'a mut IoUring,
    state: IoUringState<'a>,
    in_progress: usize,
}

impl<'a> IoUringRuntime<'a> {
    pub fn new(io_uring: &'a mut IoUring) -> Self {
        Self {
            io_uring,
            state: IoUringState::new(),
            in_progress: 0,
        }
    }

    pub fn enqueue_single(&mut self, entry: squeue::Entry) -> io::Result<()> {
        unsafe {
            self.io_uring
                .submission()
                .push(&entry)
                .map_err(io::Error::other)?;
        }

        Ok(())
    }

    pub fn enqueue<F>(&mut self, mut entries: F) -> io::Result<()>
    where
        F: FnMut(&mut IoUringState<'a>) -> io::Result<Option<squeue::Entry>>,
    {
        let mut sqe = self.io_uring.submission();

        if self.in_progress + sqe.len() >= IO_URING_QUEUE_LENGTH as _ {
            return Ok(());
        }

        while let Some(entry) = entries(&mut self.state)? {
            unsafe { sqe.push(&entry).expect("SQE is not full") };

            if self.in_progress + sqe.len() >= IO_URING_QUEUE_LENGTH as _ {
                break;
            }
        }

        Ok(())
    }

    pub fn submit_and_wait(&mut self, want: usize) -> io::Result<()> {
        self.in_progress += self.io_uring.submit_and_wait(want)?;
        Ok(())
    }

    pub fn completed(&mut self) -> impl Iterator<Item = io::Result<(u64, IoUringResponse)>> {
        self.io_uring.completion().map(|entry| {
            self.in_progress -= 1;

            let id = entry.user_data();
            let result = entry.result();

            if result < 0 {
                self.state.abort(id);

                return Err(io::Error::other(format!(
                    "io_uring operation {id} failed ({result})"
                )));
            }

            let length = result as _;
            let resp = self.state.finalize(id, length)?;
            Ok((id, resp))
        })
    }
}

impl<'a> Drop for IoUringRuntime<'a> {
    fn drop(&mut self) {
        while self.in_progress > 0 {
            // TODO: Cancel operations with `io_uring::Submitter::register_sync_cancel`?

            // TODO: Implement `wait` (without submit) based on `io_uring::Submitter::enter`?
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
struct IoUringState<'a> {
    requests: HashMap<RequestId, IoUringRequest<'a>>,
}

impl<'a> IoUringState<'a> {
    pub fn new() -> Self {
        Self {
            requests: HashMap::new(),
        }
    }

    pub fn read(
        &mut self,
        id: RequestId,
        fd: Fd,
        byte_offset: u64,
        byte_length: u64,
    ) -> io::Result<squeue::Entry> {
        let buffer = self
            .init(id, IoUringRequest::Read(vec![0; byte_length as _]))?
            .expect_read();

        let length = u32::try_from(byte_length).expect("read byte length fit within u32");
        let entry = opcode::Read::new(fd, buffer.as_mut_ptr(), length)
            .offset(byte_offset)
            .build()
            .user_data(id);

        Ok(entry)
    }

    pub fn write(
        &mut self,
        id: RequestId,
        fd: Fd,
        byte_offset: u64,
        bytes: &'a [u8],
    ) -> io::Result<squeue::Entry> {
        let bytes = self.init(id, IoUringRequest::Write(bytes))?.expect_write();

        let length = u32::try_from(bytes.len()).expect("write buffer length fit within u32");
        let entry = opcode::Write::new(fd, bytes.as_ptr(), length)
            .offset(byte_offset)
            .build()
            .user_data(id);

        Ok(entry)
    }

    fn init(
        &mut self,
        id: RequestId,
        req: IoUringRequest<'a>,
    ) -> io::Result<&mut IoUringRequest<'a>> {
        let hash_map::Entry::Vacant(entry) = self.requests.entry(id) else {
            return Err(io::Error::other(format!("request {id} already exists")));
        };

        let req = entry.insert(req);
        Ok(req)
    }

    pub fn finalize(&mut self, id: RequestId, length: u32) -> io::Result<IoUringResponse> {
        let req = self
            .requests
            .remove(&id)
            .ok_or_else(|| io::Error::other("request {id} does not exist"))?;

        let resp = match req {
            IoUringRequest::Read(buffer) => {
                assert_eq!(buffer.len(), length as usize);
                IoUringResponse::Read(buffer)
            }

            IoUringRequest::Write(buffer) => {
                assert_eq!(buffer.len(), length as usize);
                IoUringResponse::Write
            }
        };

        Ok(resp)
    }

    pub fn abort(&mut self, id: RequestId) -> bool {
        self.requests.remove(&id).is_some()
    }

    fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }
}

impl<'a> Drop for IoUringState<'a> {
    fn drop(&mut self) {
        debug_assert!(self.is_empty());
    }
}

type RequestId = u64;

#[derive(Debug)]
enum IoUringRequest<'a> {
    Read(Vec<u8>),
    Write(&'a [u8]),
}

impl<'a> IoUringRequest<'a> {
    pub fn expect_read(&mut self) -> &mut Vec<u8> {
        #[expect(clippy::match_wildcard_for_single_variants)]
        match self {
            IoUringRequest::Read(buffer) => buffer,
            _ => panic!(),
        }
    }

    pub fn expect_write(&self) -> &[u8] {
        #[expect(clippy::match_wildcard_for_single_variants)]
        match self {
            IoUringRequest::Write(buffer) => buffer,
            _ => panic!(),
        }
    }
}

#[derive(Debug)]
enum IoUringResponse {
    Read(Vec<u8>),
    Write,
}

impl IoUringResponse {
    pub fn expect_read(self) -> Vec<u8> {
        #[expect(clippy::match_wildcard_for_single_variants)]
        match self {
            Self::Read(buffer) => buffer,
            _ => panic!(),
        }
    }

    pub fn expect_write(self) {
        #[expect(clippy::match_wildcard_for_single_variants)]
        match self {
            Self::Write => (),
            _ => panic!(),
        }
    }
}
