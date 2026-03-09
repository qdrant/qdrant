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
            let mut rt = Runtime::new(io_uring);

            let entry = rt.state.read(0, self.fd(), range)?;
            rt.enqueue_single(entry)?;
            rt.submit_and_wait(1)?;

            let (_, resp) = rt.completed().next().expect("read operation completed")?;
            let buffer = resp.expect_read();
            Ok(Cow::from(buffer))
        })
    }

    fn read_batch<const SEQUENTIAL: bool>(
        &self,
        ranges: impl IntoIterator<Item = ElementsRange>,
        mut callback: impl FnMut(usize, &[u8]) -> Result<()>,
    ) -> Result<()> {
        IO_URING.with_borrow_mut(|io_uring| {
            let mut rt = Runtime::new(io_uring);
            let mut ranges = ranges.into_iter().enumerate().peekable();

            while ranges.peek().is_some() || rt.in_progress > 0 {
                rt.enqueue(|state| {
                    let Some((id, range)) = ranges.next() else {
                        return Ok(None);
                    };

                    let entry = state.read(id as _, self.fd(), range)?;
                    Ok(Some(entry))
                })?;

                rt.submit_and_wait(1)?;

                for result in rt.completed() {
                    let (id, resp) = result?;
                    let buffer = resp.expect_read();
                    callback(id as _, &buffer)?;
                }
            }

            Ok(())
        })
    }

    fn read_multi<const SEQUENTIAL: bool>(
        files: &[Self],
        reads: impl IntoIterator<Item = (FileIndex, ElementsRange)>,
        mut callback: impl FnMut(usize, FileIndex, &[u8]) -> Result<()>,
    ) -> Result<()>
    where
        Self: Sized,
    {
        IO_URING.with_borrow_mut(|io_uring| {
            let mut rt = Runtime::new(io_uring);
            let mut reads = reads.into_iter().enumerate().peekable();
            let mut file_idxs = Vec::new();

            while reads.peek().is_some() || rt.in_progress > 0 {
                rt.enqueue(|state| {
                    let Some((id, (file_idx, range))) = reads.next() else {
                        return Ok(None);
                    };

                    let file = files.get(file_idx).ok_or_else(|| {
                        io::Error::other(format!("invalid file index {file_idx}"))
                    })?;

                    file_idxs.push(file_idx);

                    let entry = state.read(id as _, file.fd(), range)?;
                    Ok(Some(entry))
                })?;

                rt.submit_and_wait(1)?;

                for result in rt.completed() {
                    let (id, resp) = result?;
                    let buffer = resp.expect_read();
                    let file_idx = file_idxs.get(id as usize).copied().unwrap_or(usize::MAX);
                    callback(id as _, file_idx, &buffer)?;
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
            let mut rt = Runtime::new(io_uring);

            let entry = rt.state.write(0, self.fd(), offset, data)?;
            rt.enqueue_single(entry)?;
            rt.submit_and_wait(1)?;

            let (_, resp) = rt.completed().next().expect("write operation completed")?;
            resp.expect_write();
            Ok(())
        })
    }

    fn write_batch<'a>(
        &mut self,
        offset_data: impl IntoIterator<Item = (ElementOffset, &'a [u8])>,
    ) -> Result<()> {
        IO_URING.with_borrow_mut(|io_uring| {
            let mut rt = Runtime::new(io_uring);
            let mut offset_data = offset_data.into_iter().enumerate().peekable();

            while offset_data.peek().is_some() || rt.in_progress > 0 {
                rt.enqueue(|state| {
                    let Some((id, (offset, data))) = offset_data.next() else {
                        return Ok(None);
                    };

                    let entry = state.write(id as _, self.fd(), offset, data)?;
                    Ok(Some(entry))
                })?;

                rt.submit_and_wait(1)?;

                for result in rt.completed() {
                    let (_, resp) = result?;
                    resp.expect_write();
                }
            }

            Ok(())
        })
    }

    fn write_multi<'a>(
        files: &mut [Self],
        writes: impl IntoIterator<Item = (FileIndex, ElementOffset, &'a [u8])>,
    ) -> Result<()>
    where
        Self: Sized,
    {
        IO_URING.with_borrow_mut(|io_uring| {
            let mut rt = Runtime::new(io_uring);
            let mut writes = writes.into_iter().enumerate().peekable();

            while writes.peek().is_some() || rt.in_progress > 0 {
                rt.enqueue(|state| {
                    let Some((id, (file_idx, offset, data))) = writes.next() else {
                        return Ok(None);
                    };

                    let file = files.get(file_idx).ok_or_else(|| {
                        io::Error::other(format!("invalid file index {file_idx}"))
                    })?;

                    let entry = state.write(id as _, file.fd(), offset, data)?;
                    Ok(Some(entry))
                })?;

                rt.submit_and_wait(1)?;

                for result in rt.completed() {
                    let (_, resp) = result?;
                    resp.expect_write();
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

struct Runtime<'a> {
    io_uring: &'a mut IoUring,
    state: State<'a>,
    in_progress: usize,
}

impl<'a> Runtime<'a> {
    pub fn new(io_uring: &'a mut IoUring) -> Self {
        Self {
            io_uring,
            state: State::new(),
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
        F: FnMut(&mut State<'a>) -> io::Result<Option<squeue::Entry>>,
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

    pub fn completed(&mut self) -> impl Iterator<Item = io::Result<(u64, Response)>> {
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

impl<'a> Drop for Runtime<'a> {
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
struct State<'a> {
    requests: HashMap<RequestId, Request<'a>>,
}

impl<'a> State<'a> {
    pub fn new() -> Self {
        Self {
            requests: HashMap::new(),
        }
    }

    pub fn read(
        &mut self,
        id: RequestId,
        fd: Fd,
        range: ElementsRange,
    ) -> io::Result<squeue::Entry> {
        let ElementsRange {
            start: offset,
            length,
        } = range;

        let buffer = self
            .init(id, Request::Read(vec![0; length as _]))?
            .expect_read();

        let length = u32::try_from(length).expect("read range length fit within u32");
        let entry = opcode::Read::new(fd, buffer.as_mut_ptr(), length)
            .offset(offset)
            .build()
            .user_data(id);

        Ok(entry)
    }

    pub fn write(
        &mut self,
        id: RequestId,
        fd: Fd,
        offset: ElementOffset,
        buffer: &'a [u8],
    ) -> io::Result<squeue::Entry> {
        let buffer = self.init(id, Request::Write(buffer))?.expect_write();

        let length = u32::try_from(buffer.len()).expect("write buffer length fit within u32");
        let entry = opcode::Write::new(fd, buffer.as_ptr(), length)
            .offset(offset)
            .build()
            .user_data(id);

        Ok(entry)
    }

    fn init(&mut self, id: RequestId, req: Request<'a>) -> io::Result<&mut Request<'a>> {
        let hash_map::Entry::Vacant(entry) = self.requests.entry(id) else {
            return Err(io::Error::other(format!("request {id} already exists")));
        };

        let req = entry.insert(req);
        Ok(req)
    }

    pub fn finalize(&mut self, id: RequestId, length: u32) -> io::Result<Response> {
        let req = self
            .requests
            .remove(&id)
            .ok_or_else(|| io::Error::other("request {id} does not exist"))?;

        let resp = match req {
            Request::Read(buffer) => {
                assert_eq!(buffer.len(), length as usize);
                Response::Read(buffer)
            }

            Request::Write(buffer) => {
                assert_eq!(buffer.len(), length as usize);
                Response::Write
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

impl<'a> Drop for State<'a> {
    fn drop(&mut self) {
        debug_assert!(self.is_empty());
    }
}

type RequestId = u64;

#[derive(Debug)]
enum Request<'a> {
    Read(Vec<u8>),
    Write(&'a [u8]),
}

impl<'a> Request<'a> {
    pub fn expect_read(&mut self) -> &mut Vec<u8> {
        #[expect(clippy::match_wildcard_for_single_variants)]
        match self {
            Request::Read(buffer) => buffer,
            _ => panic!(),
        }
    }

    pub fn expect_write(&self) -> &[u8] {
        #[expect(clippy::match_wildcard_for_single_variants)]
        match self {
            Request::Write(buffer) => buffer,
            _ => panic!(),
        }
    }
}

#[derive(Debug)]
enum Response {
    Read(Vec<u8>),
    Write,
}

impl Response {
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
