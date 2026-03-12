use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::hash_map;
use std::io::{self, Read as _};
use std::mem::{self, MaybeUninit, size_of};
use std::os::fd::AsRawFd as _;
use std::sync::Arc;

use ::io_uring::types::Fd;
use ::io_uring::{IoUring, Probe, opcode, squeue};
use ahash::AHashMap;
use fs_err as fs;

use super::*;

thread_local! {
    static IO_URING: io::Result<RefCell<IoUring>> = init_io_uring().map(RefCell::new);
}

const IO_URING_QUEUE_LENGTH: u32 = 16;

fn init_io_uring() -> io::Result<IoUring> {
    let io_uring = IoUring::new(IO_URING_QUEUE_LENGTH)?;

    let mut probe = Probe::new();
    io_uring.submitter().register_probe(&mut probe)?;

    if probe.is_supported(opcode::Read::CODE) && probe.is_supported(opcode::Write::CODE) {
        Ok(io_uring)
    } else {
        Err(io::Error::other(
            "io_uring does not support required operations",
        ))
    }
}

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

impl<T: bytemuck::Pod + 'static> UniversalRead<T> for IoUringFile {
    fn open(path: impl AsRef<Path>, _options: OpenOptions) -> Result<Self>
    where
        Self: Sized,
    {
        // Check that `io_uring` was successfully initialized
        with_uring_runtime::<'_, u8, _, _>(|_| ())
            .map_err(|e| UniversalIoError::IoUringNotSupported(e.to_string()))?;

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
        with_uring_runtime(|mut rt| {
            let entry = rt.state.read(0, self.fd(), range)?;
            rt.enqueue_single(entry)?;
            rt.submit_and_wait(1)?;

            let (_, resp) = rt.completed().next().expect("read operation completed")?;
            let items = resp.expect_read();
            Ok(Cow::from(items))
        })?
    }

    fn read_batch<const SEQUENTIAL: bool>(
        &self,
        ranges: impl IntoIterator<Item = ElementsRange>,
        mut callback: impl FnMut(usize, &[T]) -> Result<()>,
    ) -> Result<()> {
        with_uring_runtime(|mut rt| {
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
                    let items = resp.expect_read();
                    callback(id as _, &items)?;
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
        with_uring_runtime(|mut rt| {
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

                    let entry = state.read(id as _, file.fd(), range)?;
                    Ok(Some(entry))
                })?;

                rt.submit_and_wait(1)?;

                for result in rt.completed() {
                    let (id, resp) = result?;

                    let file_idx = file_indices
                        .get(id as usize)
                        .copied()
                        .expect("file index is tracked");

                    let items = resp.expect_read();
                    callback(id as _, file_idx, &items)?;
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
        with_uring_runtime(|mut rt| {
            let byte_offset = element_to_byte_offset::<T>(offset);
            let entry = rt.state.write(0, self.fd(), byte_offset, items)?;
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
        with_uring_runtime(|mut rt| {
            let mut items = items.into_iter().enumerate().peekable();

            while items.peek().is_some() || rt.in_progress > 0 {
                rt.enqueue(|state| {
                    let Some((id, (offset, items))) = items.next() else {
                        return Ok(None);
                    };

                    let byte_offset = element_to_byte_offset::<T>(offset);
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
        writes: impl IntoIterator<Item = (FileIndex, ElementOffset, &'a [T])>,
    ) -> Result<()>
    where
        Self: Sized,
    {
        with_uring_runtime(|mut rt| {
            let mut writes = writes.into_iter().enumerate().peekable();

            while writes.peek().is_some() || rt.in_progress > 0 {
                rt.enqueue(|state| {
                    let Some((id, (file_index, offset, items))) = writes.next() else {
                        return Ok(None);
                    };

                    let file = files.get(file_index).ok_or_else(|| {
                        io::Error::other(format!("invalid file index {file_index}"))
                    })?;

                    let byte_offset = element_to_byte_offset::<T>(offset);
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

/// Run a closure with access to the thread-local io_uring runtime.
///
/// `'data` is the lifetime of any write buffers that will be submitted to io_uring.
/// The compiler enforces that write data outlives all in-flight operations, because
/// `IoUringState<'data, T>` holds `&'data [T]` references until operations complete.
///
/// The io_uring borrow lifetime is handled via HRTB (`for<'uring>`), keeping it
/// independent from the caller's data lifetime.
fn with_uring_runtime<'data, T: 'data, Out, F>(with_uring: F) -> io::Result<Out>
where
    F: for<'uring> FnOnce(IoUringRuntime<'uring, 'data, T>) -> Out,
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
        let rt = IoUringRuntime::new(&mut io_uring);
        let output = with_uring(rt);
        Ok(output)
    })
}

struct IoUringRuntime<'uring, 'data, T> {
    io_uring: &'uring mut IoUring,
    state: IoUringState<'data, T>,
    in_progress: usize,
}

impl<'uring, 'data, T> IoUringRuntime<'uring, 'data, T> {
    pub fn new(io_uring: &'uring mut IoUring) -> Self {
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
        F: FnMut(&mut IoUringState<'data, T>) -> io::Result<Option<squeue::Entry>>,
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

    pub fn completed(&mut self) -> impl Iterator<Item = io::Result<(u64, IoUringResponse<T>)>> {
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

impl<'uring, 'data, T> Drop for IoUringRuntime<'uring, 'data, T> {
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
struct IoUringState<'data, T> {
    requests: AHashMap<RequestId, IoUringRequest<'data, T>>,
}

impl<'data, T> IoUringState<'data, T> {
    pub fn new() -> Self {
        Self {
            requests: AHashMap::new(),
        }
    }

    /// Allocates `Vec<MaybeUninit<T>>`, reinterprets it as `Vec<MaybeUninit<u8>>`, and stores the byte buffer
    /// so the kernel writes into correctly aligned memory for `T`.
    pub fn read(&mut self, id: RequestId, fd: Fd, range: ElementsRange) -> io::Result<squeue::Entry>
    where
        T: bytemuck::Pod,
    {
        let ElementsRange {
            start: offset,
            length,
        } = range;

        let mut items: Vec<MaybeUninit<T>> = Vec::with_capacity(length as _);
        items.resize_with(length as _, || MaybeUninit::uninit());
        let items = self.init(id, IoUringRequest::Read(items))?.expect_read();

        let bytes_ptr = items.as_mut_ptr().cast();
        let byte_offset = offset as usize * size_of::<T>();
        let byte_length = length as usize * size_of::<T>();
        let byte_length = u32::try_from(byte_length).expect("read buffer length fit within u32");
        let entry = opcode::Read::new(fd, bytes_ptr, byte_length)
            .offset(byte_offset as _)
            .build()
            .user_data(id);

        Ok(entry)
    }

    pub fn write(
        &mut self,
        id: RequestId,
        fd: Fd,
        byte_offset: u64,
        items: &'data [T],
    ) -> io::Result<squeue::Entry>
    where
        T: bytemuck::Pod,
    {
        let items = self.init(id, IoUringRequest::Write(items))?.expect_write();

        let bytes: &[u8] = bytemuck::cast_slice(items);
        let byte_length = u32::try_from(bytes.len()).expect("write buffer length fit within u32");
        let entry = opcode::Write::new(fd, bytes.as_ptr(), byte_length)
            .offset(byte_offset)
            .build()
            .user_data(id);

        Ok(entry)
    }

    fn init(
        &mut self,
        id: RequestId,
        req: IoUringRequest<'data, T>,
    ) -> io::Result<&mut IoUringRequest<'data, T>> {
        let hash_map::Entry::Vacant(entry) = self.requests.entry(id) else {
            return Err(io::Error::other(format!("request {id} already exists")));
        };

        let req = entry.insert(req);
        Ok(req)
    }

    pub fn finalize(&mut self, id: RequestId, byte_length: u32) -> io::Result<IoUringResponse<T>> {
        let req = self
            .requests
            .remove(&id)
            .ok_or_else(|| io::Error::other("request {id} does not exist"))?;

        let resp = match req {
            IoUringRequest::Read(items) => {
                assert_eq!(mem::size_of_val(items.as_slice()), byte_length as usize);
                let items: Vec<T> = unsafe { mem::transmute(items) };
                IoUringResponse::Read(items)
            }

            IoUringRequest::Write(items) => {
                assert_eq!(mem::size_of_val(items), byte_length as usize);
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

impl<'data, T> Drop for IoUringState<'data, T> {
    fn drop(&mut self) {
        debug_assert!(self.is_empty());
    }
}

/// Convert element offset to byte offset
fn element_to_byte_offset<T>(element_offset: ElementOffset) -> u64 {
    element_offset * size_of::<T>() as u64
}

type RequestId = u64;

#[derive(Debug)]
enum IoUringRequest<'data, T> {
    Read(Vec<MaybeUninit<T>>),
    Write(&'data [T]),
}

impl<'data, T> IoUringRequest<'data, T> {
    pub fn expect_read(&mut self) -> &mut Vec<MaybeUninit<T>> {
        #[expect(clippy::match_wildcard_for_single_variants)]
        match self {
            IoUringRequest::Read(buffer) => buffer,
            _ => panic!(),
        }
    }

    pub fn expect_write(&self) -> &'data [T] {
        #[expect(clippy::match_wildcard_for_single_variants)]
        match self {
            IoUringRequest::Write(buffer) => buffer,
            _ => panic!(),
        }
    }
}

#[derive(Debug)]
enum IoUringResponse<T> {
    Read(Vec<T>),
    Write,
}

impl<T> IoUringResponse<T> {
    pub fn expect_read(self) -> Vec<T> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_uring_file_for_u64() -> Result<()> {
        // 1. Write some u64 binary data to a file using regular std::fs APIs
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_u64.bin");

        let data: Vec<u64> = (0..128).collect();
        let bytes = bytemuck::cast_slice(&data);
        fs_err::write(&path, bytes).unwrap();

        // 2. Read data back using `IoUringFile` and verify it matches what was written
        let file = <IoUringFile as UniversalRead<u64>>::open(&path, OpenOptions::default())?;

        // Read all elements
        let read_back = <IoUringFile as UniversalRead<u64>>::read::<true>(
            &file,
            ElementsRange {
                start: 0,
                length: data.len() as u64,
            },
        )?;
        assert_eq!(read_back.as_ref(), &data);

        // Read a sub-range
        let read_sub = <IoUringFile as UniversalRead<u64>>::read::<true>(
            &file,
            ElementsRange {
                start: 10,
                length: 20,
            },
        )?;
        assert_eq!(read_sub.as_ref(), &data[10..30]);

        // Verify len()
        let len = <IoUringFile as UniversalRead<u64>>::len(&file)?;
        assert_eq!(len, 128);

        Ok(())
    }
}
