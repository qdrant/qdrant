use std::borrow::Cow;
use std::collections::{VecDeque, hash_map};
use std::io::{self, Read as _, Seek as _};
use std::marker::PhantomData;
use std::mem::{self, MaybeUninit};
use std::os::fd::AsRawFd as _;
use std::path::PathBuf;
use std::sync::Arc;

use ::io_uring::types::Fd;
use ::io_uring::{IoUring, opcode, squeue};
use ahash::AHashMap;
use fs_err as fs;
use fs_err::os::unix::fs::OpenOptionsExt;

use super::io_uring_pool::{self, IO_URING_QUEUE_LENGTH, IoUringGuard};
use super::*;
use crate::generic_consts::AccessPattern;
use crate::maybe_uninit::assume_init_vec;

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
        io_uring_pool::check_io_uring_supported()
            .map_err(UniversalIoError::IoUringNotSupported)?;

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
            rt.submit_and_wait(1)?;

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
        with_uring_runtime(|mut rt| {
            let mut reads = reads.into_iter().enumerate().peekable();
            let mut file_indices = Vec::new();

            while reads.peek().is_some() || rt.in_progress > 0 {
                rt.enqueue(|state| {
                    let Some((id, (file_index, range))) = reads.next() else {
                        return Ok(None);
                    };

                    let file = files.get(file_index).ok_or({
                        UniversalIoError::InvalidFileIndex {
                            file_index,
                            files: files.len(),
                        }
                    })?;

                    file_indices.push(file_index);

                    let entry = state.read(id as _, file.fd(), range, file.uses_o_direct)?;
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
        debug_assert_eq!(byte_len % size_of::<T>() as u64, 0);

        Ok(items_len)
    }

    fn populate(&self) -> Result<()> {
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
            rt.submit_and_wait(1)?;

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

/// Lazy, pipelined iterator over io_uring read results.
///
/// Keeps the io_uring submission queue continuously full: on every [`next()`] call it
/// refills free queue slots with new reads before waiting for completions. This means
/// the kernel always has work queued, maximising I/O parallelism.
///
/// The iterator holds its own [`IoUringState`] (request tracking / buffers) and
/// `in_progress` counter that persist across calls. On each [`next()`] it temporarily
/// borrows the thread-local [`IoUring`] ring to submit and reap.
struct IoUringReadIter<'a, T: bytemuck::Pod + 'static, I: Iterator<Item = ReadRange>> {
    file: &'a IoUringFile,
    /// Exclusive `IoUring` instance taken from the pool — returned on drop.
    guard: IoUringGuard,
    ranges: std::iter::Enumerate<I>,
    buffer: VecDeque<(usize, Vec<T>)>,
    state: IoUringState<'static, T>,
    in_progress: usize,
    /// `!Send + !Sync` — the iterator is tied to the thread whose pool it uses.
    _not_send: PhantomData<*const ()>,
}

impl<'a, T: bytemuck::Pod + 'static, I: Iterator<Item = ReadRange>> IoUringReadIter<'a, T, I> {
    fn new(file: &'a IoUringFile, ranges: impl IntoIterator<IntoIter = I>) -> Result<Self> {
        let guard =
            io_uring_pool::take_io_uring().map_err(UniversalIoError::IoUringNotSupported)?;
        Ok(Self {
            file,
            guard,
            ranges: ranges.into_iter().enumerate(),
            buffer: VecDeque::new(),
            state: IoUringState::new(),
            in_progress: 0,
            _not_send: PhantomData,
        })
    }

    /// Refill the submission queue, submit, and collect completions.
    ///
    /// When `wait` is `true`, blocks until at least one completion is available.
    /// When `false`, submits new reads and reaps only already-available completions.
    ///
    /// Returns `true` if there is (or may be) more work, `false` if fully exhausted.
    fn step(&mut self, wait: bool) -> Result<bool> {
        let io_uring = self.guard.io_uring();
        let fd = self.file.fd();
        let uses_o_direct = self.file.uses_o_direct;

        // Fill every free slot in the submission queue with a new read.
        let mut newly_queued = 0usize;
        {
            let mut sqe = io_uring.submission();
            while self.in_progress + sqe.len() < IO_URING_QUEUE_LENGTH as usize {
                let Some((id, range)) = self.ranges.next() else {
                    break;
                };
                let entry = self.state.read(id as _, fd, range, uses_o_direct)?;
                unsafe { sqe.push(&entry).expect("SQE is not full") };
                newly_queued += 1;
            }
            // SubmissionQueue::drop syncs the tail pointer.
        }

        // Nothing queued AND nothing in flight → exhausted.
        if self.in_progress == 0 && newly_queued == 0 {
            return Ok(false);
        }

        // Submit pending SQEs, optionally waiting for at least one completion.
        let want = if wait { 1 } else { 0 };
        self.in_progress += io_uring
            .submit_and_wait(want)
            .map_err(|err| io_error_context(err, "failed to submit io_uring operations"))?;

        // Reap all available completions.
        let cqes: Vec<_> = io_uring
            .completion()
            .map(|cqe: ::io_uring::cqueue::Entry| (cqe.user_data(), cqe.result()))
            .collect();

        for (id, result) in cqes {
            self.in_progress -= 1;

            if result < 0 {
                self.state.abort(id);
                return Err(io_error_context(
                    io::Error::from_raw_os_error(-result),
                    format!("io_uring operation {id} failed"),
                )
                .into());
            }

            let length = result as _;
            let resp = self.state.finalize(id, length)?;
            let items = resp.expect_read();
            self.buffer.push_back((id as usize, items));
        }

        Ok(true)
    }
}

impl<'a, T: bytemuck::Pod + 'static, I: Iterator<Item = ReadRange>> Iterator
    for IoUringReadIter<'a, T, I>
{
    type Item = Result<(usize, Cow<'a, [T]>)>;

    fn next(&mut self) -> Option<Self::Item> {
        // Always try to refill the submission queue and reap completions.
        // If the buffer is empty we must block-wait; otherwise just submit new
        // reads and opportunistically collect anything already done.
        let wait = self.buffer.is_empty();
        match self.step(wait) {
            Ok(false) if wait => return None,
            Err(e) => return Some(Err(e)),
            _ => {}
        }

        let (idx, data) = self.buffer.pop_front()?;
        Some(Ok((idx, Cow::Owned(data))))
    }
}

impl<'a, T: bytemuck::Pod + 'static, I: Iterator<Item = ReadRange>> Drop
    for IoUringReadIter<'a, T, I>
{
    fn drop(&mut self) {
        if self.in_progress == 0 {
            return;
        }
        let io_uring = self.guard.io_uring();
        while self.in_progress > 0 {
            if io_uring.submit_and_wait(self.in_progress).is_err() {
                break;
            }
            let cqes: Vec<_> = io_uring
                .completion()
                .map(|cqe: ::io_uring::cqueue::Entry| (cqe.user_data(), cqe.result()))
                .collect();
            for (id, result) in cqes {
                self.in_progress -= 1;
                if result < 0 {
                    self.state.abort(id);
                } else {
                    let _ = self.state.finalize(id, result as _);
                }
            }
        }
    }
}

/// Run a closure with exclusive access to an `IoUring` instance from the pool.
///
/// `'data` is the lifetime of any write buffers that will be submitted to io_uring.
/// The compiler enforces that write data outlives all in-flight operations, because
/// `IoUringState<'data, T>` holds `&'data [T]` references until operations complete.
///
/// The io_uring borrow lifetime is handled via HRTB (`for<'uring>`), keeping it
/// independent from the caller's data lifetime.
fn with_uring_runtime<'data, T: 'data, Out, F>(with_uring: F) -> Result<Out>
where
    F: for<'uring> FnOnce(IoUringRuntime<'uring, 'data, T>) -> Out,
{
    let mut guard =
        io_uring_pool::take_io_uring().map_err(UniversalIoError::IoUringNotSupported)?;
    let rt = IoUringRuntime::new(guard.io_uring());
    let output = with_uring(rt);
    Ok(output)
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

    pub fn enqueue<F>(&mut self, mut entries: F) -> Result<()>
    where
        F: FnMut(&mut IoUringState<'data, T>) -> Result<Option<squeue::Entry>>,
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
        self.in_progress += self
            .io_uring
            .submit_and_wait(want)
            .map_err(|err| io_error_context(err, "failed to submit io_uring operations"))?;

        Ok(())
    }

    pub fn completed(&mut self) -> impl Iterator<Item = io::Result<(u64, IoUringResponse<T>)>> {
        self.io_uring.completion().map(|entry| {
            self.in_progress -= 1;

            let id = entry.user_data();
            let result = entry.result();

            if result < 0 {
                self.state.abort(id);

                return Err(io_error_context(
                    io::Error::from_raw_os_error(-result),
                    format!("io_uring operation {id} failed"),
                ));
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
    pub fn read(
        &mut self,
        id: RequestId,
        fd: Fd,
        range: ReadRange,
        allow_short_read: bool,
    ) -> io::Result<squeue::Entry>
    where
        T: bytemuck::Pod,
    {
        let ReadRange {
            byte_offset,
            length,
        } = range;

        let mut items: Vec<MaybeUninit<T>> = Vec::with_capacity(length as _);
        items.resize_with(length as _, || MaybeUninit::uninit());

        let items = self
            .init(
                id,
                IoUringRequest::Read {
                    buffer: items,
                    allow_short_read,
                },
            )?
            .expect_read();

        let bytes_ptr = items.as_mut_ptr().cast();
        let byte_length = length * size_of::<T>() as u64;
        let byte_length = u32::try_from(byte_length).expect("read buffer length fit within u32");
        let entry = opcode::Read::new(fd, bytes_ptr, byte_length)
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
            IoUringRequest::Read {
                buffer: mut items,
                allow_short_read,
            } => {
                if allow_short_read {
                    let actual_items = byte_length as usize / mem::size_of::<T>();
                    debug_assert!(
                        actual_items <= items.len(),
                        "read returned more bytes than requested"
                    );
                    // Truncate to the actual number of items read (short read at EOF).
                    items.truncate(actual_items);
                } else {
                    assert_eq!(mem::size_of_val(items.as_slice()), byte_length as usize);
                }
                let items: Vec<T> = unsafe { assume_init_vec(items) };
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

type RequestId = u64;

#[derive(Debug)]
enum IoUringRequest<'data, T> {
    Read {
        buffer: Vec<MaybeUninit<T>>,
        allow_short_read: bool,
    },
    Write(&'data [T]),
}

impl<'data, T> IoUringRequest<'data, T> {
    pub fn expect_read(&mut self) -> &mut Vec<MaybeUninit<T>> {
        #[expect(clippy::match_wildcard_for_single_variants)]
        match self {
            IoUringRequest::Read { buffer, .. } => buffer,
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

pub(super) fn io_error_context(err: io::Error, context: impl Into<String>) -> io::Error {
    io::Error::new(err.kind(), IoErrorContext::new(err, context))
}

#[derive(Debug, thiserror::Error)]
#[error("{context}: {error}")]
struct IoErrorContext {
    context: String,
    error: io::Error,
}

impl IoErrorContext {
    fn new(error: io::Error, context: impl Into<String>) -> Self {
        Self {
            context: context.into(),
            error,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generic_consts::Sequential;

    #[test]
    fn test_io_uring_file_for_u64() -> Result<()> {
        // 1. Write some u64 binary data to a file using regular std::fs APIs
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_u64.bin");

        let data: Vec<u64> = (0..128).collect();
        let bytes = bytemuck::cast_slice(&data);
        fs_err::write(&path, bytes).unwrap();

        // 2. Read data back using `IoUringFile` and verify it matches what was written
        let file = TypedStorage::<IoUringFile, u64>::open(&path, OpenOptions::default())?;

        // Read all elements
        let read_back = file.read::<Sequential>(ReadRange {
            byte_offset: 0,
            length: data.len() as u64,
        })?;
        assert_eq!(read_back.as_ref(), &data);

        // Read a sub-range (start at element 10, byte offset = 10 * size_of::<u64>())
        let read_sub = file.read::<Sequential>(ReadRange {
            byte_offset: 10 * size_of::<u64>() as u64,
            length: 20,
        })?;
        assert_eq!(read_sub.as_ref(), &data[10..30]);

        // Verify len()
        let len = file.len()?;
        assert_eq!(len, 128);

        Ok(())
    }

    #[test]
    fn test_io_uring_read_batch() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_batch.bin");

        let data: Vec<u64> = (0..256).collect();
        fs_err::write(&path, bytemuck::cast_slice(&data)).unwrap();

        let file = TypedStorage::<IoUringFile, u64>::open(&path, OpenOptions::default())?;
        let elem = size_of::<u64>() as u64;

        // Non-contiguous ranges across the file.
        let ranges = vec![
            ReadRange {
                byte_offset: 0,
                length: 10,
            }, // [0..10]
            ReadRange {
                byte_offset: 50 * elem,
                length: 20,
            }, // [50..70]
            ReadRange {
                byte_offset: 100 * elem,
                length: 5,
            }, // [100..105]
            ReadRange {
                byte_offset: 200 * elem,
                length: 56,
            }, // [200..256]
        ];

        let expected: Vec<&[u64]> = vec![
            &data[0..10],
            &data[50..70],
            &data[100..105],
            &data[200..256],
        ];

        // --- read_batch (callback API) ---
        let mut batch_results: Vec<(usize, Vec<u64>)> = Vec::new();
        file.read_batch::<Sequential>(ranges.clone(), |idx, slice| {
            batch_results.push((idx, slice.to_vec()));
            Ok(())
        })?;

        batch_results.sort_by_key(|(idx, _)| *idx);
        for (idx, items) in &batch_results {
            assert_eq!(
                items.as_slice(),
                expected[*idx],
                "read_batch mismatch at index {idx}"
            );
        }

        // --- read_iter (iterator API) ---
        let mut iter_results: Vec<(usize, Vec<u64>)> = Vec::new();
        for record in file.read_iter::<Sequential>(ranges.clone()) {
            let (idx, cow) = record?;
            iter_results.push((idx, cow.into_owned()));
        }

        iter_results.sort_by_key(|(idx, _)| *idx);
        for (idx, items) in &iter_results {
            assert_eq!(
                items.as_slice(),
                expected[*idx],
                "read_iter mismatch at index {idx}"
            );
        }

        // --- read_iter with more ranges than the io_uring queue depth (64 > 16) ---
        let many_ranges: Vec<ReadRange> = (0..64)
            .map(|i| ReadRange {
                byte_offset: i * elem,
                length: 1,
            })
            .collect();

        let mut count = 0;
        for record in file.read_iter::<Sequential>(many_ranges) {
            let (idx, cow) = record?;
            assert_eq!(
                cow.as_ref(),
                &[data[idx]],
                "many-ranges mismatch at index {idx}"
            );
            count += 1;
        }
        assert_eq!(count, 64);

        Ok(())
    }

    #[test]
    fn test_io_uring_concurrent_read_iter() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let elem = size_of::<u64>() as u64;

        // Large enough to span many io_uring batches (64 ranges, queue depth 16).
        const NUM_ELEMENTS: u64 = 6400;
        const NUM_RANGES: u64 = 64;
        const CHUNK: u64 = NUM_ELEMENTS / NUM_RANGES; // 100 elements per range

        // File A: 0..NUM_ELEMENTS
        let path_a = dir.path().join("a.bin");
        let data_a: Vec<u64> = (0..NUM_ELEMENTS).collect();
        fs_err::write(&path_a, bytemuck::cast_slice(&data_a)).unwrap();

        // File B: offset so values never overlap with A.
        let path_b = dir.path().join("b.bin");
        let data_b: Vec<u64> = (1_000_000..1_000_000 + NUM_ELEMENTS).collect();
        fs_err::write(&path_b, bytemuck::cast_slice(&data_b)).unwrap();

        let opts = OpenOptions {
            prevent_caching: Some(false), // should be true
            ..Default::default()
        };
        let file_a = TypedStorage::<IoUringFile, u64>::open(&path_a, opts)?;
        let file_b = TypedStorage::<IoUringFile, u64>::open(&path_b, opts)?;

        // NUM_RANGES ranges, each reading CHUNK elements — well over the queue depth.
        let ranges_a: Vec<ReadRange> = (0..NUM_RANGES)
            .map(|i| ReadRange {
                byte_offset: i * CHUNK * elem,
                length: CHUNK,
            })
            .collect();
        let ranges_b: Vec<ReadRange> = ranges_a.clone();

        let iter_a = file_a.read_iter::<Sequential>(ranges_a);
        let iter_b = file_b.read_iter::<Sequential>(ranges_b);

        // Zip alternates next() calls between the two iterators on the same
        // thread-local io_uring ring. With in-flight operations left across
        // next() calls, one iterator can reap the other's CQEs.
        let mut count = 0u64;
        for (rec_a, rec_b) in iter_a.zip(iter_b) {
            let (idx_a, cow_a) = rec_a?;
            let (idx_b, cow_b) = rec_b?;

            let start_a = idx_a as u64 * CHUNK;
            assert_eq!(
                cow_a.as_ref(),
                &data_a[start_a as usize..(start_a + CHUNK) as usize],
                "file A mismatch at range index {idx_a}"
            );

            let start_b = idx_b as u64 * CHUNK;
            assert_eq!(
                cow_b.as_ref(),
                &data_b[start_b as usize..(start_b + CHUNK) as usize],
                "file B mismatch at range index {idx_b}"
            );
            count += 1;
        }
        assert_eq!(count, NUM_RANGES);

        Ok(())
    }
}
