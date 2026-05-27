use std::io;
use std::ops::Range;

use ::io_uring::types::Fd;
use ::io_uring::{opcode, squeue};
use aligned_vec::{AVec, ConstAlign, RuntimeAlign};
use slab::Slab;

use super::*;
use crate::universal_io::UserData;

/// Required alignment for `O_DIRECT` reads (both file offset and buffer).
const KERNEL_PAGE_SIZE: usize = 4096; // 4 kB

pub struct IoUringRuntime<'data, U: UserData> {
    pub io_uring: IoUringGuard,
    pub state: IoUringState<'data, U>,
    pub in_progress: usize,
}

impl<'data, U> IoUringRuntime<'data, U>
where
    U: UserData,
{
    pub fn new() -> Result<Self> {
        let mut io_uring = pool::get_io_uring()?;
        let capacity = io_uring.submission().capacity();
        let rt = Self {
            io_uring,
            state: IoUringState::with_capacity(capacity),
            in_progress: 0,
        };

        Ok(rt)
    }

    /// Push entries into Submission Queue while `entries` returns `Ok(Something)`
    /// or the queue is full.
    pub fn enqueue_while<F>(&mut self, mut entries: F) -> Result<()>
    where
        F: FnMut(&mut IoUringState<'data, U>) -> Result<Option<squeue::Entry>>,
    {
        let mut squeue = self.io_uring.submission();

        if self.in_progress + squeue.len() >= IO_URING_QUEUE_LENGTH as _ {
            return Ok(());
        }

        while let Some(entry) = entries(&mut self.state)? {
            unsafe { squeue.push(&entry).expect("submission queue is not full") };

            if self.in_progress + squeue.len() >= IO_URING_QUEUE_LENGTH as _ {
                break;
            }
        }

        Ok(())
    }

    pub fn enqueued(&mut self) -> usize {
        self.io_uring.submission().len()
    }

    pub fn submit_and_wait(&mut self, want: usize) -> io::Result<()> {
        let enqueued = self.io_uring.submission().len();

        debug_assert!(
            want == 0 || enqueued + self.in_progress >= want,
            "io_uring would block: \
             requested to wait for {want} operations to complete, \
             but not enough operations are enqueued or in-progress"
        );

        self.submit_and_wait_retry_early_wakeup(want)?;

        let remaining = self.io_uring.submission().len();
        debug_assert!(enqueued == 0 || enqueued > remaining);
        debug_assert_eq!(remaining, 0);

        self.in_progress += enqueued - remaining;

        Ok(())
    }

    fn submit_and_wait_retry_early_wakeup(&mut self, want: usize) -> io::Result<()> {
        self.submit_and_wait_retry_eintr(want)?;

        while want > 0 && self.io_uring.completion().is_empty() {
            self.submit_and_wait_retry_eintr(want)?;
        }

        Ok(())
    }

    fn submit_and_wait_retry_eintr(&self, want: usize) -> io::Result<()> {
        let result = loop {
            match self.io_uring.submit_and_wait(want) {
                Err(err) if err.kind() == io::ErrorKind::Interrupted => (),
                res => break res,
            }
        };

        result.map_err(|err| io_error_context(err, "failed to submit io_uring operations"))?;
        Ok(())
    }

    pub fn completed(&mut self) -> impl Iterator<Item = io::Result<(U, IoUringResponse)>> {
        self.io_uring.completion().map(|entry| {
            self.in_progress -= 1;

            let slot = entry.user_data() as usize;
            let result = entry.result();

            if result < 0 {
                self.state.abort(slot);

                return Err(io_error_context(
                    io::Error::from_raw_os_error(-result),
                    format!("io_uring operation in slot {slot} failed"),
                ));
            }

            let length = result as _;
            let (user_data, resp) = self.state.finalize(slot, length)?;
            Ok((user_data, resp))
        })
    }
}

impl<'data, U> Drop for IoUringRuntime<'data, U>
where
    U: UserData,
{
    fn drop(&mut self) {
        while self.in_progress > 0 || !self.io_uring.submission().is_empty() {
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
pub struct IoUringState<'data, U> {
    requests: Slab<PendingRequest<'data, U>>,
}

#[derive(Debug)]
struct PendingRequest<'data, U> {
    user_data: U,
    request: IoUringRequest<'data>,
}

impl<'data, U> IoUringState<'data, U>
where
    U: UserData,
{
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            requests: Slab::with_capacity(capacity),
        }
    }

    /// Prepare the buffer for storing the read with correct alignment, and returns the queue entry.
    pub fn read(
        &mut self,
        user_data: U,
        fd: Fd,
        range: Range<u64>,
        align: usize,
        o_direct: bool,
    ) -> squeue::Entry {
        if o_direct {
            self.read_o_direct(user_data, fd, range, align)
        } else {
            self.read_exact(user_data, fd, range, align)
        }
    }

    /// Allocates an `align`-aligned [`AlignedBuf`] sized exactly to the request,
    /// so the kernel writes into a buffer that is already correctly aligned for
    /// the caller's downstream cast.
    fn read_exact(
        &mut self,
        user_data: U,
        fd: Fd,
        range: Range<u64>,
        align: usize,
    ) -> squeue::Entry {
        let size =
            u32::try_from(range.end - range.start).expect("read buffer length fit within u32");
        let buffer = AVec::with_capacity(align, size as usize);
        let (slot, req) = self.init(user_data, IoUringRequest::Read { buffer });

        opcode::Read::new(fd, req.expect_read().as_mut_ptr(), size)
            .offset(range.start)
            .build()
            .user_data(slot as u64)
    }

    /// Allocates a page-aligned scratch byte buffer rounded up to whole pages.
    fn read_o_direct(
        &mut self,
        user_data: U,
        fd: Fd,
        range: Range<u64>,
        align: usize,
    ) -> squeue::Entry {
        // Make sure read buffer is kernel-page aligned
        let pages_start = range.start & !(KERNEL_PAGE_SIZE as u64 - 1);
        let pages_end = range.end.next_multiple_of(KERNEL_PAGE_SIZE as u64);

        let buffer = AVec::with_capacity(KERNEL_PAGE_SIZE, (pages_end - pages_start) as usize);

        // Range within the page-aligned buffer
        let inner_range = (range.start - pages_start) as usize..(range.end - pages_start) as usize;

        let (slot, req) = self.init(
            user_data,
            IoUringRequest::ODirectRead {
                buffer,
                inner_range,
                align,
            },
        );
        let buffer = req.expect_o_direct_read();

        let pages_len_bytes =
            u32::try_from(pages_end - pages_start).expect("read buffer length fit within u32");

        opcode::Read::new(fd, buffer.as_mut_ptr(), pages_len_bytes)
            .offset(pages_start)
            .build()
            .user_data(slot as u64)
    }

    pub fn write(
        &mut self,
        user_data: U,
        fd: Fd,
        byte_offset: u64,
        bytes: &'data [u8],
    ) -> squeue::Entry {
        let (slot, _) = self.init(user_data, IoUringRequest::Write(bytes));

        let byte_length = u32::try_from(bytes.len()).expect("write buffer length fit within u32");
        opcode::Write::new(fd, bytes.as_ptr(), byte_length)
            .offset(byte_offset)
            .build()
            .user_data(slot as u64)
    }

    fn init(
        &mut self,
        user_data: U,
        request: IoUringRequest<'data>,
    ) -> (usize, &mut IoUringRequest<'data>) {
        let entry = self.requests.vacant_entry();
        let slot = entry.key();
        let pending = entry.insert(PendingRequest { user_data, request });
        (slot, &mut pending.request)
    }

    pub fn finalize(&mut self, slot: usize, byte_length: u32) -> io::Result<(U, IoUringResponse)> {
        let PendingRequest { user_data, request } = self
            .requests
            .try_remove(slot)
            .ok_or_else(|| io::Error::other(format!("request in slot {slot} does not exist")))?;

        let byte_length = byte_length as usize;

        let resp = match request {
            IoUringRequest::Read { mut buffer } => {
                assert!(buffer.capacity() >= byte_length);
                // SAFETY: the kernel wrote `byte_length` into the buffer.
                unsafe { buffer.set_len(byte_length) };
                IoUringResponse::Read(buffer)
            }
            IoUringRequest::ODirectRead {
                mut buffer,
                inner_range,
                align,
            } => {
                // SAFETY: the kernel wrote `byte_length` into the buffer.
                unsafe { buffer.set_len(byte_length) };

                // We need to return an `align`-aligned buffer

                // TODO(perf): Currently, we are allocating 2 Vecs: a page-aligned vec, and then the `align`-aligned one.
                //             In theory we can use `opcode::ReadFixed` to use preregistered buffers so that we only
                //             allocate one extra buffer per read. Not done for now since it complicates implementation.

                //
                // buffer_bytes
                // │     ┌────┬────items┬────┬────┐      byte_length    │
                // ├─────┤ 1  │ 2  │ 3  │ 4  │ 5  ├───────────>|────────┤
                // │     └────┴────┴────┴────┴────┘                     │
                //       ^
                //   inner_range.start

                let result = AVec::from_slice(align, &buffer[inner_range]);
                IoUringResponse::Read(result)
            }

            IoUringRequest::Write(bytes) => {
                assert_eq!(bytes.len(), byte_length);
                IoUringResponse::Write
            }
        };

        Ok((user_data, resp))
    }

    pub fn abort(&mut self, slot: usize) {
        self.requests.try_remove(slot);
    }
}

impl<'data, U> Drop for IoUringState<'data, U> {
    fn drop(&mut self) {
        debug_assert!(self.requests.is_empty());
    }
}

#[derive(Debug)]
pub enum IoUringRequest<'data> {
    Read {
        buffer: AVec<u8, RuntimeAlign>,
    },

    ODirectRead {
        buffer: AVec<u8, ConstAlign<KERNEL_PAGE_SIZE>>,
        inner_range: Range<usize>,
        align: usize,
    },

    Write(&'data [u8]),
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "matchers for individual variants"
)]
impl<'data> IoUringRequest<'data> {
    pub fn expect_read(&mut self) -> &mut AVec<u8, RuntimeAlign> {
        match self {
            IoUringRequest::Read { buffer } => buffer,
            _ => panic!(),
        }
    }

    pub fn expect_o_direct_read(&mut self) -> &mut AVec<u8, ConstAlign<KERNEL_PAGE_SIZE>> {
        match self {
            IoUringRequest::ODirectRead { buffer, .. } => buffer,
            _ => panic!(),
        }
    }
}

#[derive(Debug)]
pub enum IoUringResponse {
    Read(AVec<u8, RuntimeAlign>),
    Write,
}

impl IoUringResponse {
    pub fn expect_read(self) -> AVec<u8, RuntimeAlign> {
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
