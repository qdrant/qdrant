use std::io;
use std::ops::Range;

use ::io_uring::types::Fd;
use ::io_uring::{opcode, squeue};
use aligned_vec::{AVec, RuntimeAlign};
use slab::Slab;

use super::*;

/// Required alignment for `O_DIRECT` reads (both file offset and buffer).
const KERNEL_PAGE_SIZE: usize = 4096; // 4 kB

pub type IoUringReadRuntime<U> = IoUringRuntime<Read, U>;
pub type IoUringWriteRuntime<'data, U> = IoUringRuntime<Write<'data>, U>;

pub struct IoUringRuntime<Req, U> {
    io_uring: IoUringGuard,
    state: IoUringState<Req, U>,
    in_progress: usize,
}

impl<Req, U> IoUringRuntime<Req, U> {
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

    pub fn can_schedule(&mut self) -> bool {
        self.enqueued() + self.in_progress < IO_URING_QUEUE_LENGTH as _
    }

    pub fn enqueued(&mut self) -> usize {
        self.io_uring.submission().len()
    }

    pub fn in_progress(&self) -> usize {
        self.in_progress
    }

    pub fn state(&mut self) -> &mut IoUringState<Req, U> {
        &mut self.state
    }

    pub fn enqueue(&mut self, entry: squeue::Entry) -> Result<()> {
        unsafe {
            self.io_uring
                .submission()
                .push(&entry)
                .map_err(|_| UniversalIoError::QueueIsFull)
        }
    }

    /// Push entries into Submission Queue while `entries` returns `Ok(Something)`
    /// or the queue is full.
    pub fn enqueue_while<F>(&mut self, mut entries: F) -> Result<()>
    where
        F: FnMut(&mut IoUringState<Req, U>) -> Result<Option<squeue::Entry>>,
    {
        let mut squeue = self.io_uring.submission();

        if squeue.len() + self.in_progress >= IO_URING_QUEUE_LENGTH as _ {
            return Ok(());
        }

        while let Some(entry) = entries(&mut self.state)? {
            unsafe { squeue.push(&entry).expect("submission queue is not full") };

            if squeue.len() + self.in_progress >= IO_URING_QUEUE_LENGTH as _ {
                break;
            }
        }

        Ok(())
    }

    pub fn submit_and_wait(&mut self, want: usize) -> io::Result<()> {
        let enqueued = self.enqueued();

        debug_assert!(
            want == 0 || enqueued + self.in_progress >= want,
            "io_uring would block: \
             requested to wait for {want} operations to complete, \
             but not enough operations are enqueued or in-progress"
        );

        self.submit_and_wait_retry_early_wakeup(want)?;

        let remaining = self.enqueued();
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

    pub fn completed(&mut self) -> impl Iterator<Item = io::Result<(U, Req::Resp)>> + '_
    where
        Req: Finalize,
    {
        self.completions().map(|result| {
            let (user_data, request, length) = result?;
            let response = request.finalize(length);
            Ok((user_data, response))
        })
    }

    fn completions(&mut self) -> impl Iterator<Item = io::Result<(U, Req, u32)>> + '_ {
        self.io_uring.completion().map(|entry| {
            self.in_progress -= 1;

            let slot = entry.user_data() as usize;
            let result = entry.result();

            let (user_data, request) = self.state.complete(slot)?;

            if result >= 0 {
                Ok((user_data, request, result as _))
            } else {
                Err(io_error_context(
                    io::Error::from_raw_os_error(-result),
                    format!("io_uring operation in slot {slot} failed"),
                ))
            }
        })
    }
}

impl<Req, U> Drop for IoUringRuntime<Req, U> {
    fn drop(&mut self) {
        // TODO: Cancel operations with `io_uring::Submitter::register_sync_cancel`?

        while self.enqueued() > 0 || self.in_progress > 0 {
            self.submit_and_wait(self.in_progress)
                .expect("pending operations submitted");

            for result in self.completions() {
                if let Err(err) = result {
                    log::debug!("{err}");
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct IoUringState<Req, U> {
    requests: Slab<Pending<Req, U>>,
}

#[derive(Debug)]
struct Pending<Req, U> {
    request: Req,
    user_data: U,
}

impl<Req, U> IoUringState<Req, U> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            requests: Slab::with_capacity(capacity),
        }
    }
}

impl<U> IoUringState<Read, U> {
    pub fn read(
        &mut self,
        user_data: U,
        fd: Fd,
        direct_io: bool,
        range: Range<u64>,
        align: usize,
    ) -> squeue::Entry {
        let Range { start, end } = range;

        let offset = start;
        let length = u32::try_from(end - start).expect("read length fit within u32");

        // Extend read range to next KERNEL_PAGE_SIZE boundary
        let kernel_length = if direct_io {
            assert!(
                align.is_multiple_of(KERNEL_PAGE_SIZE),
                "O_DIRECT read buffer must be aligned to {KERNEL_PAGE_SIZE} bytes (alignment: {align})",
            );

            assert!(
                offset.is_multiple_of(KERNEL_PAGE_SIZE as _),
                "O_DIRECT read offset must be aligned to {KERNEL_PAGE_SIZE} bytes (offset: {offset})",
            );

            // O_DIRECT read length must be aligned to KERNEL_PAGE_SIZE bytes
            length
                .checked_next_multiple_of(KERNEL_PAGE_SIZE as _)
                .expect("read length fit within u32")
        } else {
            length
        };

        let read = Read {
            buffer: AVec::with_capacity(align, kernel_length as _),
            expected_length: length,
        };

        let (slot, read) = self.init(user_data, read);
        opcode::Read::new(fd, read.buffer.as_mut_ptr(), kernel_length)
            .offset(range.start)
            .build()
            .user_data(slot as u64)
    }
}

impl<'data, U> IoUringState<Write<'data>, U> {
    pub fn write(
        &mut self,
        user_data: U,
        fd: Fd,
        offset: u64,
        bytes: &'data [u8],
    ) -> squeue::Entry {
        let length = u32::try_from(bytes.len()).expect("write length fit within u32");
        let (slot, _) = self.init(user_data, Write { bytes });
        opcode::Write::new(fd, bytes.as_ptr(), length)
            .offset(offset)
            .build()
            .user_data(slot as u64)
    }
}

impl<Req, U> IoUringState<Req, U> {
    fn init(&mut self, user_data: U, request: Req) -> (usize, &mut Req) {
        let entry = self.requests.vacant_entry();
        let slot = entry.key();
        let pending = entry.insert(Pending { request, user_data });
        (slot, &mut pending.request)
    }

    fn complete(&mut self, slot: usize) -> io::Result<(U, Req)> {
        let pending = self.requests.try_remove(slot).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("request slot {slot} is empty"),
            )
        })?;

        let Pending { request, user_data } = pending;
        Ok((user_data, request))
    }
}

impl<Req, U> Drop for IoUringState<Req, U> {
    fn drop(&mut self) {
        debug_assert!(self.requests.is_empty());
    }
}

/// Validate that io_uring request completed successfully and extract response to return
pub trait Finalize {
    type Resp;
    fn finalize(self, result: u32) -> Self::Resp;
}

#[derive(Clone, Debug)]
pub struct Read {
    buffer: AVec<u8, RuntimeAlign>,
    expected_length: u32,
}

impl Finalize for Read {
    type Resp = AVec<u8, RuntimeAlign>;

    fn finalize(self, bytes_read: u32) -> Self::Resp {
        let Self {
            mut buffer,
            expected_length,
        } = self;

        assert_eq!(expected_length, bytes_read);

        assert!(buffer.capacity() >= bytes_read as _);
        unsafe { buffer.set_len(bytes_read as _) };
        buffer
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Write<'data> {
    bytes: &'data [u8],
}

impl<'data> Finalize for Write<'data> {
    type Resp = ();

    fn finalize(self, bytes_written: u32) {
        let Self { bytes } = self;
        assert_eq!(bytes.len(), bytes_written as usize);
    }
}
