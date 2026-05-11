use std::io;
use std::mem::MaybeUninit;

use ::io_uring::types::Fd;
use ::io_uring::{opcode, squeue};
use slab::Slab;

use super::*;
use crate::maybe_uninit;
use crate::universal_io::read::UserData;

const KERNEL_PAGE_SIZE: u64 = 4096; // 4 kB

#[derive(Debug, Clone)]
#[repr(C, align(4096))]
pub struct PageAlignedBytes([MaybeUninit<u8>; KERNEL_PAGE_SIZE as usize]);

impl PageAlignedBytes {
    const fn uninit() -> Self {
        Self([MaybeUninit::uninit(); KERNEL_PAGE_SIZE as usize])
    }

    fn as_maybe_bytes(slice: &[Self]) -> &[MaybeUninit<u8>] {
        unsafe {
            std::slice::from_raw_parts(
                slice.as_ptr().cast::<MaybeUninit<u8>>(),
                slice.len() * KERNEL_PAGE_SIZE as usize,
            )
        }
    }
}

pub struct IoUringRuntime<'data, T: bytemuck::Pod, U: UserData = u64> {
    pub io_uring: IoUringGuard,
    pub state: IoUringState<'data, T, U>,
    pub in_progress: usize,
}

impl<'data, T, U> IoUringRuntime<'data, T, U>
where
    T: bytemuck::Pod,
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
        F: FnMut(&mut IoUringState<'data, T, U>) -> Result<Option<squeue::Entry>>,
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

    pub fn completed(&mut self) -> impl Iterator<Item = io::Result<(U, IoUringResponse<T>)>> {
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

impl<'data, T, U> Drop for IoUringRuntime<'data, T, U>
where
    T: bytemuck::Pod,
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
pub struct IoUringState<'data, T, U> {
    requests: Slab<(U, IoUringRequest<'data, T>)>,
}

impl<'data, T, U> IoUringState<'data, T, U>
where
    T: bytemuck::Pod,
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
        range: ReadRange,
        o_direct: bool,
    ) -> squeue::Entry {
        if o_direct {
            self.read_o_direct(user_data, fd, range)
        } else {
            self.read_exact(user_data, fd, range)
        }
    }

    /// Allocates `Vec<MaybeUninit<T>>`, reinterprets it as `Vec<MaybeUninit<u8>>`, and stores the byte buffer
    /// so the kernel writes into correctly aligned memory for `T`.
    fn read_exact(&mut self, user_data: U, fd: Fd, range: ReadRange) -> squeue::Entry {
        let ReadRange {
            byte_offset,
            length,
        } = range;

        // Size the buffer exactly to the number of items to read
        let items: Vec<MaybeUninit<T>> = vec![MaybeUninit::uninit(); length as _];
        let (slot, req) = self.init(user_data, IoUringRequest::Read { items });
        let items = req.expect_read();

        let bytes_ptr = items.as_mut_ptr().cast();
        let byte_length = length * size_of::<T>() as u64;
        let byte_length = u32::try_from(byte_length).expect("read buffer length fit within u32");

        opcode::Read::new(fd, bytes_ptr, byte_length)
            .offset(byte_offset)
            .build()
            .user_data(slot as u64)
    }

    /// Allocates a `Vec<MaybeUninit<u8>>` aligned to 4kB.
    fn read_o_direct(&mut self, user_data: U, fd: Fd, range: ReadRange) -> squeue::Entry {
        let ReadRange {
            byte_offset,
            length,
        } = range;

        // Make sure read buffer is kernel-page aligned
        let page_byte_offset = byte_offset & !(KERNEL_PAGE_SIZE - 1); // page-aligned byte offset
        let inner_byte_offset = byte_offset - page_byte_offset; // offset within buffer where the request starts

        let bytes_len =
            (inner_byte_offset + length * size_of::<T>() as u64).next_multiple_of(KERNEL_PAGE_SIZE);

        let num_pages = bytes_len / KERNEL_PAGE_SIZE;

        let buffer: Vec<PageAlignedBytes> = vec![PageAlignedBytes::uninit(); num_pages as _];

        let (slot, req) = self.init(
            user_data,
            IoUringRequest::ODirectRead {
                buffer,
                inner_byte_offset: inner_byte_offset as usize,
                items_len: length as usize,
            },
        );
        let buffer = req.expect_o_direct_read();

        let bytes_len = u32::try_from(bytes_len).expect("read buffer length fit within u32");
        let bytes_ptr = buffer.as_mut_ptr().cast();

        opcode::Read::new(fd, bytes_ptr, bytes_len)
            .offset(page_byte_offset)
            .build()
            .user_data(slot as u64)
    }

    pub fn write(
        &mut self,
        user_data: U,
        fd: Fd,
        byte_offset: u64,
        items: &'data [T],
    ) -> squeue::Entry {
        let (slot, req) = self.init(user_data, IoUringRequest::Write(items));
        let items = req.expect_write();

        let bytes: &[u8] = bytemuck::cast_slice(items);
        let byte_length = u32::try_from(bytes.len()).expect("write buffer length fit within u32");
        opcode::Write::new(fd, bytes.as_ptr(), byte_length)
            .offset(byte_offset)
            .build()
            .user_data(slot as u64)
    }

    fn init(
        &mut self,
        user_data: U,
        req: IoUringRequest<'data, T>,
    ) -> (usize, &mut IoUringRequest<'data, T>) {
        let entry = self.requests.vacant_entry();
        let slot = entry.key();
        let (_, req) = entry.insert((user_data, req));
        (slot, req)
    }

    pub fn finalize(
        &mut self,
        slot: usize,
        byte_length: u32,
    ) -> io::Result<(U, IoUringResponse<T>)> {
        let (user_data, req) = self
            .requests
            .try_remove(slot)
            .ok_or_else(|| io::Error::other(format!("request in slot {slot} does not exist")))?;

        let byte_length = byte_length as usize;

        let resp = match req {
            IoUringRequest::Read { items } => {
                assert_eq!(size_of_val(items.as_slice()), byte_length);

                let items: Vec<T> = unsafe { maybe_uninit::assume_init_vec(items) };
                IoUringResponse::Read(items)
            }
            IoUringRequest::ODirectRead {
                buffer,
                inner_byte_offset,
                items_len,
            } => {
                let buffer_bytes = PageAlignedBytes::as_maybe_bytes(&buffer);

                // We need to return a `T`-aligned Vec

                // TODO(perf): Currently, we are allocating 2 Vecs: a page-aligned vec, and then the `T`-aligned one.
                //             In theory we can use `opcode::ReadFixed` to use preregistered buffers so that we only
                //             allocate one extra buffer per read. Not done for now since it complicates implementation.

                //
                // buffer_bytes
                // │     ┌────┬────items┬────┬────┐      byte_length    │
                // ├─────┤ 1  │ 2  │ 3  │ 4  │ 5  ├───────────>|────────┤
                // │     └────┴────┴────┴────┴────┘                     │
                //       ^
                //   inner_byte_offset

                let initialized_bytes = unsafe { buffer_bytes[..byte_length].assume_init_ref() };

                // Make sure there are at least the requested num of items
                let avail_items = byte_length.saturating_sub(inner_byte_offset) / size_of::<T>();
                assert!(
                    items_len <= avail_items,
                    "expected at least {items_len} items, got {avail_items}"
                );

                // Cast requested range into &[T]
                let items_range = inner_byte_offset..inner_byte_offset + items_len * size_of::<T>();
                let items_bytes = &initialized_bytes[items_range];
                let items_slice = bytemuck::cast_slice(items_bytes);

                // Copy into new Vec
                let items: Vec<T> = items_slice.to_vec();

                IoUringResponse::Read(items)
            }

            IoUringRequest::Write(items) => {
                assert_eq!(size_of_val(items), byte_length);
                IoUringResponse::Write
            }
        };

        Ok((user_data, resp))
    }

    pub fn abort(&mut self, slot: usize) {
        self.requests.try_remove(slot);
    }
}

impl<'data, T, U> Drop for IoUringState<'data, T, U> {
    fn drop(&mut self) {
        debug_assert!(self.requests.is_empty());
    }
}

#[derive(Debug)]
pub enum IoUringRequest<'data, T> {
    Read {
        items: Vec<MaybeUninit<T>>,
    },

    ODirectRead {
        buffer: Vec<PageAlignedBytes>,
        inner_byte_offset: usize,
        items_len: usize,
    },

    Write(&'data [T]),
}

impl<'data, T> IoUringRequest<'data, T> {
    pub fn expect_read(&mut self) -> &mut Vec<MaybeUninit<T>> {
        match self {
            IoUringRequest::Read { items } => items,
            _ => panic!(),
        }
    }

    pub fn expect_o_direct_read(&mut self) -> &mut Vec<PageAlignedBytes> {
        match self {
            IoUringRequest::ODirectRead {
                buffer,
                inner_byte_offset: _,
                items_len: _,
            } => buffer,
            _ => panic!(),
        }
    }

    pub fn expect_write(&self) -> &'data [T] {
        match self {
            IoUringRequest::Write(items) => items,
            _ => panic!(),
        }
    }
}

#[derive(Debug)]
pub enum IoUringResponse<T> {
    Read(Vec<T>),
    Write,
}

impl<T> IoUringResponse<T> {
    pub fn expect_read(self) -> Vec<T> {
        #[expect(clippy::match_wildcard_for_single_variants)]
        match self {
            Self::Read(items) => items,
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
