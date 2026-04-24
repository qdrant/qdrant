use std::io;
use std::mem::MaybeUninit;

use ::io_uring::types::Fd;
use ::io_uring::{opcode, squeue};
use slab::Slab;

use super::*;
use crate::maybe_uninit;

pub struct IoUringRuntime<'data, T, Meta = u64> {
    pub io_uring: IoUringGuard,
    pub state: IoUringState<'data, T, Meta>,
    pub in_progress: usize,
}

impl<'data, T, Meta> IoUringRuntime<'data, T, Meta> {
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
        F: FnMut(&mut IoUringState<'data, T, Meta>) -> Result<Option<squeue::Entry>>,
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

    pub fn completion_is_empty(&mut self) -> bool {
        self.io_uring.completion().is_empty()
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

    pub fn completed(&mut self) -> impl Iterator<Item = io::Result<(Meta, IoUringResponse<T>)>> {
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
            let (meta, resp) = self.state.finalize(slot, length)?;
            Ok((meta, resp))
        })
    }
}

impl<'data, T, Meta> Drop for IoUringRuntime<'data, T, Meta> {
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
pub struct IoUringState<'data, T, Meta> {
    requests: Slab<(Meta, IoUringRequest<'data, T>)>,
}

impl<'data, T, Meta> IoUringState<'data, T, Meta> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            requests: Slab::with_capacity(capacity),
        }
    }

    /// Allocates `Vec<MaybeUninit<T>>`, reinterprets it as `Vec<MaybeUninit<u8>>`, and stores the byte buffer
    /// so the kernel writes into correctly aligned memory for `T`.
    pub fn read(&mut self, meta: Meta, fd: Fd, range: ReadRange, direct_io: bool) -> squeue::Entry
    where
        T: bytemuck::Pod,
    {
        let ReadRange {
            byte_offset,
            length,
        } = range;

        let mut items: Vec<MaybeUninit<T>> = Vec::with_capacity(length as _);
        items.resize_with(length as _, || MaybeUninit::uninit());

        let (slot, req) = self.init(meta, IoUringRequest::Read { items, direct_io });
        let items = req.expect_read();

        let bytes_ptr = items.as_mut_ptr().cast();
        let byte_length = length * size_of::<T>() as u64;
        let byte_length = u32::try_from(byte_length).expect("read buffer length fit within u32");
        opcode::Read::new(fd, bytes_ptr, byte_length)
            .offset(byte_offset)
            .build()
            .user_data(slot as u64)
    }

    pub fn write(
        &mut self,
        meta: Meta,
        fd: Fd,
        byte_offset: u64,
        items: &'data [T],
    ) -> squeue::Entry
    where
        T: bytemuck::Pod,
    {
        let (slot, req) = self.init(meta, IoUringRequest::Write(items));
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
        meta: Meta,
        req: IoUringRequest<'data, T>,
    ) -> (usize, &mut IoUringRequest<'data, T>) {
        let entry = self.requests.vacant_entry();
        let slot = entry.key();
        let (_, req) = entry.insert((meta, req));
        (slot, req)
    }

    pub fn finalize(
        &mut self,
        slot: usize,
        byte_length: u32,
    ) -> io::Result<(Meta, IoUringResponse<T>)> {
        let (meta, req) = self
            .requests
            .try_remove(slot)
            .ok_or_else(|| io::Error::other(format!("request in slot {slot} does not exist")))?;

        let byte_length = byte_length as usize;

        let resp = match req {
            IoUringRequest::Read {
                mut items,
                direct_io,
            } => {
                if direct_io {
                    let item_length = byte_length / size_of::<T>();
                    debug_assert!(item_length <= items.len());

                    items.truncate(item_length);
                } else {
                    assert_eq!(size_of_val(items.as_slice()), byte_length);
                }

                let items: Vec<T> = unsafe { maybe_uninit::assume_init_vec(items) };
                IoUringResponse::Read(items)
            }

            IoUringRequest::Write(items) => {
                assert_eq!(size_of_val(items), byte_length);
                IoUringResponse::Write
            }
        };

        Ok((meta, resp))
    }

    pub fn abort(&mut self, slot: usize) {
        self.requests.try_remove(slot);
    }
}

impl<'data, T, Meta> Drop for IoUringState<'data, T, Meta> {
    fn drop(&mut self) {
        debug_assert!(self.requests.is_empty());
    }
}

#[derive(Debug)]
pub enum IoUringRequest<'data, T> {
    Read {
        items: Vec<MaybeUninit<T>>,
        direct_io: bool,
    },

    Write(&'data [T]),
}

impl<'data, T> IoUringRequest<'data, T> {
    pub fn expect_read(&mut self) -> &mut Vec<MaybeUninit<T>> {
        #[expect(clippy::match_wildcard_for_single_variants)]
        match self {
            IoUringRequest::Read { items, .. } => items,
            _ => panic!(),
        }
    }

    pub fn expect_write(&self) -> &'data [T] {
        #[expect(clippy::match_wildcard_for_single_variants)]
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
