use std::collections::hash_map;
use std::io;
use std::mem::{self, MaybeUninit};

use ::io_uring::types::Fd;
use ::io_uring::{opcode, squeue};
use ahash::AHashMap;

use super::super::*;
use super::pool::{self, IO_URING_QUEUE_LENGTH, IoUringGuard};
use crate::maybe_uninit::assume_init_vec;

pub struct IoUringRuntime<'data, T> {
    io_uring: IoUringGuard,
    pub state: IoUringState<'data, T>,
    pub in_progress: usize,
}

impl<'data, T> IoUringRuntime<'data, T> {
    pub fn new() -> Result<Self> {
        let rt = Self {
            io_uring: pool::take_io_uring()?,
            state: IoUringState::new(),
            in_progress: 0,
        };

        Ok(rt)
    }

    pub fn enqueue<F>(&mut self, mut entries: F) -> Result<()>
    where
        F: FnMut(&mut IoUringState<'data, T>) -> Result<Option<squeue::Entry>>,
    {
        let mut sqe = self.io_uring.io_uring().submission();

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
        let enqueued = self.io_uring.io_uring().submission().len();

        debug_assert!(
            want == 0 || enqueued + self.in_progress >= want,
            "io_uring would block: \
             requested to wait for {want} operations to complete, \
             but not enough operations are enqueued or in-progress"
        );

        self.submit_and_wait_retry_early_wakeup(want)?;

        let remaining = self.io_uring.io_uring().submission().len();
        debug_assert!(enqueued > remaining);
        debug_assert_eq!(remaining, 0);

        self.in_progress += enqueued - remaining;

        Ok(())
    }

    fn submit_and_wait_retry_early_wakeup(&mut self, want: usize) -> io::Result<()> {
        self.submit_and_wait_retry_eintr(want)?;

        while want > 0 && self.io_uring.io_uring().completion().is_empty() {
            self.submit_and_wait_retry_eintr(want)?;
        }

        Ok(())
    }

    fn submit_and_wait_retry_eintr(&mut self, want: usize) -> io::Result<()> {
        let result = loop {
            match self.io_uring.io_uring().submit_and_wait(want) {
                Err(err) if err.kind() == io::ErrorKind::Interrupted => (),
                res => break res,
            }
        };

        result.map_err(|err| io_error_context(err, "failed to submit io_uring operations"))?;
        Ok(())
    }

    pub fn completed(&mut self) -> impl Iterator<Item = io::Result<(u64, IoUringResponse<T>)>> {
        self.io_uring.io_uring().completion().map(|entry| {
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

impl<'data, T> Drop for IoUringRuntime<'data, T> {
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
pub(super) struct IoUringState<'data, T> {
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
            .ok_or_else(|| io::Error::other(format!("request {id} does not exist")))?;

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

pub(super) type RequestId = u64;

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
pub(super) enum IoUringResponse<T> {
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

pub(crate) fn io_error_context(err: io::Error, context: impl Into<String>) -> io::Error {
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
