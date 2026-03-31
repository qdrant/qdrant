use std::borrow::Cow;
use std::collections::VecDeque;
use std::io;
use std::marker::PhantomData;

use super::super::*;
use super::IoUringFile;
use super::pool::{self, IO_URING_QUEUE_LENGTH, IoUringGuard};
use super::runtime::{IoUringState, io_error_context};

/// Lazy, pipelined iterator over io_uring read results.
///
/// Keeps the io_uring submission queue continuously full: on every [`next()`] call it
/// refills free queue slots with new reads before waiting for completions. This means
/// the kernel always has work queued, maximising I/O parallelism.
///
/// The iterator holds its own [`IoUringState`] (request tracking / buffers) and
/// `in_progress` counter that persist across calls. On each [`next()`] it temporarily
/// borrows the thread-local [`IoUring`] ring to submit and reap.
pub(super) struct IoUringReadIter<'a, T: bytemuck::Pod + 'static, I: Iterator<Item = ReadRange>> {
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
    pub fn new(file: &'a IoUringFile, ranges: impl IntoIterator<IntoIter = I>) -> Result<Self> {
        let guard = pool::take_io_uring().map_err(UniversalIoError::IoUringNotSupported)?;
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

        // `submit_and_wait` may return before completions are available on
        // older kernels; retry until at least one completion is ready.
        while wait && io_uring.completion().is_empty() {
            self.in_progress += io_uring
                .submit_and_wait(1)
                .map_err(|err| io_error_context(err, "failed to submit io_uring operations"))?;
        }

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
