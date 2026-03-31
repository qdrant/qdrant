use std::borrow::Cow;
use std::collections::VecDeque;
use std::io;
use std::marker::PhantomData;

use ::io_uring::types::Fd;
use ahash::AHashMap;

use super::super::*;
use super::IoUringFile;
use super::pool::{self, IO_URING_QUEUE_LENGTH, IoUringGuard};
use super::runtime::{IoUringState, RequestId, io_error_context};

// ---------------------------------------------------------------------------
// FileResolver trait — abstracts single-file vs multi-file differences
// ---------------------------------------------------------------------------

pub(super) trait FileResolver<'a, T: bytemuck::Pod + 'static> {
    /// Element yielded by the caller's range iterator.
    type RangeItem;
    /// Extra data carried per in-flight request.  `()` for single-file,
    /// `FileIndex` for multi-file.
    type Meta: Copy;
    /// The item produced by the `Iterator` impl.
    type OutputItem;

    /// Map a range item to `(fd, uses_o_direct, range, meta)`.
    fn resolve(&self, item: Self::RangeItem) -> Result<(Fd, bool, ReadRange, Self::Meta)>;

    /// Record `meta` for an in-flight request (no-op when `Meta = ()`).
    fn track(&mut self, request_id: RequestId, meta: Self::Meta);

    /// Retrieve and remove `meta` for a completed request.
    fn untrack(&mut self, request_id: RequestId) -> Self::Meta;

    /// Remove tracking for an aborted request (error / drop path).
    fn untrack_abort(&mut self, request_id: RequestId);

    /// Build the final output item from index, meta, and data.
    fn into_output(idx: usize, meta: Self::Meta, data: Vec<T>) -> Self::OutputItem;
}

// ---------------------------------------------------------------------------
// SingleFileResolver
// ---------------------------------------------------------------------------

pub(super) struct SingleFileResolver<'a> {
    file: &'a IoUringFile,
}

impl<'a, T: bytemuck::Pod + 'static> FileResolver<'a, T> for SingleFileResolver<'a> {
    type RangeItem = ReadRange;
    type Meta = ();
    type OutputItem = (usize, Cow<'a, [T]>);

    #[inline(always)]
    fn resolve(&self, range: ReadRange) -> Result<(Fd, bool, ReadRange, ())> {
        Ok((self.file.fd(), self.file.uses_o_direct, range, ()))
    }

    #[inline(always)]
    fn track(&mut self, _request_id: RequestId, _meta: ()) {}

    #[inline(always)]
    fn untrack(&mut self, _request_id: RequestId) {}

    #[inline(always)]
    fn untrack_abort(&mut self, _request_id: RequestId) {}

    #[inline(always)]
    fn into_output(idx: usize, _meta: (), data: Vec<T>) -> Self::OutputItem {
        (idx, Cow::Owned(data))
    }
}

// ---------------------------------------------------------------------------
// MultiFileResolver
// ---------------------------------------------------------------------------

pub(super) struct MultiFileResolver<'a> {
    files: &'a [IoUringFile],
    file_indices: AHashMap<RequestId, FileIndex>,
}

impl<'a, T: bytemuck::Pod + 'static> FileResolver<'a, T> for MultiFileResolver<'a> {
    type RangeItem = (FileIndex, ReadRange);
    type Meta = FileIndex;
    type OutputItem = (usize, FileIndex, Cow<'a, [T]>);

    #[inline]
    fn resolve(
        &self,
        (file_index, range): (FileIndex, ReadRange),
    ) -> Result<(Fd, bool, ReadRange, FileIndex)> {
        let file = self
            .files
            .get(file_index)
            .ok_or(UniversalIoError::InvalidFileIndex {
                file_index,
                files: self.files.len(),
            })?;
        Ok((file.fd(), file.uses_o_direct, range, file_index))
    }

    #[inline]
    fn track(&mut self, request_id: RequestId, meta: FileIndex) {
        self.file_indices.insert(request_id, meta);
    }

    #[inline]
    fn untrack(&mut self, request_id: RequestId) -> FileIndex {
        self.file_indices
            .remove(&request_id)
            .expect("file index is tracked")
    }

    #[inline]
    fn untrack_abort(&mut self, request_id: RequestId) {
        self.file_indices.remove(&request_id);
    }

    #[inline]
    fn into_output(idx: usize, meta: FileIndex, data: Vec<T>) -> Self::OutputItem {
        (idx, meta, Cow::Owned(data))
    }
}

// ---------------------------------------------------------------------------
// Unified iterator
// ---------------------------------------------------------------------------

/// Lazy, pipelined iterator over io_uring read results.
///
/// Keeps the io_uring submission queue continuously full: on every [`next()`] call it
/// refills free queue slots with new reads before waiting for completions. This means
/// the kernel always has work queued, maximising I/O parallelism.
///
/// Parameterised over a [`FileResolver`] so that the same code handles both
/// single-file and multi-file reads with zero overhead for the single-file case.
pub(super) struct IoUringReadIterInner<
    'a,
    T: bytemuck::Pod + 'static,
    I: Iterator<Item = R::RangeItem>,
    R: FileResolver<'a, T>,
> {
    resolver: R,
    /// Exclusive `IoUring` instance taken from the pool — returned on drop.
    guard: IoUringGuard,
    ranges: std::iter::Enumerate<I>,
    buffer: VecDeque<(usize, R::Meta, Vec<T>)>,
    state: IoUringState<'static, T>,
    in_progress: usize,
    /// `!Send + !Sync` — the iterator is tied to the thread whose pool it uses.
    _not_send: PhantomData<*const ()>,
    _lifetime: PhantomData<&'a ()>,
}

/// Single-file read iterator.
pub(super) type IoUringReadIter<'a, T, I> = IoUringReadIterInner<'a, T, I, SingleFileResolver<'a>>;

/// Multi-file read iterator.
pub(super) type IoUringReadMultiIter<'a, T, I> =
    IoUringReadIterInner<'a, T, I, MultiFileResolver<'a>>;

// -- Constructors -----------------------------------------------------------

impl<'a, T: bytemuck::Pod + 'static, I: Iterator<Item = ReadRange>>
    IoUringReadIterInner<'a, T, I, SingleFileResolver<'a>>
{
    pub fn new(file: &'a IoUringFile, ranges: impl IntoIterator<IntoIter = I>) -> Result<Self> {
        Self::new_inner(SingleFileResolver { file }, ranges)
    }
}

impl<'a, T: bytemuck::Pod + 'static, I: Iterator<Item = (FileIndex, ReadRange)>>
    IoUringReadIterInner<'a, T, I, MultiFileResolver<'a>>
{
    pub fn new(files: &'a [IoUringFile], reads: impl IntoIterator<IntoIter = I>) -> Result<Self> {
        Self::new_inner(
            MultiFileResolver {
                files,
                file_indices: AHashMap::new(),
            },
            reads,
        )
    }
}

// -- Shared logic -----------------------------------------------------------

impl<'a, T: bytemuck::Pod + 'static, I: Iterator<Item = R::RangeItem>, R: FileResolver<'a, T>>
    IoUringReadIterInner<'a, T, I, R>
{
    fn new_inner(resolver: R, ranges: impl IntoIterator<IntoIter = I>) -> Result<Self> {
        let guard = pool::take_io_uring().map_err(UniversalIoError::IoUringNotSupported)?;
        Ok(Self {
            resolver,
            guard,
            ranges: ranges.into_iter().enumerate(),
            buffer: VecDeque::new(),
            state: IoUringState::new(),
            in_progress: 0,
            _not_send: PhantomData,
            _lifetime: PhantomData,
        })
    }

    /// Refill the submission queue, submit, and collect completions.
    ///
    /// Returns `true` if there is (or may be) more work, `false` if fully exhausted.
    fn step(&mut self) -> Result<bool> {
        let io_uring = self.guard.io_uring();

        // Fill every free slot in the submission queue with a new read.
        let mut newly_queued = 0usize;
        {
            let mut sqe = io_uring.submission();
            while self.in_progress + sqe.len() < IO_URING_QUEUE_LENGTH as usize {
                let Some((id, range_item)) = self.ranges.next() else {
                    break;
                };

                let (fd, uses_o_direct, range, meta) = self.resolver.resolve(range_item)?;
                self.resolver.track(id as RequestId, meta);

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

        // `submit_and_wait` may return before completions are available on
        // older kernels; retry until at least one completion is ready.
        while io_uring.completion().is_empty() {
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
                self.resolver.untrack_abort(id);
                return Err(io_error_context(
                    io::Error::from_raw_os_error(-result),
                    format!("io_uring operation {id} failed"),
                )
                .into());
            }

            let meta = self.resolver.untrack(id);

            let length = result as _;
            let resp = self.state.finalize(id, length)?;
            let items = resp.expect_read();
            self.buffer.push_back((id as usize, meta, items));
        }

        Ok(true)
    }
}

impl<'a, T: bytemuck::Pod + 'static, I: Iterator<Item = R::RangeItem>, R: FileResolver<'a, T>>
    Iterator for IoUringReadIterInner<'a, T, I, R>
{
    type Item = Result<R::OutputItem>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() {
            match self.step() {
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
                _ => {}
            }
        }

        let (idx, meta, data) = self.buffer.pop_front()?;
        Some(Ok(R::into_output(idx, meta, data)))
    }
}

impl<'a, T: bytemuck::Pod + 'static, I: Iterator<Item = R::RangeItem>, R: FileResolver<'a, T>> Drop
    for IoUringReadIterInner<'a, T, I, R>
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
                self.resolver.untrack_abort(id);
                if result < 0 {
                    self.state.abort(id);
                } else {
                    let _ = self.state.finalize(id, result as _);
                }
            }
        }
    }
}
