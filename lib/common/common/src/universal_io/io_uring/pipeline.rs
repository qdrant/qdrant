use std::mem::ManuallyDrop;
use std::ops::Range;

use ::io_uring::types::Fd;

use super::pool::IO_URING_QUEUE_LENGTH;
use super::{IoUringFile, IoUringRuntime};
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::{AccessPattern, Sequential};
use crate::universal_io::{
    BorrowedReadPipeline, OwnedReadPipeline, Result, UniversalIoError, UniversalRead, UserData,
};

pub struct BorrowedIoUringPipeline<'file, U>
where
    U: UserData,
{
    inner: IoUringPipelineInner<'file, U>,
}

impl<'file, U> BorrowedReadPipeline<'file, U> for BorrowedIoUringPipeline<'file, U>
where
    U: UserData,
{
    type File = IoUringFile;

    fn new() -> Result<Self> {
        Ok(Self {
            inner: IoUringPipelineInner::new()?,
        })
    }

    fn can_schedule(&mut self) -> bool {
        self.inner.can_schedule()
    }

    fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        file: &'file IoUringFile,
        range: Range<u64>,
        align: usize,
    ) -> Result<()> {
        // Safety: `file.fd()` doesn't outlive the inner pipeline because of
        // `'file` lifetime.
        unsafe {
            self.inner
                .schedule(user_data, file.fd(), file.direct_io, range, align)
        }
    }

    fn wait(&mut self) -> Result<Option<(U, ACow<'file>)>> {
        self.inner.wait()
    }
}

pub struct OwnedIoUringPipeline<U>
where
    U: UserData,
{
    file: ManuallyDrop<IoUringFile>,
    inner: ManuallyDrop<IoUringPipelineInner<'static, U>>,
}

impl<U> OwnedReadPipeline<U> for OwnedIoUringPipeline<U>
where
    U: UserData,
{
    type File = IoUringFile;

    fn new(file: IoUringFile) -> Result<Self> {
        let inner = IoUringPipelineInner::new()?;
        Ok(Self {
            file: ManuallyDrop::new(file),
            inner: ManuallyDrop::new(inner),
        })
    }

    fn can_schedule(&mut self) -> bool {
        self.inner.can_schedule()
    }

    fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        range: Range<u64>,
        align: usize,
    ) -> Result<()> {
        // Safety: `self.file.fd()` doesn't outlive the inner pipeline because
        // of explicit drop order in `impl Drop`.
        unsafe {
            self.inner
                .schedule(user_data, self.file.fd(), self.file.direct_io, range, align)
        }
    }

    fn schedule_whole(&mut self, user_data: U, from: u64) -> Result<()> {
        let eof = self.file.len::<u8>()?;
        if from >= eof {
            return Ok(());
        }
        self.schedule::<Sequential>(user_data, from..eof, 1)
    }

    fn wait(&mut self) -> Result<Option<(U, ACow<'_>)>> {
        self.inner.wait()
    }

    fn into_inner(self) -> IoUringFile {
        // Wrap in `ManuallyDrop` so our own `Drop` impl doesn't run; we take
        // both fields out by hand instead.
        let mut this = ManuallyDrop::new(self);

        let Self { file, inner } = &mut *this;

        // SAFETY: `this` is `ManuallyDrop`, so its destructor never runs and
        // each field is taken exactly once.
        let file = unsafe { ManuallyDrop::take(file) };
        let inner = unsafe { ManuallyDrop::take(inner) };
        drop(inner);
        file
    }
}

impl<U> Drop for OwnedIoUringPipeline<U>
where
    U: UserData,
{
    fn drop(&mut self) {
        // Drop `inner` before `file`.
        let Self { file, inner } = self;
        let file: IoUringFile = unsafe { ManuallyDrop::take(file) };
        let inner: IoUringPipelineInner<_> = unsafe { ManuallyDrop::take(inner) };
        drop(inner);
        drop(file);
    }
}

struct IoUringPipelineInner<'file, U>
where
    U: UserData,
{
    runtime: IoUringRuntime<'file, U>,
}

impl<'file, U> IoUringPipelineInner<'file, U>
where
    U: UserData,
{
    fn new() -> Result<Self> {
        Ok(Self {
            runtime: IoUringRuntime::new()?,
        })
    }

    fn can_schedule(&mut self) -> bool {
        let squeue = self.runtime.io_uring.submission();
        self.runtime.in_progress + squeue.len() < IO_URING_QUEUE_LENGTH as _
    }

    /// # Safety
    ///
    /// The caller must ensure that the `fd` will not outlive the pipeline.
    unsafe fn schedule(
        &mut self,
        user_data: U,
        fd: Fd,
        direct_io: bool,
        range: Range<u64>,
        align: usize,
    ) -> Result<()> {
        let mut squeue = self.runtime.io_uring.submission();

        if self.runtime.in_progress + squeue.len() >= IO_URING_QUEUE_LENGTH as _ {
            return Err(UniversalIoError::QueueIsFull);
        }

        let entry = self
            .runtime
            .state
            .read(user_data, fd, range, align, direct_io);

        unsafe {
            squeue.push(&entry).expect("submission queue is not full");
        }

        Ok(())
    }

    fn wait(&mut self) -> Result<Option<(U, ACow<'file>)>> {
        let next = self.runtime.completed().next();

        let enqueued = self.runtime.enqueued();

        if next.is_some() && enqueued > 0 {
            self.runtime.submit_and_wait(0)?;
        } else if next.is_none() && enqueued + self.runtime.in_progress > 0 {
            self.runtime.submit_and_wait(1)?;
        }

        let Some(result) = next.or_else(|| self.runtime.completed().next()) else {
            return Ok(None);
        };

        let (user_data, resp) = result?;
        Ok(Some((user_data, ACow::Owned(resp.expect_read()))))
    }
}
