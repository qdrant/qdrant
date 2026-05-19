use std::borrow::Cow;
use std::mem::ManuallyDrop;

use ::io_uring::types::Fd;

use super::pool::IO_URING_QUEUE_LENGTH;
use super::{IoUringFile, IoUringRuntime};
use crate::generic_consts::AccessPattern;
use crate::universal_io::{
    BorrowedReadPipeline, Item, OwnedReadPipeline, ReadRange, Result, UniversalIoError, UserData,
};

pub struct BorrowedIoUringPipeline<'file, T, U>
where
    T: Item,
    U: UserData,
{
    inner: IoUringPipelineInner<'file, T, U>,
}

impl<'file, T, U> BorrowedReadPipeline<'file, T, U> for BorrowedIoUringPipeline<'file, T, U>
where
    T: Item,
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
        range: ReadRange,
    ) -> Result<()> {
        // Safety: `file.fd()` doesn't outlive the inner pipeline because of
        // `'file` lifetime.
        unsafe {
            self.inner
                .schedule(user_data, file.fd(), file.direct_io, range)
        }
    }

    fn wait(&mut self) -> Result<Option<(U, Cow<'file, [T]>)>> {
        self.inner.wait()
    }
}

pub struct OwnedIoUringPipeline<T, U>
where
    T: Item,
    U: UserData,
{
    file: ManuallyDrop<IoUringFile>,
    inner: ManuallyDrop<IoUringPipelineInner<'static, T, U>>,
}

impl<T, U> std::fmt::Debug for OwnedIoUringPipeline<T, U>
where
    T: bytemuck::Pod,
    U: UserData,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedIoUringPipeline")
            .field("file", &self.file)
            // .field("inner", &self.inner)
            .finish()
    }
}

impl<T, U> OwnedReadPipeline<T, U> for OwnedIoUringPipeline<T, U>
where
    T: Item,
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

    fn schedule<P>(&mut self, user_data: U, range: ReadRange) -> Result<()>
    where
        P: AccessPattern,
    {
        // Safety: `self.file.fd()` doesn't outlive the inner pipeline because
        // of explicit drop order in `impl Drop`.
        unsafe {
            self.inner
                .schedule(user_data, self.file.fd(), self.file.direct_io, range)
        }
    }

    fn wait(&mut self) -> Result<Option<(U, Cow<'_, [T]>)>> {
        self.inner.wait()
    }
}

impl<T, U> Drop for OwnedIoUringPipeline<T, U>
where
    T: Item,
    U: UserData,
{
    fn drop(&mut self) {
        // Drop `inner` before `file`.
        let Self { file, inner } = self;
        let file: IoUringFile = unsafe { ManuallyDrop::take(file) };
        let inner: IoUringPipelineInner<_, _> = unsafe { ManuallyDrop::take(inner) };
        drop(inner);
        drop(file);
    }
}

struct IoUringPipelineInner<'file, T, U>
where
    T: Item,
    U: UserData,
{
    runtime: IoUringRuntime<'file, T, U>,
}

impl<'file, T, U> IoUringPipelineInner<'file, T, U>
where
    T: Item,
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
        range: ReadRange,
    ) -> Result<()> {
        let mut squeue = self.runtime.io_uring.submission();

        if self.runtime.in_progress + squeue.len() >= IO_URING_QUEUE_LENGTH as _ {
            return Err(UniversalIoError::QueueIsFull);
        }

        let entry = self.runtime.state.read(user_data, fd, range, direct_io);

        unsafe {
            squeue.push(&entry).expect("submission queue is not full");
        }

        Ok(())
    }

    fn wait(&mut self) -> Result<Option<(U, Cow<'file, [T]>)>> {
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
        Ok(Some((user_data, Cow::Owned(resp.expect_read()))))
    }
}
