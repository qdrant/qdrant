use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ops::Range;

use ::io_uring::types::Fd;

use super::{IoUringFile, IoUringReadRuntime, KERNEL_PAGE_SIZE};
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::{AccessPattern, Sequential};
use crate::universal_io::{
    BorrowedReadPipeline, OwnedReadPipeline, Result, UniversalIoError, UniversalRead, UserData,
};

pub struct BorrowedIoUringPipeline<'file, U>
where
    U: UserData,
{
    inner: IoUringPipelineInner<U>,
    _phantom: PhantomData<&'file ()>,
}

impl<'file, U: UserData> BorrowedReadPipeline<'file, U> for BorrowedIoUringPipeline<'file, U> {
    type File = IoUringFile;

    fn new() -> Result<Self> {
        let inner = IoUringPipelineInner::new()?;

        let pipeline = Self {
            inner,
            _phantom: PhantomData,
        };

        Ok(pipeline)
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
        // SAFETY: `fd` does not outlive `inner` pipeline, because `Self` is bound by `'file` lifetime

        unsafe {
            self.inner
                .schedule(user_data, file.fd(), file.direct_io, range, align)
        }
    }

    fn wait(&mut self) -> Result<Option<(U, ACow<'file>)>> {
        self.inner.wait()
    }
}

pub struct OwnedIoUringPipeline<U: UserData> {
    inner: ManuallyDrop<IoUringPipelineInner<U>>,
    file: ManuallyDrop<IoUringFile>,
}

impl<U: UserData> OwnedReadPipeline<U> for OwnedIoUringPipeline<U> {
    type File = IoUringFile;

    fn new(file: IoUringFile) -> Result<Self> {
        let inner = IoUringPipelineInner::new()?;

        let pipeline = Self {
            inner: ManuallyDrop::new(inner),
            file: ManuallyDrop::new(file),
        };

        Ok(pipeline)
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
        // SAFETY: `fd` does not outlive `inner` pipeline, because `inner` is always dropped before `file`

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

        let align = if self.file.direct_io {
            KERNEL_PAGE_SIZE
        } else {
            1
        };

        self.schedule::<Sequential>(user_data, from..eof, align)
    }

    fn wait(&mut self) -> Result<Option<(U, ACow<'_>)>> {
        self.inner.wait()
    }

    #[expect(
        clippy::let_and_return,
        reason = "better readability around unsafe code"
    )]
    fn into_inner(self) -> IoUringFile {
        // Wrap `self` in `ManuallyDrop`, so `Drop` doesn't run at the end of `into_inner` scope
        let mut this = ManuallyDrop::new(self);

        // Drop `inner` during `destructure`, then return `file`
        let file = unsafe { this.destructure() };
        file
    }
}

impl<U> OwnedIoUringPipeline<U> {
    /// Destructure the pipeline. Drop `inner` and return `file`.
    ///
    /// # Safety:
    ///
    /// Must only be called once. `self` is uninitilized after `destructure` call,
    /// so caller must prevent `Drop` from running and ensure `self` is not accessed
    /// after the call.
    unsafe fn destructure(&mut self) -> IoUringFile {
        // `inner` might borrow `file`, so we must drop it before returning `file`.
        //
        // Take `file` and `inner` out of `ManuallyDrop`, so that we can rely on drop
        // at the end of scope or panic. Default drop order is *reverse* of declaration,
        // so we must take `file` *before* `inner`.

        let Self { inner, file } = self;

        let file = unsafe { ManuallyDrop::take(file) };
        let inner = unsafe { ManuallyDrop::take(inner) };

        drop(inner);
        file
    }
}

impl<U> Drop for OwnedIoUringPipeline<U> {
    fn drop(&mut self) {
        // Drop `inner` during `destructure`, then drop `file` explicitly
        let file = unsafe { self.destructure() };
        drop(file);
    }
}

struct IoUringPipelineInner<U: UserData> {
    runtime: IoUringReadRuntime<U>,
}

impl<U: UserData> IoUringPipelineInner<U> {
    fn new() -> Result<Self> {
        let runtime = IoUringReadRuntime::new()?;
        Ok(Self { runtime })
    }

    fn can_schedule(&mut self) -> bool {
        self.runtime.can_schedule()
    }

    /// # Safety
    ///
    /// The caller must ensure that `fd` will not outlive the pipeline.
    unsafe fn schedule(
        &mut self,
        user_data: U,
        fd: Fd,
        direct_io: bool,
        range: Range<u64>,
        align: usize,
    ) -> Result<()> {
        if !self.can_schedule() {
            return Err(UniversalIoError::QueueIsFull);
        }

        let entry = self
            .runtime
            .state()
            .read(user_data, fd, direct_io, range, align);

        self.runtime.enqueue(entry)?;

        Ok(())
    }

    fn wait(&mut self) -> Result<Option<(U, ACow<'static>)>> {
        let next = self.runtime.completed().next();

        let enqueued = self.runtime.enqueued();

        if next.is_some() && enqueued > 0 {
            self.runtime.submit_and_wait(0)?;
        } else if next.is_none() && enqueued + self.runtime.in_progress() > 0 {
            self.runtime.submit_and_wait(1)?;
        }

        let Some(result) = next.or_else(|| self.runtime.completed().next()) else {
            return Ok(None);
        };

        let (user_data, buffer) = result?;
        Ok(Some((user_data, ACow::Owned(buffer))))
    }
}
