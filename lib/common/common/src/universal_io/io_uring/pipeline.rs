use std::borrow::Cow;

use super::pool::IO_URING_QUEUE_LENGTH;
use super::{IoUringFile, IoUringRuntime};
use crate::generic_consts::AccessPattern;
use crate::universal_io::{ReadRange, Result, UniversalIoError, UniversalReadPipeline, UserData};

pub struct IoUringPipeline<'file, T, U>
where
    T: bytemuck::Pod,
    U: UserData,
{
    runtime: IoUringRuntime<'file, T, U>,
}

impl<'file, T, U> UniversalReadPipeline<'file, T, U> for IoUringPipeline<'file, T, U>
where
    T: bytemuck::Pod,
    U: UserData,
{
    type File = IoUringFile;

    fn new() -> Result<Self> {
        Ok(Self {
            runtime: IoUringRuntime::new()?,
        })
    }

    fn can_schedule(&mut self) -> bool {
        let squeue = self.runtime.io_uring.submission();
        self.runtime.in_progress + squeue.len() < IO_URING_QUEUE_LENGTH as _
    }

    fn schedule<P>(
        &mut self,
        user_data: U,
        file: &'file IoUringFile,
        range: ReadRange,
    ) -> Result<()>
    where
        P: AccessPattern,
    {
        let mut squeue = self.runtime.io_uring.submission();

        if self.runtime.in_progress + squeue.len() >= IO_URING_QUEUE_LENGTH as _ {
            return Err(UniversalIoError::QueueIsFull);
        }

        let entry = self
            .runtime
            .state
            .read(user_data, file.fd(), range, file.direct_io);

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
