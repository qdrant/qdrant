use std::marker::PhantomData;
use std::ops::Range;

use super::{IoUringFile, IoUringReadRuntime, KERNEL_PAGE_SIZE};
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::{AccessPattern, Sequential};
use crate::universal_io::{ReadPipeline, Result, UniversalIoError, UniversalRead as _, UserData};

pub struct IoUringPipeline<'file, U>
where
    U: UserData,
{
    runtime: IoUringReadRuntime<U>,
    _phantom: PhantomData<&'file ()>,
}

impl<'file, U: UserData> ReadPipeline<'file, U> for IoUringPipeline<'file, U> {
    type File = IoUringFile;

    fn new() -> Result<Self> {
        let pipeline = Self {
            runtime: IoUringReadRuntime::new()?,
            _phantom: PhantomData,
        };

        Ok(pipeline)
    }

    fn can_schedule(&mut self) -> bool {
        self.runtime.can_schedule()
    }

    fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        file: &'file IoUringFile,
        range: Range<u64>,
        align: usize,
    ) -> Result<()> {
        // IoUringPipeline is bound by 'file lifetime, so it can never outlive file
        // or hold fd longer than file is valid

        if !self.can_schedule() {
            return Err(UniversalIoError::QueueIsFull);
        }

        let entry = self
            .runtime
            .state()
            .read(user_data, file.fd(), file.direct_io, range, align);

        self.runtime.enqueue(entry)?;

        Ok(())
    }

    fn schedule_whole(&mut self, user_data: U, file: &'file Self::File, from: u64) -> Result<()> {
        let eof = file.len::<u8>()?;

        if from >= eof {
            return Ok(());
        }

        let align = if file.direct_io { KERNEL_PAGE_SIZE } else { 1 };
        self.schedule::<Sequential>(user_data, file, from..eof, align)
    }

    fn wait(&mut self) -> Result<Option<(U, ACow<'file>)>> {
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
