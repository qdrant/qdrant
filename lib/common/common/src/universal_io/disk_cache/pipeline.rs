use std::ops::Range;

use super::CachedSlice;
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::{AccessPattern, Sequential};
use crate::universal_io::{
    BorrowedReadPipeline, OwnedReadPipeline, Result, UniversalIoError, UserData,
};

pub struct BorrowedDiskCacheReadPipeline<'file, U>
where
    U: UserData,
{
    result: Option<(U, ACow<'file>)>,
}

impl<'file, U> BorrowedReadPipeline<'file, U> for BorrowedDiskCacheReadPipeline<'file, U>
where
    U: UserData,
{
    type File = CachedSlice;

    fn new() -> Result<Self> {
        Ok(Self { result: None })
    }

    fn can_schedule(&mut self) -> bool {
        self.result.is_none()
    }

    fn schedule<P>(
        &mut self,
        user_data: U,
        file: &'file CachedSlice,
        range: Range<u64>,
        align: usize,
    ) -> Result<()>
    where
        P: AccessPattern,
    {
        if self.result.is_some() {
            return Err(UniversalIoError::QueueIsFull);
        }

        let byte_range = range.start as usize..range.end as usize;
        self.result = Some((user_data, file.get_range_bytes(byte_range, align)?));
        Ok(())
    }

    fn wait(&mut self) -> Result<Option<(U, ACow<'file>)>> {
        Ok(self.result.take())
    }
}

pub struct OwnedDiskCacheReadPipeline<U> {
    file: CachedSlice,
    pending: Option<(U, Range<u64>, usize)>,
}

impl<U> OwnedReadPipeline<U> for OwnedDiskCacheReadPipeline<U>
where
    U: UserData,
{
    type File = CachedSlice;

    fn new(file: CachedSlice) -> Result<Self> {
        Ok(Self {
            file,
            pending: None,
        })
    }

    fn can_schedule(&mut self) -> bool {
        self.pending.is_none()
    }

    fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        range: Range<u64>,
        align: usize,
    ) -> Result<()> {
        if self.pending.is_some() {
            return Err(UniversalIoError::QueueIsFull);
        }

        // FIXME: This is a temporary stub implementation.
        self.pending = Some((user_data, range, align));
        Ok(())
    }

    fn schedule_whole(&mut self, user_data: U, from: u64) -> Result<()> {
        let eof = self.file.len::<u8>() as u64;
        self.schedule::<Sequential>(user_data, from..eof, 1)
    }

    fn wait(&mut self) -> Result<Option<(U, ACow<'_>)>> {
        let Some((user_data, range, align)) = self.pending.take() else {
            return Ok(None);
        };
        let start = usize::try_from(range.start).expect("range.start is within usize");
        let end = usize::try_from(range.end).expect("range.end is within usize");
        let bytes = self.file.get_range_bytes(start..end, align)?;
        Ok(Some((user_data, bytes)))
    }
}
