use std::ops::Range;

use super::CachedSlice;
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::{AccessPattern, Sequential};
use crate::universal_io::{ReadPipeline, Result, UniversalIoError, UserData};

pub struct DiskCacheReadPipeline<'file, U> {
    result: Option<(U, ACow<'file>)>,
}

impl<'file, U> ReadPipeline<'file, U> for DiskCacheReadPipeline<'file, U>
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

    fn schedule_whole(&mut self, user_data: U, file: &'file Self::File, from: u64) -> Result<()> {
        let eof = file.len::<u8>() as u64;
        self.schedule::<Sequential>(user_data, file, from..eof, 1)
    }

    fn wait(&mut self) -> Result<Option<(U, ACow<'file>)>> {
        Ok(self.result.take())
    }
}
