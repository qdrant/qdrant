use std::ops::Range;

use super::{MmapFile, read_bytes};
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::{AccessPattern, Random, Sequential};
use crate::universal_io::{
    BorrowedReadPipeline, OwnedReadPipeline, Result, UniversalIoError, UniversalRead, UserData,
};

pub struct BorrowedMmapReadPipeline<'file, U> {
    result: Option<(U, &'file [u8])>,
}

impl<'file, U> BorrowedReadPipeline<'file, U> for BorrowedMmapReadPipeline<'file, U>
where
    U: UserData,
{
    type File = MmapFile;

    fn new() -> Result<Self> {
        Ok(Self { result: None })
    }

    fn can_schedule(&mut self) -> bool {
        self.result.is_none()
    }

    fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        file: &'file MmapFile,
        range: Range<u64>,
        _align: usize,
    ) -> Result<()> {
        if self.result.is_some() {
            return Err(UniversalIoError::QueueIsFull);
        }

        self.result = Some((user_data, read_bytes(file.as_bytes::<P>(), range)?));
        Ok(())
    }

    fn wait(&mut self) -> Result<Option<(U, ACow<'file>)>> {
        let result = self.result.take();
        Ok(result.map(|(user_data, bytes)| (user_data, ACow::Borrowed(bytes))))
    }
}

#[derive(Debug)]
pub struct OwnedMmapReadPipeline<U> {
    file: MmapFile,
    pending: Option<(U, Range<u64>, bool)>,
}

impl<U> OwnedReadPipeline<U> for OwnedMmapReadPipeline<U>
where
    U: UserData,
{
    type File = MmapFile;

    fn new(file: MmapFile) -> Result<Self> {
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
        _align: usize,
    ) -> Result<()> {
        if self.pending.is_some() {
            return Err(UniversalIoError::QueueIsFull);
        }
        self.pending = Some((user_data, range, P::IS_SEQUENTIAL));
        Ok(())
    }

    fn schedule_whole(&mut self, user_data: U) -> Result<()> {
        let length = self.file.len::<u8>()?;
        self.schedule::<Sequential>(user_data, 0..length, 1)
    }

    fn wait(&mut self) -> Result<Option<(U, ACow<'_>)>> {
        let Some((user_data, range, is_sequential)) = self.pending.take() else {
            return Ok(None);
        };
        let bytes = if is_sequential {
            self.file.as_bytes::<Sequential>()
        } else {
            self.file.as_bytes::<Random>()
        };
        let slice = read_bytes(bytes, range)?;
        Ok(Some((user_data, ACow::Borrowed(slice))))
    }
}
