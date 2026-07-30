use std::ops::Range;

use super::{MmapFile, read_bytes};
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::{AccessPattern, Sequential};
use crate::universal_io::{ReadPipeline, UioResult, UniversalIoError, UserData};

pub struct MmapReadPipeline<'file, U> {
    result: Option<(U, &'file [u8])>,
}

impl<'file, U> ReadPipeline<'file, U> for MmapReadPipeline<'file, U>
where
    U: UserData,
{
    type File = MmapFile;

    fn new() -> UioResult<Self> {
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
    ) -> UioResult<()> {
        if self.result.is_some() {
            return Err(UniversalIoError::QueueIsFull);
        }

        self.result = Some((user_data, read_bytes(file.as_bytes::<P>(), range)?));
        Ok(())
    }

    fn schedule_whole(
        &mut self,
        user_data: U,
        file: &'file Self::File,
        from: u64,
    ) -> UioResult<()> {
        let eof = file.len as u64;
        self.schedule::<Sequential>(user_data, file, from..eof, 1)
    }

    fn wait(&mut self) -> UioResult<Option<(U, ACow<'file>)>> {
        let result = self.result.take();
        Ok(result.map(|(user_data, bytes)| (user_data, ACow::Borrowed(bytes))))
    }
}
