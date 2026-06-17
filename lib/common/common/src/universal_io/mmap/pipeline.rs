use std::ops::Range;

use super::{MmapFile, read_bytes};
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::AccessPattern;
use crate::universal_io::{ReadPipeline, Result, UniversalIoError, UserData};

pub struct MmapReadPipeline<'file, U> {
    result: Option<(U, &'file [u8])>,
}

impl<'file, U> ReadPipeline<'file, U> for MmapReadPipeline<'file, U>
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
