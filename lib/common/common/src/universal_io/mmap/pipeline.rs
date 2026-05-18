use std::borrow::Cow;

use super::{MmapFile, read};
use crate::generic_consts::AccessPattern;
use crate::universal_io::{ReadRange, Result, UniversalIoError, UniversalReadPipeline, UserData};

pub struct MmapReadPipeline<'file, T, U> {
    result: Option<(U, &'file [T])>,
}

impl<'file, T, U> UniversalReadPipeline<'file, T, U> for MmapReadPipeline<'file, T, U>
where
    T: bytemuck::Pod,
    U: UserData,
{
    type File = MmapFile;

    fn new() -> Result<Self> {
        Ok(Self { result: None })
    }

    fn can_schedule(&mut self) -> bool {
        self.result.is_none()
    }

    fn schedule<P>(&mut self, user_data: U, file: &'file MmapFile, range: ReadRange) -> Result<()>
    where
        P: AccessPattern,
    {
        if self.result.is_some() {
            return Err(UniversalIoError::QueueIsFull);
        }

        self.result = Some((user_data, read(file.as_bytes::<P>(), range)?));
        Ok(())
    }

    fn wait(&mut self) -> Result<Option<(U, Cow<'file, [T]>)>> {
        let result = self.result.take();
        Ok(result.map(|(user_data, items)| (user_data, Cow::Borrowed(items))))
    }
}
