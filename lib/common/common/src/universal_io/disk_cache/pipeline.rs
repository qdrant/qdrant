use std::borrow::Cow;

use super::CachedSlice;
use crate::generic_consts::{AccessPattern, Random};
use crate::universal_io::{
    ReadRange, Result, UniversalIoError, UniversalRead, UniversalReadPipeline, UserData,
};

pub struct DiskCacheReadPipeline<'file, T, U>
where
    T: bytemuck::Pod,
    U: UserData,
{
    result: Option<(U, Cow<'file, [T]>)>,
}

impl<'file, T, U> UniversalReadPipeline<'file, T, U> for DiskCacheReadPipeline<'file, T, U>
where
    T: bytemuck::Pod,
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
        range: ReadRange,
    ) -> Result<()>
    where
        P: AccessPattern,
    {
        if self.result.is_some() {
            return Err(UniversalIoError::QueueIsFull);
        }

        self.result = Some((user_data, file.read::<Random, T>(range)?));
        Ok(())
    }

    fn wait(&mut self) -> Result<Option<(U, Cow<'file, [T]>)>> {
        Ok(self.result.take())
    }
}
