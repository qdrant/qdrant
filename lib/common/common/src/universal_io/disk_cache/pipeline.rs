use std::borrow::Cow;

use super::CachedSlice;
use crate::generic_consts::{AccessPattern, Random};
use crate::universal_io::{
    BorrowedReadPipeline, Item, OwnedReadPipeline, ReadRange, Result, UniversalIoError,
    UniversalRead, UserData,
};

pub struct BorrowedDiskCacheReadPipeline<'file, T, U>
where
    T: Item,
    U: UserData,
{
    result: Option<(U, Cow<'file, [T]>)>,
}

impl<'file, T, U> BorrowedReadPipeline<'file, T, U> for BorrowedDiskCacheReadPipeline<'file, T, U>
where
    T: Item,
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

        // FIXME: This is a temporary stub implementation.
        self.result = Some((user_data, file.read::<Random, T>(range)?));
        Ok(())
    }

    fn wait(&mut self) -> Result<Option<(U, Cow<'file, [T]>)>> {
        Ok(self.result.take())
    }
}

pub struct OwnedDiskCacheReadPipeline<T, U> {
    file: CachedSlice,
    pending: Option<(U, ReadRange)>,
    phantom: std::marker::PhantomData<T>,
}

impl<T, U> OwnedReadPipeline<T, U> for OwnedDiskCacheReadPipeline<T, U>
where
    T: Item,
    U: UserData,
{
    type File = CachedSlice;

    fn new(file: CachedSlice) -> Result<Self> {
        Ok(Self {
            file,
            pending: None,
            phantom: std::marker::PhantomData,
        })
    }

    fn can_schedule(&mut self) -> bool {
        self.pending.is_none()
    }

    fn schedule<P>(&mut self, user_data: U, range: ReadRange) -> Result<()>
    where
        P: AccessPattern,
    {
        if self.pending.is_some() {
            return Err(UniversalIoError::QueueIsFull);
        }

        // FIXME: This is a temporary stub implementation.
        self.pending = Some((user_data, range));
        Ok(())
    }

    fn wait(&mut self) -> Result<Option<(U, Cow<'_, [T]>)>> {
        let Some((user_data, range)) = self.pending.take() else {
            return Ok(None);
        };
        let items = self.file.read::<Random, T>(range)?;
        Ok(Some((user_data, items)))
    }
}
