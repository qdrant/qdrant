use std::borrow::Cow;

use super::{MmapFile, MmapFileInner, read};
use crate::generic_consts::{AccessPattern, Random, Sequential};
use crate::universal_io::{
    BorrowedReadPipeline, OwnedReadPipeline, ReadRange, Result, UniversalIoError, UserData,
};

pub struct BorrowedMmapReadPipeline<'file, T, U> {
    result: Option<(U, &'file [T])>,
}

impl<'file, T, U> BorrowedReadPipeline<'file, T, U> for BorrowedMmapReadPipeline<'file, T, U>
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

        self.result = Some((user_data, read(file.inner.as_bytes::<P>(), range)?));
        Ok(())
    }

    fn wait(&mut self) -> Result<Option<(U, Cow<'file, [T]>)>> {
        let result = self.result.take();
        Ok(result.map(|(user_data, items)| (user_data, Cow::Borrowed(items))))
    }
}

pub struct OwnedMmapReadPipeline<T, U> {
    inner: MmapFileInner,
    pending: Option<(U, ReadRange, bool)>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, U> OwnedReadPipeline<T, U> for OwnedMmapReadPipeline<T, U>
where
    T: bytemuck::Pod,
    U: UserData,
{
    type File = MmapFile;

    fn new(file: &MmapFile) -> Result<Self> {
        Ok(Self {
            inner: file.inner.clone(),
            pending: None,
            _phantom: std::marker::PhantomData,
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
        self.pending = Some((user_data, range, P::IS_SEQUENTIAL));
        Ok(())
    }

    fn wait(&mut self) -> Result<Option<(U, Cow<'_, [T]>)>> {
        let Some((user_data, range, is_sequential)) = self.pending.take() else {
            return Ok(None);
        };
        let bytes = if is_sequential {
            self.inner.as_bytes::<Sequential>()
        } else {
            self.inner.as_bytes::<Random>()
        };
        Ok(Some((user_data, Cow::Borrowed(read::<T>(bytes, range)?))))
    }
}
