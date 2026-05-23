use std::borrow::Cow;
use std::marker::PhantomData;

use bytemuck::TransparentWrapper;

use crate::generic_consts::AccessPattern;
use crate::universal_io::{
    BorrowedReadPipeline, Item, OwnedReadPipeline, ReadRange, Result, UserData,
};

/// Default implementation of [`BorrowedReadPipeline`] for wrappers.
pub struct BorrowedWrappedReadPipeline<'a, File, Inner> {
    inner: Inner,
    _phantom: PhantomData<&'a File>,
}

impl<'a, File, Inner, T, U> BorrowedReadPipeline<'a, T, U>
    for BorrowedWrappedReadPipeline<'a, File, Inner>
where
    File: TransparentWrapper<Inner::File>,
    Inner: BorrowedReadPipeline<'a, T, U>,
    T: Item,
    U: UserData,
{
    type File = File;

    #[inline]
    fn new() -> Result<Self> {
        let wrapper = Self {
            inner: BorrowedReadPipeline::new()?,
            _phantom: PhantomData,
        };

        Ok(wrapper)
    }

    #[inline]
    fn can_schedule(&mut self) -> bool {
        self.inner.can_schedule()
    }

    #[inline]
    fn schedule<P>(&mut self, user_data: U, file: &'a File, range: ReadRange) -> Result<()>
    where
        P: AccessPattern,
    {
        self.inner
            .schedule::<P>(user_data, File::peel_ref(file), range)
    }

    #[inline]
    fn wait(&mut self) -> Result<Option<(U, Cow<'a, [T]>)>> {
        self.inner.wait()
    }
}

/// Default implementation of [`OwnedReadPipeline`] for wrappers.
pub struct OwnedWrappedReadPipeline<File, Inner> {
    inner: Inner,
    _phantom: PhantomData<File>,
}

impl<File, Inner, T, U> OwnedReadPipeline<T, U> for OwnedWrappedReadPipeline<File, Inner>
where
    File: TransparentWrapper<Inner::File>,
    Inner: OwnedReadPipeline<T, U>,
    T: Item,
    U: UserData,
{
    type File = File;

    #[inline]
    fn new(file: File) -> Result<Self> {
        let wrapper = Self {
            inner: OwnedReadPipeline::new(File::peel(file))?,
            _phantom: PhantomData,
        };

        Ok(wrapper)
    }

    #[inline]
    fn can_schedule(&mut self) -> bool {
        self.inner.can_schedule()
    }

    #[inline]
    fn schedule<P>(&mut self, user_data: U, range: ReadRange) -> Result<()>
    where
        P: AccessPattern,
    {
        self.inner.schedule::<P>(user_data, range)
    }

    #[inline]
    fn wait(&mut self) -> Result<Option<(U, Cow<'_, [T]>)>> {
        self.inner.wait()
    }
}
