use std::marker::PhantomData;
use std::ops::Range;

use bytemuck::TransparentWrapper;

use crate::ext::aligned_vec::ACow;
use crate::generic_consts::AccessPattern;
use crate::universal_io::{BorrowedReadPipeline, OwnedReadPipeline, Result, UserData};

/// Default implementation of [`BorrowedReadPipeline`] for wrappers.
pub struct BorrowedWrappedReadPipeline<'a, File, Inner> {
    inner: Inner,
    _phantom: PhantomData<&'a File>,
}

impl<'a, File, Inner, U> BorrowedReadPipeline<'a, U>
    for BorrowedWrappedReadPipeline<'a, File, Inner>
where
    File: TransparentWrapper<Inner::File> + 'a,
    Inner: BorrowedReadPipeline<'a, U>,
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
    fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        file: &'a File,
        range: Range<u64>,
        align: usize,
    ) -> Result<()> {
        self.inner
            .schedule::<P>(user_data, File::peel_ref(file), range, align)
    }

    #[inline]
    fn wait(&mut self) -> Result<Option<(U, ACow<'a>)>> {
        self.inner.wait()
    }
}

/// Default implementation of [`OwnedReadPipeline`] for wrappers.
pub struct OwnedWrappedReadPipeline<File, Inner> {
    inner: Inner,
    _phantom: PhantomData<File>,
}

impl<File, Inner, U> OwnedReadPipeline<U> for OwnedWrappedReadPipeline<File, Inner>
where
    File: TransparentWrapper<Inner::File>,
    Inner: OwnedReadPipeline<U>,
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
    fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        range: Range<u64>,
        align: usize,
    ) -> Result<()> {
        self.inner.schedule::<P>(user_data, range, align)
    }

    #[inline]
    fn schedule_whole(&mut self, user_data: U) -> Result<()> {
        self.inner.schedule_whole(user_data)
    }

    #[inline]
    fn wait(&mut self) -> Result<Option<(U, ACow<'_>)>> {
        self.inner.wait()
    }
}
