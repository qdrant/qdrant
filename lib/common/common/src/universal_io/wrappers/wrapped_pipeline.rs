use std::borrow::Cow;
use std::marker::PhantomData;

use bytemuck::TransparentWrapper;

use crate::generic_consts::AccessPattern;
use crate::universal_io::read::{UniversalReadPipeline, UserData};
use crate::universal_io::{ReadRange, Result};

/// Default implementation of [`UniversalReadPipeline`] for wrappers.
pub struct WrappedReadPipeline<'a, File, Inner> {
    inner: Inner,
    _phantom: PhantomData<&'a File>,
}

impl<'a, File, Inner, T, U> UniversalReadPipeline<'a, T, U> for WrappedReadPipeline<'a, File, Inner>
where
    File: TransparentWrapper<Inner::File>,
    Inner: UniversalReadPipeline<'a, T, U>,
    T: bytemuck::Pod,
    U: UserData,
{
    type File = File;

    #[inline]
    fn new() -> Result<Self> {
        let wrapper = Self {
            inner: UniversalReadPipeline::new()?,
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
