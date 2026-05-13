use std::marker::PhantomData;
use std::ops::Range;

use bytemuck::TransparentWrapper;

use crate::aligned_buf::AlignedCow;
use crate::generic_consts::AccessPattern;
use crate::universal_io::Result;
use crate::universal_io::read::{UniversalReadPipeline, UserData};

/// Default implementation of [`UniversalReadPipeline`] for wrappers.
pub struct WrappedReadPipeline<'a, File, Inner> {
    inner: Inner,
    _phantom: PhantomData<&'a File>,
}

impl<'a, File, Inner, U> UniversalReadPipeline<'a, U> for WrappedReadPipeline<'a, File, Inner>
where
    File: TransparentWrapper<Inner::File> + 'a,
    Inner: UniversalReadPipeline<'a, U>,
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
    fn wait(&mut self) -> Result<Option<(U, AlignedCow<'a>)>> {
        self.inner.wait()
    }
}
