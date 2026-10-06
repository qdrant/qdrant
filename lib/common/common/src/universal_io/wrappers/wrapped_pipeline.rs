use std::marker::PhantomData;
use std::ops::Range;

use bytemuck::TransparentWrapper;

use crate::counter::hardware_counter::HardwareCounterCell;
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::AccessPattern;
use crate::universal_io::{ReadPipeline, UioResult, UserData};

/// Default [`ReadPipeline`] implementation for transparent wrappers
pub struct WrappedReadPipeline<File, Inner> {
    inner: Inner,
    _phantom: PhantomData<fn() -> File>,
}

impl<'file, File, Inner, U> ReadPipeline<'file, U> for WrappedReadPipeline<File, Inner>
where
    File: TransparentWrapper<Inner::File> + 'file,
    Inner: ReadPipeline<'file, U>,
    U: UserData,
{
    type File = File;

    #[inline]
    fn new() -> UioResult<Self> {
        let wrapper = Self {
            inner: Inner::new()?,
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
        file: &'file File,
        range: Range<u64>,
        align: usize,
        hw_counter: &HardwareCounterCell,
    ) -> UioResult<()> {
        self.inner
            .schedule::<P>(user_data, File::peel_ref(file), range, align, hw_counter)
    }

    fn schedule_whole(
        &mut self,
        user_data: U,
        file: &'file Self::File,
        from: u64,
        hw_counter: &HardwareCounterCell,
    ) -> UioResult<()> {
        self.inner
            .schedule_whole(user_data, File::peel_ref(file), from, hw_counter)
    }

    #[inline]
    fn wait(&mut self, hw_counter: &HardwareCounterCell) -> UioResult<Option<(U, ACow<'file>)>> {
        self.inner.wait(hw_counter)
    }
}
