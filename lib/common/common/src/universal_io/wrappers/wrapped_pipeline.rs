use std::borrow::Cow;

use bytemuck::TransparentWrapper;

use crate::generic_consts::AccessPattern;
use crate::universal_io::read::UniversalReadPipeline;
use crate::universal_io::{ReadRange, Result, UniversalRead};

/// Default implementation of [`UniversalReadPipeline`] for wrappers.
pub struct WrappedReadPipeline<'a, T, S, P, Meta>(S::ReadPipeline<'a, P, Meta>)
where
    T: Copy + 'static,
    S: UniversalRead<T> + 'a,
    P: AccessPattern;

impl<'a, T, Outer, S, P, Meta> UniversalReadPipeline<'a, T, Outer, Meta>
    for WrappedReadPipeline<'a, T, S, P, Meta>
where
    T: Copy + 'static,
    Outer: UniversalRead<T> + TransparentWrapper<S>,
    S: UniversalRead<T>,
    P: AccessPattern,
{
    #[inline]
    fn new() -> Result<Self> {
        Ok(Self(UniversalReadPipeline::new()?))
    }

    #[inline]
    fn can_schedule(&mut self) -> bool {
        self.0.can_schedule()
    }

    #[inline]
    fn schedule(&mut self, meta: Meta, file: &'a Outer, range: ReadRange) -> Result<()> {
        self.0.schedule(meta, Outer::peel_ref(file), range)
    }

    #[inline]
    fn wait(&mut self) -> Result<Option<(Meta, Cow<'a, [T]>)>> {
        self.0.wait()
    }
}
