use std::borrow::Cow;
use std::marker::PhantomData;

use bytemuck::TransparentWrapper;

use crate::generic_consts::AccessPattern;
use crate::universal_io::read::UniversalReadPipeline;
use crate::universal_io::{ReadRange, Result, UniversalRead};

/// Default implementation of [`UniversalReadPipeline`] for wrappers.
pub struct WrappedReadPipeline<'a, T, Outer, S, P, Meta>
where
    T: Copy + 'static,
    S: UniversalRead<T> + 'a,
    P: AccessPattern,
{
    inner: S::ReadPipeline<'a, P, Meta>,
    _phantom: PhantomData<Outer>,
}

impl<'a, T, Outer, S, P, Meta> UniversalReadPipeline<'a, T, Meta>
    for WrappedReadPipeline<'a, T, Outer, S, P, Meta>
where
    T: Copy + 'static,
    Outer: UniversalRead<T> + TransparentWrapper<S> + 'a,
    S: UniversalRead<T>,
    P: AccessPattern,
{
    type File = Outer;

    #[inline]
    fn new() -> Result<Self> {
        Ok(Self {
            inner: UniversalReadPipeline::new()?,
            _phantom: PhantomData,
        })
    }

    #[inline]
    fn can_schedule(&mut self) -> bool {
        self.inner.can_schedule()
    }

    #[inline]
    fn schedule<P1>(&mut self, meta: Meta, file: &'a Outer, range: ReadRange) -> Result<()>
    where
        P1: AccessPattern,
    {
        self.inner
            .schedule::<P1>(meta, Outer::peel_ref(file), range)
    }

    #[inline]
    fn wait(&mut self) -> Result<Option<(Meta, Cow<'a, [T]>)>> {
        self.inner.wait()
    }
}
