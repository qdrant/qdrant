use std::iter;
use std::marker::PhantomData;

use ::io_uring::types::Fd;

use super::*;

pub struct IoUringReadIter<T: 'static, Meta, I: Iterator> {
    ranges: iter::Peekable<I>,
    runtime: IoUringRuntime<'static, T, Meta>,
    _phantom: PhantomData<*const ()>, // `!Send + !Sync`
}

impl<T, Meta, I> IoUringReadIter<T, Meta, I>
where
    T: bytemuck::Pod,
    I: Iterator<Item = (Meta, Fd, bool, ReadRange)>,
{
    pub fn new(ranges: I) -> Result<Self> {
        let iter = Self {
            ranges: ranges.peekable(),
            runtime: IoUringRuntime::new()?,
            _phantom: PhantomData,
        };

        Ok(iter)
    }

    fn next_impl(&mut self) -> Result<Option<(Meta, Vec<T>)>> {
        if self.runtime.completion_is_empty()
            && (self.ranges.peek().is_some() || self.runtime.in_progress > 0)
        {
            self.runtime.enqueue_while(|state| {
                let Some((meta, fd, direct_io, range)) = self.ranges.next() else {
                    return Ok(None);
                };
                let entry = state.read(meta, fd, range, direct_io);
                Ok(Some(entry))
            })?;

            self.runtime.submit_and_wait(1)?;
        }

        let next = self
            .runtime
            .completed()
            .next()
            .transpose()?
            .map(|(meta, resp)| (meta, resp.expect_read()));

        Ok(next)
    }
}

impl<T, Meta, I> Iterator for IoUringReadIter<T, Meta, I>
where
    T: bytemuck::Pod,
    I: Iterator<Item = (Meta, Fd, bool, ReadRange)>,
{
    type Item = Result<(Meta, Vec<T>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_impl().transpose()
    }
}
