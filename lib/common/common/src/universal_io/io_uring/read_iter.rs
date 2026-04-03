use std::iter;
use std::marker::PhantomData;

use ::io_uring::types::Fd;

use super::*;

pub struct IoUringReadIter<T: 'static, RequestId, I: Iterator> {
    ranges: iter::Peekable<I>,
    runtime: IoUringRuntime<'static, T, RequestId>,
    _phantom: PhantomData<*const ()>, // `!Send + !Sync`
}

impl<T, RequestId, I> IoUringReadIter<T, RequestId, I>
where
    T: bytemuck::Pod,
    I: Iterator<Item = (RequestId, Fd, bool, ReadRange)>,
{
    pub fn new(ranges: I) -> Result<Self> {
        let iter = Self {
            ranges: ranges.peekable(),
            runtime: IoUringRuntime::new()?,
            _phantom: PhantomData,
        };

        Ok(iter)
    }

    fn next_impl(&mut self) -> Result<Option<(RequestId, Vec<T>)>> {
        if self.runtime.completion_is_empty()
            && (self.ranges.peek().is_some() || self.runtime.in_progress > 0)
        {
            self.runtime.enqueue_while(|state| {
                let Some((id, fd, direct_io, range)) = self.ranges.next() else {
                    return Ok(None);
                };
                let entry = state.read(id, fd, range, direct_io)?;
                Ok(Some(entry))
            })?;

            self.runtime.submit_and_wait(1)?;
        }

        let next = self
            .runtime
            .completed()
            .next()
            .transpose()?
            .map(|(id, resp)| (id, resp.expect_read()));

        Ok(next)
    }
}

impl<T, RequestId, I> Iterator for IoUringReadIter<T, RequestId, I>
where
    T: bytemuck::Pod,
    I: Iterator<Item = (RequestId, Fd, bool, ReadRange)>,
{
    type Item = Result<(RequestId, Vec<T>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_impl().transpose()
    }
}
