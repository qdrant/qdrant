use std::borrow::Cow;
use std::iter;
use std::marker::PhantomData;

use super::*;

pub struct IoUringReadIter<'a, T: 'static, I: Iterator> {
    file: &'a IoUringFile,
    ranges: iter::Peekable<iter::Enumerate<I>>,
    runtime: IoUringRuntime<'static, T>,
    _phantom: PhantomData<*const ()>, // `!Send + !Sync`
}

impl<'a, T, I> IoUringReadIter<'a, T, I>
where
    T: bytemuck::Pod,
    I: Iterator<Item = ReadRange>,
{
    pub fn new(file: &'a IoUringFile, ranges: I) -> Result<Self> {
        let iter = Self {
            file,
            ranges: ranges.enumerate().peekable(),
            runtime: IoUringRuntime::new()?,
            _phantom: PhantomData,
        };

        Ok(iter)
    }

    fn next_impl(&mut self) -> Result<Option<(usize, Cow<'a, [T]>)>> {
        if self.runtime.completion_is_empty()
            && (self.ranges.peek().is_some() || self.runtime.in_progress > 0)
        {
            self.runtime.enqueue_while(|state| {
                let Some((id, range)) = self.ranges.next() else {
                    return Ok(None);
                };

                let entry = state.read(id as _, self.file.fd(), range, 0, self.file.direct_io)?;
                Ok(Some(entry))
            })?;

            self.runtime.submit_and_wait(1)?;
        }

        let next = self
            .runtime
            .completed()
            .next()
            .transpose()?
            .map(|(id, resp)| {
                let id = id as _;
                let (_, items) = resp.expect_read();

                (id, Cow::from(items))
            });

        Ok(next)
    }
}

impl<'a, T, I> Iterator for IoUringReadIter<'a, T, I>
where
    T: bytemuck::Pod,
    I: Iterator<Item = ReadRange>,
{
    type Item = Result<(usize, Cow<'a, [T]>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_impl().transpose()
    }
}

pub struct IoUringReadMultiIter<'a, T: 'static, I: Iterator> {
    files: &'a [IoUringFile],
    ranges: iter::Peekable<iter::Enumerate<I>>,
    runtime: IoUringRuntime<'static, T>,
    _phantom: PhantomData<*const ()>, // `!Send + !Sync`
}

impl<'a, T, I> IoUringReadMultiIter<'a, T, I>
where
    T: bytemuck::Pod,
    I: Iterator<Item = (FileIndex, ReadRange)>,
{
    pub fn new(files: &'a [IoUringFile], ranges: I) -> Result<Self> {
        let iter = Self {
            files,
            ranges: ranges.enumerate().peekable(),
            runtime: IoUringRuntime::new()?,
            _phantom: PhantomData,
        };

        Ok(iter)
    }

    #[expect(clippy::type_complexity)]
    fn next_impl(&mut self) -> Result<Option<(usize, FileIndex, Cow<'a, [T]>)>> {
        if self.runtime.completion_is_empty()
            && (self.ranges.peek().is_some() || self.runtime.in_progress > 0)
        {
            self.runtime.enqueue_while(|state| {
                let Some((id, (file_index, range))) = self.ranges.next() else {
                    return Ok(None);
                };

                let file =
                    self.files
                        .get(file_index)
                        .ok_or(UniversalIoError::InvalidFileIndex {
                            file_index,
                            files: self.files.len(),
                        })?;

                let entry = state.read(id as _, file.fd(), range, file_index, file.direct_io)?;
                Ok(Some(entry))
            })?;

            self.runtime.submit_and_wait(1)?;
        }

        let next = self
            .runtime
            .completed()
            .next()
            .transpose()?
            .map(|(id, resp)| {
                let id = id as _;
                let (file_index, items) = resp.expect_read();

                (id, file_index, Cow::from(items))
            });

        Ok(next)
    }
}

impl<'a, T, I> Iterator for IoUringReadMultiIter<'a, T, I>
where
    T: bytemuck::Pod,
    I: Iterator<Item = (FileIndex, ReadRange)>,
{
    type Item = Result<(usize, FileIndex, Cow<'a, [T]>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_impl().transpose()
    }
}
