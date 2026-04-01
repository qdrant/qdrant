use std::borrow::Cow;
use std::iter;
use std::marker::PhantomData;

use ahash::HashMapExt as _;

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
                let Some((idx, range)) = self.ranges.next() else {
                    return Ok(None);
                };

                let entry = state.read(idx as _, self.file.fd(), range, self.file.uses_o_direct)?;

                Ok(Some(entry))
            })?;

            self.runtime.submit_and_wait(1)?;
        }

        let next = self
            .runtime
            .completed()
            .next()
            .transpose()?
            .map(|(idx, resp)| (idx as _, Cow::from(resp.expect_read())));

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
    file_indexes: ahash::HashMap<usize, FileIndex>,
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
            file_indexes: ahash::HashMap::with_capacity(IO_URING_QUEUE_LENGTH as _),
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
                let Some((idx, (file_index, range))) = self.ranges.next() else {
                    return Ok(None);
                };

                let file =
                    self.files
                        .get(file_index)
                        .ok_or(UniversalIoError::InvalidFileIndex {
                            file_index,
                            files: self.files.len(),
                        })?;

                self.file_indexes.insert(idx, file_index);

                let entry = state.read(idx as _, file.fd(), range, file.uses_o_direct)?;
                Ok(Some(entry))
            })?;

            self.runtime.submit_and_wait(1)?;
        }

        let next = self
            .runtime
            .completed()
            .next()
            .transpose()?
            .map(|(idx, resp)| {
                let idx = idx as _;
                let file_index = self.file_indexes.remove(&idx).expect("file index tracked");
                let items = Cow::from(resp.expect_read());

                (idx, file_index, items)
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
