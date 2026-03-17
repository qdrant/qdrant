#![allow(mismatched_lifetime_syntaxes)]

use std::marker::PhantomData;

use fallible_iterator::FallibleIterator;

use super::*;
use crate::generic_consts::{Sequential, SubmissionOrder};

/// A bounded view into a [`UniversalRead`] source.
///
/// Prefer [`my_try_fold`](Self::my_try_fold) / [`my_try_for_each`](Self::my_try_for_each)
/// over [`iter`](Self::iter) for bulk operations, as they read the entire range
/// in a single batch.
pub struct UniversalSlice<'a, T: Copy + 'static, S: UniversalRead<T>> {
    pub(super) source: &'a S,
    pub(super) range: ReadRange,
    pub(super) _phantom: PhantomData<T>,
}

// Manual impls to avoid `S: Copy/Clone` bounds from derive.
impl<T: Copy + 'static, S: UniversalRead<T>> Clone for UniversalSlice<'_, T, S> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T: Copy + 'static, S: UniversalRead<T>> Copy for UniversalSlice<'_, T, S> {}

impl<'a, T: Copy + 'static, S: UniversalRead<T>> UniversalSlice<'a, T, S> {
    /// Narrow this slice to a sub-range.
    pub fn range(self, range: ReadRange) -> Result<Self> {
        let end = range.byte_offset + range.length;
        if end > self.range.byte_offset + self.range.length {
            return Err(UniversalIoError::OutOfBounds {
                start: range.byte_offset,
                end,
                elements: 0, // TODO
            });
        }
        Ok(Self {
            source: self.source,
            range: ReadRange {
                byte_offset: self.range.byte_offset + range.byte_offset,
                length: range.length,
            },
            _phantom: PhantomData,
        })
    }

    /// Fold over all elements in order.
    ///
    /// Elements are always processed in order, even though `read_batch` may
    /// invoke its callback out of order (e.g. io_uring).
    pub fn my_fold<B>(&self, init: B, mut f: impl FnMut(B, T) -> B) -> Result<B> {
        const CHUNK: u64 = 32;
        let mut buf = [std::mem::MaybeUninit::<T>::uninit(); CHUNK as usize];
        let mut acc = init;
        for chunk in self.range.iter_chunks(CHUNK) {
            let n = chunk.length as usize;
            self.source
                .read_batch::<Sequential, SubmissionOrder, UniversalIoError>(
                    chunk.iter_chunks(1),
                    |idx, data| {
                        buf[idx].write(data[0]);
                        Ok(())
                    },
                )?;
            for item in &buf[..n] {
                acc = f(acc, unsafe { item.assume_init() });
            }
        }
        Ok(acc)
    }

    /// Execute a closure for each element.
    pub fn my_for_each(&self, mut f: impl FnMut(T)) -> Result<()> {
        self.my_fold((), |(), item| f(item))
    }

    /// Filter-map elements into a collection that implements `Default + Extend`.
    pub fn filter_map_collect<C, U>(&self, mut f: impl FnMut(T) -> Option<U>) -> Result<C>
    where
        C: Default + Extend<U>,
    {
        self.my_fold(C::default(), |mut acc, item| {
            acc.extend(f(item));
            acc
        })
    }

    /// Element-by-element iterator. Prefer [`my_try_fold`](Self::my_try_fold) for bulk ops.
    pub fn iter(&self) -> UniversalIteratorResult<'a, T, S> {
        UniversalIteratorResult {
            source: self.source,
            range: self.range,
            _phantom: PhantomData,
        }
    }

    /// Element-by-element iterator that panics on read errors.
    /// TODO(uio): Remove this. It's an temporary escape hatch while migrating to uio.
    pub fn iter_unwrap(&self) -> UniversalIteratorUnwrap<'a, T, S> {
        UniversalIteratorUnwrap {
            source: self.source,
            range: self.range,
            _phantom: PhantomData,
        }
    }

    pub fn iter2(&self) -> SliceFallibleIterator<'a, T, S> {
        SliceFallibleIterator {
            source: self.source,
            range: self.range,
            _phantom: PhantomData,
        }
    }
}

/// Element-by-element iterator over a [`UniversalSlice`].
pub struct UniversalIteratorResult<'a, T: Copy + 'static, S: UniversalRead<T>> {
    source: &'a S,
    range: ReadRange,
    _phantom: PhantomData<T>,
}

impl<'a, T: Copy + 'static, S: UniversalRead<T>> Iterator for UniversalIteratorResult<'a, T, S> {
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.range.length == 0 {
            return None;
        }
        let item = self.source.read::<Sequential>(ReadRange {
            byte_offset: self.range.byte_offset,
            length: 1,
        });
        self.range.byte_offset += 1;
        self.range.length -= 1;
        Some(item.map(|cow| cow[0]))
    }
}

/// Element-by-element iterator that panics on read errors.
/// TODO(uio): Remove this. It's an temporary escape hatch while migrating to uio.
pub struct UniversalIteratorUnwrap<'a, T: Copy + 'static, S: UniversalRead<T>> {
    source: &'a S,
    range: ReadRange,
    _phantom: PhantomData<T>,
}

impl<'a, T: Copy + 'static, S: UniversalRead<T>> Iterator for UniversalIteratorUnwrap<'a, T, S> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.range.length == 0 {
            return None;
        }
        let item = self
            .source
            .read::<Sequential>(ReadRange {
                byte_offset: self.range.byte_offset,
                length: 1,
            })
            .unwrap();
        self.range.byte_offset += 1;
        self.range.length -= 1;
        Some(item[0])
    }
}

pub struct SliceFallibleIterator<'a, T: Copy + 'static, S: UniversalRead<T>> {
    source: &'a S,
    range: ReadRange,
    _phantom: PhantomData<T>,
}

impl<'a, T: Copy + 'static, S: UniversalRead<T>> FallibleIterator
    for SliceFallibleIterator<'a, T, S>
{
    type Item = T;
    type Error = UniversalIoError;

    fn next(&mut self) -> Result<Option<T>> {
        if self.range.length == 0 {
            return Ok(None);
        }
        let item = self.source.read::<Sequential>(ReadRange {
            byte_offset: self.range.byte_offset,
            length: 1,
        })?;
        self.range.byte_offset += 1;
        self.range.length -= 1;
        Ok(Some(item[0]))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.range.length as usize;
        (len, Some(len))
    }

    fn nth(&mut self, n: usize) -> Result<Option<T>> {
        let skip = (n as u64).min(self.range.length);
        self.range.byte_offset += skip;
        self.range.length -= skip;
        self.next()
    }

    fn try_fold<B, E, F>(&mut self, init: B, mut f: F) -> Result<B, E>
    where
        E: From<UniversalIoError>,
        F: FnMut(B, T) -> Result<B, E>,
    {
        let mut acc = Some(init);
        self.source.read_batch::<Sequential, SubmissionOrder, E>(
            self.range.iter_chunks(1),
            |_idx, data| {
                acc = Some(f(acc.take().unwrap(), data[0])?);
                Ok(())
            },
        )?;
        self.range.byte_offset += self.range.length;
        self.range.length = 0;
        Ok(acc.unwrap())
    }
}
