#![allow(mismatched_lifetime_syntaxes)]

use std::marker::PhantomData;

use fallible_iterator::FallibleIterator;

use super::*;
use crate::generic_consts::Sequential;

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
            self.source.read_batch::<Sequential, UniversalIoError>(
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

    pub fn iter(&self) -> UniversalIterator<'a, T, S> {
        UniversalIterator {
            source: self.source,
            rq: ReorderingQueue::new(),
            index: 0,
            len: self.range.length,
            byte_offset: self.range.byte_offset,
            _phantom: PhantomData,
        }
    }
}

const QUEUE_DEPTH: usize = 64;
pub struct UniversalIterator<'a, T: Copy + 'static, S: UniversalRead<T>> {
    source: &'a S,
    rq: ReorderingQueue<T, QUEUE_DEPTH>,
    index: u64,
    len: u64,
    byte_offset: u64,
    _phantom: PhantomData<T>,
}

impl<'a, T: Copy + 'static, S: UniversalRead<T>> FallibleIterator for UniversalIterator<'a, T, S> {
    type Item = (u64, T);
    type Error = UniversalIoError;

    fn next(&mut self) -> Result<Option<(u64, T)>> {
        if self.len == 0 {
            return Ok(None);
        }
        if let Some((seqnum, value)) = self.rq.get_unordered() {
            return Ok(Some((seqnum as u64, value)));
        }

        let it = (0..self.len)
            .map(|i| ReadRange {
                byte_offset: self.byte_offset + i * size_of::<T>() as u64,
                length: size_of::<T>() as u64,
            })
            .take(QUEUE_DEPTH);
        self.source.read_batch::<Sequential, _>(it, |idx, data| {
            self.rq.put(idx as usize, data[0]);
            Result::<(), UniversalIoError>::Ok(())
        })?;

        let (seqnum, value) = self
            .rq
            .get_unordered()
            .expect("read_batch should have filled the queue");

        Ok(Some((seqnum as u64, value)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len as usize, Some(self.len as usize))
    }

    fn nth(&mut self, n: usize) -> Result<Option<(u64, T)>> {
        let skip = (n as u64).min(self.len);
        self.byte_offset += skip * size_of::<T>() as u64;
        self.index += skip;
        self.len -= skip;
        self.next()
    }

    fn try_fold<B, E, F>(&mut self, init: B, mut f: F) -> Result<B, E>
    where
        E: From<UniversalIoError>,
        F: FnMut(B, (u64, T)) -> Result<B, E>,
    {
        let it = (0..self.len).map(|i| ReadRange {
            byte_offset: self.byte_offset + i * size_of::<T>() as u64,
            length: size_of::<T>() as u64,
        });

        let mut acc = Some(init);
        self.source.read_batch::<Sequential, E>(it, |idx, data| {
            acc = Some(f(acc.take().unwrap(), (self.index + idx as u64, data[0]))?);
            Ok(())
        })?;
        Ok(acc.unwrap())
    }
}
