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
            batch_start: 0,
            batch_len: 0,
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
    /// Absolute index of the first element in the current batch.
    batch_start: u64,
    /// Number of elements loaded in the current batch.
    batch_len: u64,
    /// Remaining elements to yield (including buffered ones).
    len: u64,
    /// Byte offset of the next element to fetch from source.
    byte_offset: u64,
    _phantom: PhantomData<T>,
}

impl<T: Copy + 'static, S: UniversalRead<T>> UniversalIterator<'_, T, S> {
    /// Advance `batch_start` past the current batch and reset `batch_len`.
    fn close_batch(&mut self) {
        self.batch_start += self.batch_len;
        self.batch_len = 0;
    }
}

impl<T: Copy + 'static, S: UniversalRead<T>> FallibleIterator for UniversalIterator<'_, T, S> {
    type Item = (u64, T);
    type Error = UniversalIoError;

    fn next(&mut self) -> Result<Option<(u64, T)>> {
        if self.len == 0 {
            return Ok(None);
        }

        if let Some((seqnum, value)) = self.rq.get_unordered() {
            self.len -= 1;
            return Ok(Some((self.batch_start + seqnum as u64, value)));
        }

        // Buffer empty — advance past previous batch and fetch a new one.
        self.close_batch();
        self.rq = ReorderingQueue::new();
        let batch_size = self.len.min(QUEUE_DEPTH as u64);
        let it = (0..batch_size).map(|i| ReadRange {
            byte_offset: self.byte_offset + i * size_of::<T>() as u64,
            length: size_of::<T>() as u64,
        });
        self.source.read_batch::<Sequential, _>(it, |idx, data| {
            self.rq.put(idx, data[0]);
            Ok::<(), UniversalIoError>(())
        })?;
        self.batch_len = batch_size;
        self.byte_offset += batch_size * size_of::<T>() as u64;

        let (seqnum, value) = self
            .rq
            .get_unordered()
            .expect("read_batch should have filled the queue");

        self.len -= 1;
        Ok(Some((self.batch_start + seqnum as u64, value)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len as usize, Some(self.len as usize))
    }

    fn nth(&mut self, n: usize) -> Result<Option<(u64, T)>> {
        // Drain buffered items first.
        let mut remaining = n;
        while remaining > 0 {
            if self.rq.get_unordered().is_some() {
                remaining -= 1;
                self.len -= 1;
            } else {
                break;
            }
        }

        // Skip unfetched items.
        if remaining > 0 {
            self.close_batch();
            let skip = (remaining as u64).min(self.len);
            self.batch_start += skip;
            self.byte_offset += skip * size_of::<T>() as u64;
            self.len -= skip;
        }

        self.next()
    }

    fn try_fold<B, E, F>(&mut self, init: B, mut f: F) -> Result<B, E>
    where
        E: From<UniversalIoError>,
        F: FnMut(B, (u64, T)) -> Result<B, E>,
    {
        // Drain buffer first.
        let mut acc = init;
        while let Some((seqnum, value)) = self.rq.get_unordered() {
            acc = f(acc, (self.batch_start + seqnum as u64, value))?;
            self.len -= 1;
        }
        self.close_batch();

        // Read all remaining in a single batch.
        if self.len > 0 {
            let batch_start = self.batch_start;
            let it = (0..self.len).map(|i| ReadRange {
                byte_offset: self.byte_offset + i * size_of::<T>() as u64,
                length: size_of::<T>() as u64,
            });

            let mut acc_opt = Some(acc);
            self.source.read_batch::<Sequential, E>(it, |idx, data| {
                acc_opt = Some(f(
                    acc_opt.take().unwrap(),
                    (batch_start + idx as u64, data[0]),
                )?);
                Ok(())
            })?;
            acc = acc_opt.unwrap();

            self.byte_offset += self.len * size_of::<T>() as u64;
            self.batch_start += self.len;
            self.len = 0;
        }

        Ok(acc)
    }
}
