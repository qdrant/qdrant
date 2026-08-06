use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::num::NonZeroUsize;
use std::vec::IntoIter as VecIntoIter;

use bytemuck::{TransparentWrapper as _, TransparentWrapperAlloc as _};
use serde::{Deserialize, Serialize};

use crate::types::ScoredPointOffset;

/// To avoid excessive memory allocation, FixedLengthPriorityQueue
/// imposes a reasonable limit on the allocation size. If the limit
/// is extremely large, we treat it as if no limit was set and
/// delay allocation, assuming that the results will fit within a
/// predefined threshold.
const LARGEST_REASONABLE_ALLOCATION_SIZE: usize = 1_048_576;

/// A container that forgets all but the top N elements
///
/// This is a MinHeap by default - it will keep the largest elements, pop smallest
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct FixedLengthPriorityQueue<T: Ord> {
    heap: BinaryHeap<Reverse<T>>,
    length: NonZeroUsize,
}

impl<T: Ord> Default for FixedLengthPriorityQueue<T> {
    fn default() -> Self {
        Self::new(1)
    }
}

impl<T: Ord> FixedLengthPriorityQueue<T> {
    /// Creates a new queue with the given length
    /// Panics if length is 0
    pub fn new(length: usize) -> Self {
        let heap = BinaryHeap::with_capacity(
            length
                .saturating_add(1)
                .min(LARGEST_REASONABLE_ALLOCATION_SIZE),
        );
        let length = NonZeroUsize::new(length).expect("length must be greater than zero");
        FixedLengthPriorityQueue::<T> { heap, length }
    }

    /// Pushes a value into the priority queue.
    ///
    /// If the queue if full, replaces the smallest value and returns it.
    pub fn push(&mut self, value: T) -> Option<T> {
        if !self.is_full() {
            return self.fill(value);
        }

        // Reject without ever constructing the `PeekMut` guard, whose `Drop` sifts the heap.
        if self.heap.peek().expect("full queue is not empty").0 >= value {
            return Some(value);
        }

        let mut x = self.heap.peek_mut().expect("full queue is not empty");
        let mut value = Reverse(value);
        std::mem::swap(&mut *x, &mut value);
        Some(value.0)
    }

    /// The `push` path taken only while the queue is not full yet, kept out of line so that
    /// the hot rejection path does not carry its register pressure.
    #[inline(never)]
    fn fill(&mut self, value: T) -> Option<T> {
        self.heap.push(Reverse(value));
        None
    }

    /// Consumes the [`FixedLengthPriorityQueue`] and returns a vector
    /// in sorted (descending) order.
    pub fn into_sorted_vec(self) -> Vec<T> {
        Reverse::peel_vec(self.heap.into_sorted_vec())
    }

    /// Returns an iterator over the elements in the queue, in arbitrary order.
    pub fn iter_unsorted(&self) -> std::slice::Iter<'_, T> {
        Reverse::peel_slice(self.heap.as_slice()).iter()
    }

    /// Returns an iterator over the elements in the queue
    /// in sorted (descending) order.
    pub fn into_iter_sorted(self) -> VecIntoIter<T> {
        self.into_sorted_vec().into_iter()
    }

    /// Returns the smallest element of the queue,
    /// if there is any.
    pub fn top(&self) -> Option<&T> {
        self.heap.peek().map(|x| &x.0)
    }

    /// Returns actual length of the queue
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// Checks if the queue is empty
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Checks if the queue is full
    pub fn is_full(&self) -> bool {
        self.heap.len() >= self.length.into()
    }

    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&T) -> bool,
    {
        self.heap.retain(|x| f(&x.0));
    }
}

impl FixedLengthPriorityQueue<ScoredPointOffset> {
    /// Pushes a chunk of points, skipping the ones that cannot enter a full queue.
    ///
    /// The outcome is identical to [`Self::push`]-ing every element.
    #[inline]
    pub fn push_chunk(&mut self, points: impl IntoIterator<Item = ScoredPointOffset>) {
        // Score of the queue's current minimum, while the queue is full.
        let mut threshold = self.min_score();

        for point in points {
            // A score at or below the minimum cannot displace anything — `push` would hand it
            // back untouched (`ScoredPointOffset` orders by score alone; NaN falls through to
            // `push`). Rejecting on this register-held f32 compare skips the call and its heap
            // bookkeeping, and with `top` ≪ point count nearly every push in a full scan is a
            // rejection.
            if threshold.is_some_and(|threshold| point.score <= threshold) {
                continue;
            }

            self.push(point);
            threshold = self.min_score();
        }
    }

    #[inline]
    fn min_score(&self) -> Option<f32> {
        self.is_full()
            .then(|| self.top().expect("full queue is not empty").score)
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{RngExt as _, SeedableRng as _};

    use super::*;

    /// `push_chunk` must yield the same queue as pushing element by element.
    #[test]
    fn test_push_chunk() {
        let mut rng = StdRng::seed_from_u64(42);
        let points = (0..1000)
            .map(|idx| ScoredPointOffset {
                idx,
                score: rng.random_range(0.0..10.0),
            })
            .collect::<Vec<_>>();

        for limit in [1, 2, 10, 1000, 2000] {
            for chunk_size in [1, 7, 64] {
                let mut expected = FixedLengthPriorityQueue::new(limit);
                points.iter().for_each(|&point| _ = expected.push(point));

                let mut actual = FixedLengthPriorityQueue::new(limit);
                for chunk in points.chunks(chunk_size) {
                    actual.push_chunk(chunk.iter().copied());
                }

                assert_eq!(
                    actual.into_sorted_vec(),
                    expected.into_sorted_vec(),
                    "limit={limit}, chunk_size={chunk_size}",
                );
            }
        }
    }
}
