use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::num::NonZeroUsize;
use std::vec::IntoIter as VecIntoIter;

use bytemuck::{TransparentWrapper as _, TransparentWrapperAlloc as _};
use serde::{Deserialize, Serialize};

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
        if self.heap.len() < self.length.into() {
            self.heap.push(Reverse(value));
            return None;
        }

        let mut x = self.heap.peek_mut().unwrap();
        let mut value = Reverse(value);
        if x.0 < value.0 {
            std::mem::swap(&mut *x, &mut value);
        }
        Some(value.0)
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
}
