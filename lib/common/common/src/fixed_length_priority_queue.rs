use std::cmp::Reverse;
use std::collections::binary_heap::Iter as BinaryHeapIter;
use std::collections::{BinaryHeap, TryReserveError};
use std::iter::Rev;
use std::num::NonZeroUsize;
use std::vec::IntoIter as VecIntoIter;

use serde::{Deserialize, Serialize};

/// This is a MinHeap by default - it will keep the largest elements, pop smallest
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct FixedLengthPriorityQueue<T: Ord> {
    heap: BinaryHeap<Reverse<T>>,
    length: NonZeroUsize,
}

impl<T: Ord> Default for FixedLengthPriorityQueue<T> {
    fn default() -> Self {
        Self::new(1).unwrap()
    }
}

impl<T: Ord> FixedLengthPriorityQueue<T> {
    /// Creates a new queue with the given length
    /// Panics if length is 0
    pub fn new(length: usize) -> Result<Self, TryReserveError> {
        let mut heap = BinaryHeap::new();
        heap.try_reserve_exact(length + 1)?;
        let heap = heap;
        let length = NonZeroUsize::new(length).expect("length must be > 0");
        Ok(FixedLengthPriorityQueue::<T> { heap, length })
    }

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

    pub fn into_vec(self) -> Vec<T> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|Reverse(x)| x)
            .collect()
    }

    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            it: self.heap.iter().rev(),
        }
    }

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

pub struct Iter<'a, T> {
    it: Rev<BinaryHeapIter<'a, Reverse<T>>>,
}

pub struct IntoIter<T> {
    it: VecIntoIter<Reverse<T>>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.it.next().map(|Reverse(x)| x)
    }
}

impl<T> Iterator for IntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.it.next().map(|Reverse(x)| x)
    }
}

impl<'a, T: Ord> IntoIterator for &'a FixedLengthPriorityQueue<T> {
    type Item = &'a T;

    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T: Ord> IntoIterator for FixedLengthPriorityQueue<T> {
    type Item = T;

    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            it: self.heap.into_sorted_vec().into_iter(),
        }
    }
}
