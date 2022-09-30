use std::cmp::Reverse;
use std::collections::binary_heap::Iter as BinaryHeapIter;
use std::collections::BinaryHeap;
use std::iter::Rev;
use std::vec::IntoIter as VecIntoIter;

use serde::{Deserialize, Serialize};

/// This is a MinHeap by default - it will keep the largest elements, pop smallest
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct FixedLengthPriorityQueue<T: Ord> {
    heap: BinaryHeap<Reverse<T>>,
    length: usize,
}

impl<T: Ord> FixedLengthPriorityQueue<T> {
    pub fn new(length: usize) -> Self {
        assert!(length > 0);
        FixedLengthPriorityQueue::<T> {
            heap: BinaryHeap::with_capacity(length + 1),
            length,
        }
    }

    pub fn push(&mut self, value: T) -> Option<T> {
        if self.heap.len() < self.length {
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

pub fn peek_top_smallest_iterable<I, E: Ord>(elements: I, top: usize) -> Vec<E>
where
    I: IntoIterator<Item = E>,
{
    if top == 0 {
        return vec![];
    }

    // If small values is better - PQ should pop-out big values first.
    // Hence is should be min-heap
    let mut pq = FixedLengthPriorityQueue::new(top);
    for element in elements {
        pq.push(Reverse(element));
    }
    pq.into_vec().into_iter().map(|Reverse(x)| x).collect()
}

pub fn peek_top_largest_iterable<I, E: Ord>(elements: I, top: usize) -> Vec<E>
where
    I: IntoIterator<Item = E>,
{
    if top == 0 {
        return vec![];
    }

    // If big values is better - PQ should pop-out small values first.
    // Hence is should be min-heap
    let mut pq = FixedLengthPriorityQueue::new(top);
    for element in elements {
        pq.push(element);
    }
    pq.into_vec()
}

pub fn peek_top_scores<E: Ord + Clone>(scores: &[E], top: usize) -> Vec<E> {
    peek_top_largest_iterable(scores.iter().cloned(), top)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peek_top() {
        let data = vec![10, 20, 40, 5, 100, 33, 84, 65, 20, 43, 44, 42];
        let res = peek_top_scores(&data, 3);
        assert_eq!(res, vec![100, 84, 65]);
    }

    #[test]
    fn test_peek_top_rev() {
        let data = vec![10, 20, 40, 5, 100, 33, 84, 65, 20, 43, 44, 42];
        let res = peek_top_smallest_iterable(data.into_iter(), 3);
        assert_eq!(res, vec![5, 10, 20]);
    }
}
