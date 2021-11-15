use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric};
use crate::types::Distance;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::binary_heap::Iter as BinaryHeapIter;
use std::collections::BinaryHeap;
use std::iter::Rev;
use std::vec::IntoIter as VecIntoIter;

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

        match self.heap.peek() {
            Some(Reverse(x)) if x < &value => {
                self.heap.push(Reverse(value));
                self.heap.pop().map(|Reverse(x)| x)
            }
            _ => Some(value),
        }
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

pub fn peek_top_scores_iterable<I, E: Ord>(scores: I, top: usize) -> Vec<E>
where
    I: IntoIterator<Item = E>,
{
    if top == 0 {
        return scores.into_iter().collect();
    }

    // If big values is better - PQ should pop-out small values first.
    // Hence is should be min-heap
    let mut pq = FixedLengthPriorityQueue::new(top);
    for score_point in scores {
        pq.push(score_point);
    }
    pq.into_vec()
}

pub fn peek_top_scores<E: Ord + Clone>(scores: &[E], top: usize) -> Vec<E> {
    peek_top_scores_iterable(scores.iter().cloned(), top)
}

pub fn mertic_object(distance: &Distance) -> Box<dyn Metric> {
    match distance {
        Distance::Cosine => Box::new(CosineMetric {}),
        Distance::Euclid => Box::new(EuclidMetric {}),
        Distance::Dot => Box::new(DotProductMetric {}),
    }
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
}
