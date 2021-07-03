use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric};
use crate::types::Distance;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BinaryHeap;

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
        return if self.heap.peek().unwrap().0 < value {
            self.heap.push(Reverse(value));
            self.heap.pop().map(|x| x.0)
        } else {
            Some(value)
        };
    }

    pub fn into_vec(self) -> Vec<T> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|x| x.0)
            .collect()
    }

    pub fn into_iterator(self) -> impl Iterator<Item = T> {
        self.heap.into_sorted_vec().into_iter().map(|x| x.0)
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> + '_ {
        self.heap.iter().rev().map(|x| &x.0)
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

pub fn peek_top_scores_iterable<I, E: Ord>(scores: I, top: usize) -> Vec<E>
where
    I: Iterator<Item = E>,
{
    if top == 0 {
        return scores.collect();
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
    return peek_top_scores_iterable(scores.iter().cloned(), top);
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
