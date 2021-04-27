use serde::{Deserialize, Serialize};
use crate::types::{Distance, Order};
use std::collections::BinaryHeap;
use std::cmp::Reverse;
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric};
use std::collections::binary_heap::Iter;
use crate::types::Distance::Euclid;


/// This is a MinHeap by default - it will keep the largest elements, pop smallest
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct FixedLengthPriorityQueue<T: Ord> {
    heap: BinaryHeap<Reverse<T>>,
    length: usize,
}

impl<T: Ord> FixedLengthPriorityQueue<T> {
    pub fn new(length: usize) -> Self {
        FixedLengthPriorityQueue::<T> {
            heap: BinaryHeap::new(),
            length,
        }
    }

    pub fn push(&mut self, value: T) -> Option<T> {
        self.heap.push(Reverse(value));
        return if self.heap.len() > self.length {
            self.heap.pop().map(|x| x.0)
        } else {
            None
        };
    }

    pub fn into_vec(self) -> Vec<T> {
        self.heap.into_sorted_vec().into_iter().map(|x| x.0).collect()
    }

    pub fn iter(&self) -> impl Iterator<Item=&T> + '_ {
        self.heap.iter().map(|x| &x.0)
    }

    pub fn top(&self) -> Option<&T> { self.heap.peek().map(|x| &x.0) }
}

pub fn peek_top_scores_iterable<I, E: Ord + Clone>(scores: I, top: usize) -> Vec<E>
    where
        I: Iterator<Item=E>,
{
    if top == 0 {
        return scores.collect();
    }

    // If big values is better - PQ should pop-out small values first.
    // Hence is should be min-heap
    let mut pq = FixedLengthPriorityQueue::new(top);
    for score_point in scores {
        pq.push(score_point.clone());
    }
    pq.into_vec()
}


pub fn peek_top_scores<E: Ord + Clone>(scores: &[E], top: usize) -> Vec<E> {
    return peek_top_scores_iterable(scores.iter().cloned(), top)
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