
use crate::types::{Distance, Order, distance_order, VectorElementType};
use std::collections::BinaryHeap;
use std::cmp::Reverse;
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric};


struct FixedLengthPriorityQueue<T> {
    heap: BinaryHeap<T>,
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
        self.heap.push(value);
        return if self.heap.len() > self.length {
            self.heap.pop()
        } else {
            None
        };
    }

    pub fn into_vec(self) -> Vec<T> {
        self.heap.into_sorted_vec()
    }
}

pub fn peek_top_scores_iterable<I, E: Ord + Clone>(scores: I, top: usize, distance: &Distance) -> Vec<E>
    where
        I: Iterator<Item=E>,
{
    if top == 0 {
        return scores.collect();
    }

    let order = distance_order(&distance);
    let res = match order {
        Order::SmallBetter => {
            // If small values is better - PQ should pop-out large values first.
            // Hence is should be max-heap
            let mut pq = FixedLengthPriorityQueue::new(top);
            for score_point in scores {
                pq.push(score_point.clone());
            }
            pq.into_vec()
        }
        Order::LargeBetter => {
            let mut pq = FixedLengthPriorityQueue::new(top);
            for score_point in scores {
                pq.push(Reverse(score_point.clone()));
            }
            pq.into_vec()
                .iter()
                .map(|x| match x { Reverse(v) => v.clone() })
                .collect()
        }
    };
    return res;
}


pub fn peek_top_scores<E: Ord + Clone>(scores: &[E], top: usize, distance: &Distance) -> Vec<E> {
    return peek_top_scores_iterable(scores.iter().cloned(), top, distance)

}

pub fn mertic_object(distance: &Distance) -> Box<dyn Metric<VectorElementType>> {
    match distance {
        Distance::Cosine => Box::new(CosineMetric {}),
        Distance::Euclid => unimplemented!(),
        Distance::Dot => Box::new(DotProductMetric {}),
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_peek_top() {
        let data = vec![10, 20, 40, 5, 100, 33, 84, 65, 20, 43, 44, 42];
        let res = peek_top_scores(&data, 3, &Distance::Dot);
        assert_eq!(res, vec![100, 84, 65]);
        let res = peek_top_scores(&data, 3, &Distance::Euclid);
        assert_eq!(res, vec![5, 10, 20]);
    }
}