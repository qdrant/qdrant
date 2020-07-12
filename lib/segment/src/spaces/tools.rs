use crate::vector_storage::vector_storage::ScoredPoint;
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


pub fn peek_top_scores(scores: &[ScoredPoint], top: usize, distance: &Distance) -> Vec<ScoredPoint> {
    if top == 0 {
        return scores.to_vec();
    }

    let order = distance_order(&distance);
    let res = match order {
        Order::LargeBetter => {
            let mut pq = FixedLengthPriorityQueue::new(top);
            for score_point in scores {
                pq.push(score_point.clone());
            }
            pq.into_vec()
        }
        Order::SmallBetter => {
            let mut pq = FixedLengthPriorityQueue::new(top);
            for score_point in scores {
                pq.push(Reverse(score_point.clone()));
            }
            pq.into_vec()
                .iter()
                .map(|&x| match x { Reverse(v) => v })
                .collect()
        }
    };
    return res;
}

pub fn mertic_object(distance: &Distance) -> Box<dyn Metric<VectorElementType>> {
    match distance {
        Distance::Cosine => Box::new(CosineMetric {}),
        Distance::Euclid => unimplemented!(),
        Distance::Dot => Box::new(DotProductMetric {}),
    }
}