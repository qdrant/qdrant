use std::cmp::Reverse;

use common::fixed_length_priority_queue::FixedLengthPriorityQueue;

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
    // Hence it should be min-heap
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
        let res = peek_top_smallest_iterable(data, 3);
        assert_eq!(res, vec![5, 10, 20]);
    }
}
