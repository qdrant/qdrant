//! The `(value, point)` order of a per-element numeric index, restored from its
//! `(value, element)` order without collecting the whole range.

use std::collections::VecDeque;

use common::types::PointOffsetType;

use super::{ElementMap, ElementOffsetType};
use crate::data_types::order_by::OrderValue;

type ValuePoint = (OrderValue, PointOffsetType);

/// The `(value, point)` pairs of `(value, element)` pairs sorted by value, in the order
/// and with the uniqueness of the flattened index: by value, then by point, each pair
/// once.
///
/// Requires `inner` to be sorted by value, as the `stream_range` of a numeric index is.
/// Only the order within a value may be arbitrary. A run of equal values is then complete
/// as soon as a different value is read, since no later pair can have its value again.
/// With pairs not sorted by value, a value could start a second run, and the result would
/// have repeated pairs out of order.
///
/// Within a value the elements come in element order, which is not point order, and a
/// point can have the value in several of its elements. So every run of equal values is
/// buffered, then sorted and deduplicated by point before it is yielded.
///
/// Each end buffers a run of its own. A run is always taken whole from the pairs neither
/// end has taken yet, which are a sorted, contiguous part of `inner` (the pairs held in
/// `front_next` and `back_next` included), so a value never ends up in both runs.
pub(super) struct PointRuns<'a, I> {
    inner: I,
    element_map: &'a ElementMap,
    /// Read from the front of `inner` past the end of the front run
    front_next: Option<(OrderValue, ElementOffsetType)>,
    /// Read from the back of `inner` past the start of the back run
    back_next: Option<(OrderValue, ElementOffsetType)>,
    /// The run being yielded by `next`, sorted
    front: VecDeque<ValuePoint>,
    /// The run being yielded by `next_back`, sorted
    back: VecDeque<ValuePoint>,
}

impl<'a, I> PointRuns<'a, I>
where
    I: DoubleEndedIterator<Item = (OrderValue, ElementOffsetType)>,
{
    pub(super) fn new(inner: I, element_map: &'a ElementMap) -> Self {
        Self {
            inner,
            element_map,
            front_next: None,
            back_next: None,
            front: VecDeque::new(),
            back: VecDeque::new(),
        }
    }

    /// The first pair not taken by either end yet.
    fn pop_front_pair(&mut self) -> Option<(OrderValue, ElementOffsetType)> {
        self.front_next
            .take()
            .or_else(|| self.inner.next())
            .or_else(|| self.back_next.take())
    }

    /// The last pair not taken by either end yet.
    fn pop_back_pair(&mut self) -> Option<(OrderValue, ElementOffsetType)> {
        self.back_next
            .take()
            .or_else(|| self.inner.next_back())
            .or_else(|| self.front_next.take())
    }

    /// Fill `front` with the next run, `false` if there is none left.
    fn fill_front(&mut self) -> bool {
        let Some((value, element)) = self.pop_front_pair() else {
            return false;
        };
        let mut run = std::mem::take(&mut self.front);
        self.push(&mut run, value, element);
        while let Some((next_value, element)) = self.pop_front_pair() {
            if next_value != value {
                self.front_next = Some((next_value, element));
                break;
            }
            self.push(&mut run, value, element);
        }
        self.front = sorted_by_point(run);
        true
    }

    /// Fill `back` with the previous run, `false` if there is none left.
    fn fill_back(&mut self) -> bool {
        let Some((value, element)) = self.pop_back_pair() else {
            return false;
        };
        let mut run = std::mem::take(&mut self.back);
        self.push(&mut run, value, element);
        while let Some((next_value, element)) = self.pop_back_pair() {
            if next_value != value {
                self.back_next = Some((next_value, element));
                break;
            }
            self.push(&mut run, value, element);
        }
        self.back = sorted_by_point(run);
        true
    }

    /// Add the point that owns `element`, if any.
    fn push(&self, run: &mut VecDeque<ValuePoint>, value: OrderValue, element: ElementOffsetType) {
        if let Some(point) = self.element_map.element_point(element) {
            run.push_back((value, point));
        }
    }
}

/// A run of equal values, sorted by point and with each point once.
fn sorted_by_point(run: VecDeque<ValuePoint>) -> VecDeque<ValuePoint> {
    let mut run = Vec::from(run);
    run.sort_unstable_by_key(|&(_, point)| point);
    run.dedup_by_key(|&mut (_, point)| point);
    VecDeque::from(run)
}

impl<I> Iterator for PointRuns<'_, I>
where
    I: DoubleEndedIterator<Item = (OrderValue, ElementOffsetType)>,
{
    type Item = ValuePoint;

    fn next(&mut self) -> Option<ValuePoint> {
        loop {
            if let Some(item) = self.front.pop_front() {
                return Some(item);
            }
            if !self.fill_front() {
                // Only the back run is left
                return self.back.pop_front();
            }
        }
    }
}

impl<I> DoubleEndedIterator for PointRuns<'_, I>
where
    I: DoubleEndedIterator<Item = (OrderValue, ElementOffsetType)>,
{
    fn next_back(&mut self) -> Option<ValuePoint> {
        loop {
            if let Some(item) = self.back.pop_back() {
                return Some(item);
            }
            if !self.fill_back() {
                return self.front.pop_back();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;
    use crate::index::field_index::nested_index::ElementMapBuilder;
    use crate::index::payload_config::IndexMutability;

    /// Any interleaving of `next` and `next_back` yields the sorted, deduplicated pairs.
    #[test]
    fn point_runs_match_sorted_pairs_from_both_ends() {
        let dir = tempfile::tempdir().unwrap();
        let mut rng = StdRng::seed_from_u64(5);

        let mut builder = ElementMapBuilder::default();
        for point in 0..50 {
            builder.add(point, rng.random_range(0..5)).unwrap();
        }
        let map = ElementMap::create(
            dir.path().join("map"),
            IndexMutability::Immutable,
            &builder,
            0,
            true,
        )
        .unwrap();

        for _ in 0..200 {
            // Few distinct values, so runs are long and points repeat within them
            let mut pairs: Vec<(OrderValue, ElementOffsetType)> = (0..map.elements_count())
                .filter_map(|element| {
                    let value = OrderValue::Int(rng.random_range(0..6));
                    rng.random_bool(0.7)
                        .then_some((value, element as ElementOffsetType))
                })
                .collect();
            pairs.sort_by_key(|&(value, _)| value);

            let mut expected: Vec<ValuePoint> = pairs
                .iter()
                .filter_map(|&(value, element)| Some((value, map.element_point(element)?)))
                .collect();
            expected.sort_unstable();
            expected.dedup();

            let mut runs = PointRuns::new(pairs.into_iter(), &map);
            let mut front = Vec::new();
            let mut back = Vec::new();
            loop {
                let item = if rng.random_bool(0.5) {
                    runs.next().map(|item| front.push(item))
                } else {
                    runs.next_back().map(|item| back.push(item))
                };
                if item.is_none() {
                    break;
                }
            }
            assert_eq!(runs.next(), None);
            back.reverse();
            front.extend(back);
            assert_eq!(front, expected);
        }
    }
}
