use std::collections::{HashMap, HashSet};
use std::hash;

use segment::types::{Payload, ScoredPoint};
use tinyvec::TinyVec;

use crate::operations::types::Record;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ResolveCondition {
    All,
    Majority,
}

pub trait Resolve: Sized {
    fn resolve(responses: Vec<Self>, condition: ResolveCondition) -> Self;
}

impl Resolve for Vec<Record> {
    fn resolve(records: Vec<Self>, condition: ResolveCondition) -> Self {
        let mut resolved = Resolver::resolve(records, |record| record.id, record_eq, condition);
        resolved.sort_unstable_by_key(|record| record.id);
        resolved
    }
}

impl Resolve for Vec<Vec<ScoredPoint>> {
    fn resolve(batches: Vec<Self>, condition: ResolveCondition) -> Self {
        // batches: <replica_id, <batch_id, ScoredPoint>>
        // transpose to <batch_id, <replica_id, ScoredPoint>>

        let batches = transpose(batches);

        batches
            .into_iter()
            .map(|points| {
                let mut resolved =
                    Resolver::resolve(points, |point| point.id, scored_point_eq, condition);

                resolved.sort_unstable();
                resolved
            })
            .collect()
    }
}

fn transpose<T>(vec: Vec<Vec<T>>) -> Vec<Vec<T>> {
    if vec.is_empty() {
        return Vec::new();
    }

    let len = vec[0].len();

    let mut iters: Vec<_> = vec.into_iter().map(IntoIterator::into_iter).collect();

    (0..len)
        .map(|_| iters.iter_mut().filter_map(Iterator::next).collect())
        .collect()
}

fn record_eq(this: &Record, other: &Record) -> bool {
    this.id == other.id && this.vector == other.vector && payload_eq(&this.payload, &other.payload)
}

fn scored_point_eq(this: &ScoredPoint, other: &ScoredPoint) -> bool {
    this.id == other.id
        && this.score == other.score
        && this.vector == other.vector
        && payload_eq(&this.payload, &other.payload)
}

fn payload_eq(this: &Option<Payload>, other: &Option<Payload>) -> bool {
    match (this, other) {
        (Some(payload), None) | (None, Some(payload)) => payload.is_empty(),
        (this, other) => this == other,
    }
}

struct Resolver<'a, Item, Id, Ident, Cmp> {
    items: HashMap<Id, ResolverRecords<'a, Item>>,
    identify: Ident,
    compare: Cmp,
}

type ResolverRecords<'a, Item> = TinyVec<[ResolverRecord<'a, Item>; RESOLVER_RECORDS_CAPACITY]>;

const RESOLVER_RECORDS_CAPACITY: usize = 5; // Expected number of replicas

impl<'a, Item, Id, Ident, Cmp> Resolver<'a, Item, Id, Ident, Cmp>
where
    Id: Eq + hash::Hash,
    Ident: Fn(&Item) -> Id,
    Cmp: Fn(&Item, &Item) -> bool,
{
    pub fn resolve(
        items: Vec<Vec<Item>>,
        identify: Ident,
        compare: Cmp,
        condition: ResolveCondition,
    ) -> Vec<Item> {
        let resolution_count = match condition {
            ResolveCondition::All => items.len(),
            ResolveCondition::Majority => items.len() / 2 + 1,
        };

        let mut resolver = Resolver::new(items.first().map_or(0, Vec::len), identify, compare);
        resolver.add_all(&items);

        // Select coordinates of accepted items, avoiding copying
        let resolved_items: HashSet<_> = resolver
            .items
            .into_iter()
            .filter_map(|(_, points)| {
                points
                    .into_iter()
                    .find(|point| point.count >= resolution_count)
                    .map(|point| (point.row, point.index))
            })
            .collect();

        // Shortcut if everything is consistent: return first items, avoiding filtering
        let is_consistent = resolved_items.len() == items.first().map_or(0, Vec::len)
            && resolved_items.iter().all(|&(row, _)| row == 0);

        if is_consistent {
            items.into_iter().next().unwrap_or_default()
        } else {
            items
                .into_iter()
                .enumerate()
                .flat_map(|(row, items)| {
                    items
                        .into_iter()
                        .enumerate()
                        .map(move |(index, item)| (row, index, item))
                })
                .filter_map(|(row, index, item)| {
                    if resolved_items.contains(&(row, index)) {
                        Some(item)
                    } else {
                        None
                    }
                })
                .collect()
        }
    }

    fn new(capacity: usize, identify: Ident, compare: Cmp) -> Self {
        Self {
            items: HashMap::with_capacity(capacity),
            identify,
            compare,
        }
    }

    fn add_all<I>(&mut self, items: I)
    where
        I: IntoIterator,
        I::Item: IntoIterator<Item = &'a Item>,
    {
        for (row, items) in items.into_iter().enumerate() {
            for (index, item) in items.into_iter().enumerate() {
                self.add((self.identify)(item), item, row, index);
            }
        }
    }

    fn add(&mut self, id: Id, item: &'a Item, row: usize, index: usize) {
        let points = self.items.entry(id).or_default();

        for point in points.iter_mut() {
            if (self.compare)(item, point.item.unwrap()) {
                point.count += 1;
                return;
            }
        }

        points.push(ResolverRecord::new(item, row, index));
    }
}

#[derive(Debug)]
struct ResolverRecord<'a, T> {
    item: Option<&'a T>,
    row: usize,
    index: usize,
    count: usize,
}

impl<'a, T> Copy for ResolverRecord<'a, T> {}

impl<'a, T> Clone for ResolverRecord<'a, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, T> Default for ResolverRecord<'a, T> {
    fn default() -> Self {
        Self {
            item: None,
            row: 0,
            index: 0,
            count: 0,
        }
    }
}

impl<'a, T> ResolverRecord<'a, T> {
    fn new(item: &'a T, row: usize, index: usize) -> Self {
        Self {
            item: Some(item),
            row,
            index,
            count: 1,
        }
    }
}

#[cfg(test)]
mod test {
    use std::fmt;

    use common::types::ScoreType;

    use super::*;

    #[rustfmt::skip]
    fn resolve_scored_points_batch_4_data() -> [Vec<ScoredPoint>; 3] {
        [
            vec![
                point(14, 0.0), point(17, 0.1), point(15, 0.1),
                point(13, 0.2), point(11, 0.2), point(12, 0.3),
                point(18, 0.3), point(16, 0.4), point(10, 0.5),
            ],
            vec![
                point(23, 0.0), point(21, 0.1), point(25, 0.2),
                point(22, 0.2), point(20, 0.3), point(24, 0.3),
            ],
            vec![
                point(30, 0.1), point(31, 0.1), point(32, 0.1),
                point(33, 0.2), point(34, 0.2), point(35, 0.3),
            ],
        ]
    }

    fn point(id: u64, score: ScoreType) -> ScoredPoint {
        ScoredPoint {
            id: id.into(),
            version: 1,
            score,
            payload: None,
            vector: None,
            shard_key: None,
        }
    }

    #[rustfmt::skip]
    fn resolve_scored_points_batch_4_input() -> Vec<Vec<Vec<ScoredPoint>>> {
        let [batch1, batch2, batch3] = resolve_scored_points_batch_4_data();

        vec![
            vec![
                batch(&batch1, [remove(2), remove(3)]),
                batch(&batch2, [remove(0), remove(3)]),
                batch(&batch3, [remove(4), remove(5)]),
            ],

            vec![
                batch(&batch1, [remove(1), modify(3)]),
                batch(&batch2, [modify(0), remove(2)]),
                batch(&batch3, [remove(3), modify(5)]),
            ],

            vec![
                batch(&batch1, [remove(1), modify(4)]),
                batch(&batch2, [modify(3), remove(5)]),
                batch(&batch3, [remove(2), modify(5)]),
            ],

            vec![
                batch1,
                batch2,
                batch3,
            ],
        ]
    }

    fn batch<const N: usize>(batch: &[ScoredPoint], mut actions: [Action; N]) -> Vec<ScoredPoint> {
        let mut batch = batch.to_owned();

        actions.sort_unstable_by_key(|action| action.index());

        let mut removed = Vec::new();

        for action in actions.into_iter() {
            let offset = removed
                .iter()
                .filter(|&&removed| removed <= action.index())
                .count();

            match action {
                Action::Remove(index) => {
                    batch.remove(index - offset);
                    removed.push(index);
                }

                Action::Modify(index) => {
                    batch[index - offset].score += 1.0;
                }
            }
        }

        batch
    }

    #[derive(Copy, Clone, Debug)]
    enum Action {
        Remove(usize),
        Modify(usize),
    }

    impl Action {
        pub fn index(self) -> usize {
            match self {
                Self::Remove(index) => index,
                Self::Modify(index) => index,
            }
        }
    }

    fn remove(index: usize) -> Action {
        Action::Remove(index)
    }

    fn modify(index: usize) -> Action {
        Action::Modify(index)
    }

    #[test]
    fn resolve_scored_points_batch_4_all() {
        let [mut batch1, mut batch2, mut batch3] = resolve_scored_points_batch_4_data();

        batch1.remove(4);
        batch1.remove(3);
        batch1.remove(2);
        batch1.remove(1);

        batch2.remove(5);
        batch2.remove(3);
        batch2.remove(2);
        batch2.remove(0);

        batch3.remove(5);
        batch3.remove(4);
        batch3.remove(3);
        batch3.remove(2);

        test_resolve(
            resolve_scored_points_batch_4_input(),
            [batch1, batch2, batch3],
            ResolveCondition::All,
        );
    }

    #[test]
    fn resolve_scored_points_batch_4_majority() {
        let [mut batch1, mut batch2, mut batch3] = resolve_scored_points_batch_4_data();

        batch1.remove(3);
        batch1.remove(1);

        batch2.remove(3);
        batch2.remove(0);

        batch3.remove(5);

        test_resolve(
            resolve_scored_points_batch_4_input(),
            [batch1, batch2, batch3],
            ResolveCondition::Majority,
        );
    }

    fn data_simple() -> [i32; 9] {
        [1, 2, 3, 4, 5, 6, 7, 8, 9]
    }

    #[rustfmt::skip]
    fn input_2() -> [Vec<i32>; 2] {
        [
            vec![1, 2, 3,       6, 7,    9,     11, 12, 13],
            vec![      3, 4, 5, 6,    8,    10, 11,       ],
        ]
    }

    fn expected_2() -> [i32; 3] {
        [3, 6, 11]
    }

    #[rustfmt::skip]
    fn input_3() -> [Vec<i32>; 3] {
        [
            vec![1, 2,          6, 7, 8,       11,      13, 14, 15,       ],
            vec![   2, 3, 4,       7,    9, 10,         13, 14,     16,   ],
            vec![         4, 5, 6, 7,    9,    11, 12,      14,         17],
        ]
    }

    fn expected_3_all() -> [i32; 2] {
        [7, 14]
    }

    fn expected_3_majority() -> [i32; 8] {
        [2, 4, 6, 7, 9, 11, 13, 14]
    }

    #[rustfmt::skip]
    fn input_4() -> [Vec<i32>; 4] {
        [
            vec![1, 2, 3,                9,     11, 12, 13, 14,     16,         19,     21, 22,     24,     27,     29],
            vec![   2, 3, 4, 5, 6,                  12, 13,     15,     17,     19,         22,     24, 26, 27, 28,   ],
            vec![      3,    5, 6, 7, 8, 9,             13,     15, 16,     18,     20,     22,         26, 27, 28,   ],
            vec![               6,    8, 9, 10, 11, 12, 13,         16,     18, 19,     21,     23,     26, 27,     29],
        ]
    }

    fn expected_4_all() -> [i32; 2] {
        [13, 27]
    }

    fn expected_4_majority() -> [i32; 10] {
        [3, 6, 9, 12, 13, 16, 19, 22, 26, 27]
    }

    #[test]
    fn resolve_0_all() {
        resolve_0(ResolveCondition::All);
    }

    #[test]
    fn resolve_0_majority() {
        resolve_0(ResolveCondition::Majority);
    }

    fn resolve_0(condition: ResolveCondition) {
        test_resolve_simple(Vec::<Vec<i32>>::new(), Vec::new(), condition);
    }

    #[test]
    fn resolve_simple_all() {
        for replicas in 1..=5 {
            resolve_simple(replicas, ResolveCondition::All);
        }
    }

    #[test]
    fn resolve_simple_majority() {
        for replicas in 1..=5 {
            resolve_simple(replicas, ResolveCondition::All);
        }
    }

    fn resolve_simple(replicas: usize, condition: ResolveCondition) {
        let input: Vec<_> = (0..replicas).map(|_| data_simple()).collect();
        let expected = data_simple();

        test_resolve_simple(input, expected, condition)
    }

    #[test]
    fn resolve_2_all() {
        test_resolve_simple(input_2(), expected_2(), ResolveCondition::All);
    }

    #[test]
    fn resolve_2_majority() {
        test_resolve_simple(input_2(), expected_2(), ResolveCondition::Majority);
    }

    #[test]
    fn resolve_3_all() {
        test_resolve_simple(input_3(), expected_3_all(), ResolveCondition::All);
    }

    #[test]
    fn resolve_3_majority() {
        test_resolve_simple(input_3(), expected_3_majority(), ResolveCondition::Majority);
    }

    #[test]
    fn resolve_4_all() {
        test_resolve_simple(input_4(), expected_4_all(), ResolveCondition::All);
    }

    #[test]
    fn resolve_4_majority() {
        test_resolve_simple(input_4(), expected_4_majority(), ResolveCondition::Majority);
    }

    fn test_resolve<T, E>(input: Vec<T>, expected: E, condition: ResolveCondition)
    where
        T: Resolve + Clone + PartialEq<E> + fmt::Debug,
        E: fmt::Debug,
    {
        assert_eq!(T::resolve(input, condition), expected);
    }

    fn test_resolve_simple<I, E>(input: I, expected: E, condition: ResolveCondition)
    where
        I: IntoIterator,
        I::Item: IntoIterator<Item = i32>,
        E: IntoIterator<Item = i32>,
    {
        test_resolve(simple_input(input), simple_expected(expected), condition);
    }

    fn simple_input<I>(input: I) -> Vec<Vec<Val>>
    where
        I: IntoIterator,
        I::Item: IntoIterator<Item = i32>,
    {
        input
            .into_iter()
            .map(|items| items.into_iter().map(Val).collect())
            .collect()
    }

    fn simple_expected<E>(expected: E) -> Vec<Val>
    where
        E: IntoIterator<Item = i32>,
    {
        expected.into_iter().map(Val).collect()
    }

    #[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
    struct Val(i32);

    impl Resolve for Vec<Val> {
        fn resolve(values: Vec<Self>, condition: ResolveCondition) -> Self {
            let mut resolved = Resolver::resolve(values, |val| val.0, PartialEq::eq, condition);

            resolved.sort_unstable();
            resolved
        }
    }
}
