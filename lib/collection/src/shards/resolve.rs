use std::collections::{HashMap, HashSet};

use segment::types::{PointIdType, ScoredPoint};
use tinyvec::TinyVec;

use crate::operations::types::Record;

const VEC_CAPACITY: usize = 5; // Expected number of replicas

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ResolveCondition {
    All,
    Majority,
}

struct ResolverRecord<'a, T> {
    item: Option<&'a T>,
    row: usize,
    index: usize,
    number: usize,
}

impl<'a, T> Default for ResolverRecord<'a, T> {
    fn default() -> Self {
        Self {
            item: None,
            row: 0,
            index: 0,
            number: 0,
        }
    }
}

impl<'a, T> ResolverRecord<'a, T> {
    fn new(item: &'a T, row: usize, index: usize) -> Self {
        Self {
            item: Some(item),
            row,
            index,
            number: 1,
        }
    }
}

struct Resolver<'a, T> {
    items: HashMap<PointIdType, TinyVec<[ResolverRecord<'a, T>; VEC_CAPACITY]>>,
}

impl<'a, T> Resolver<'a, T> {
    fn new() -> Self {
        Self {
            items: HashMap::new(),
        }
    }

    fn add<CMP>(
        &mut self,
        point_id: &PointIdType,
        item: &'a T,
        row: usize,
        index: usize,
        compare: &CMP,
    ) where
        CMP: Fn(&T, &T) -> bool,
    {
        let points = self.items.entry(*point_id).or_insert_with(TinyVec::new);
        for point in points.iter_mut() {
            if compare(point.item.unwrap(), item) {
                point.number += 1;
                return;
            }
        }
        points.push(ResolverRecord::new(item, row, index));
    }

    fn add_all<CMP, ID>(&mut self, items: &'a [Vec<T>], compare: &CMP, get_id: &ID)
    where
        CMP: Fn(&T, &T) -> bool,
        ID: Fn(&T) -> PointIdType,
    {
        for (row, batch) in items.iter().enumerate() {
            for (index, item) in batch.iter().enumerate() {
                self.add(&get_id(item), item, row, index, compare);
            }
        }
    }

    pub fn resolve<CMP, ID>(
        items: Vec<Vec<T>>,
        compare: &CMP,
        get_id: &ID,
        condition: ResolveCondition,
    ) -> Vec<T>
    where
        CMP: Fn(&T, &T) -> bool,
        ID: Fn(&T) -> PointIdType,
    {
        let required_number = match condition {
            ResolveCondition::All => items.len(),
            ResolveCondition::Majority => items.len() / 2 + 1,
        };

        let mut resolver = Resolver::new();
        resolver.add_all(&items, compare, get_id);

        // Select coordinates of accepted items, avoiding copying
        let accepted_ids: HashSet<_> = resolver
            .items
            .into_iter()
            .filter_map(|(_, points)| {
                points
                    .into_iter()
                    .find(|point| point.number >= required_number)
                    .map(|point| (point.row, point.index))
            })
            .collect();

        // Shortcut if everything is consistent: return first items, avoiding filtering
        let have_inconsistency =
            accepted_ids.len() < items[0].len() || accepted_ids.iter().any(|(row, _)| *row > 0);
        if have_inconsistency {
            items
                .into_iter()
                .enumerate()
                .flat_map(|(row_id, row)| row.into_iter().map(move |x| (row_id, x)).enumerate())
                .filter(|(index, (row, _))| accepted_ids.contains(&(*row, *index)))
                .map(|(_, (_, item))| item)
                .collect()
        } else {
            items.into_iter().next().unwrap()
        }
    }
}

pub trait Resolve: Sized {
    fn resolve(responses: Vec<Self>, condition: ResolveCondition) -> Self;
}

impl Resolve for Vec<Record> {
    fn resolve(records: Vec<Self>, condition: ResolveCondition) -> Self {
        let mut res = Resolver::resolve(records, &PartialEq::eq, &|x| x.id, condition);
        res.sort_unstable_by_key(|x| x.id);
        res
    }
}

fn transpose2<T>(v: Vec<Vec<T>>) -> Vec<Vec<T>> {
    assert!(!v.is_empty());
    let len = v[0].len();
    let mut iters: Vec<_> = v.into_iter().map(|n| n.into_iter()).collect();
    (0..len)
        .map(|_| {
            iters
                .iter_mut()
                .map(|n| n.next().unwrap())
                .collect::<Vec<T>>()
        })
        .collect()
}

impl Resolve for Vec<Vec<ScoredPoint>> {
    fn resolve(batches: Vec<Self>, condition: ResolveCondition) -> Self {
        // batches: <replica_id, <batch_id, ScoredPoint>>
        // transpose to <batch_id, <replica_id, ScoredPoint>>

        let batches = transpose2(batches);

        batches
            .into_iter()
            .map(|records| {
                let mut res = Resolver::resolve(records, &scored_points_eq, &|x| x.id, condition);
                res.sort_unstable();
                res
            })
            .collect()
    }
}

fn scored_points_eq(this: &ScoredPoint, other: &ScoredPoint) -> bool {
    this.id == other.id
        && this.score == other.score
        && this.vector == other.vector
        && this.payload == other.payload
}

#[cfg(test)]
mod test {
    use std::fmt;

    use segment::types::ScoreType;

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

    fn data_1() -> [i32; 9] {
        [1, 2, 3, 4, 5, 6, 7, 8, 9]
    }

    #[rustfmt::skip]
    fn input_2() -> [Vec<i32>; 2] {
        [
            vec![1, 2, 3,       6, 7,    9,     11, 12, 13],
            vec![      3, 4, 5, 6,    8,    10, 11,       ],
        ]
    }

    fn expected_2() -> [[i32; 2]; 3] {
        [[3; 2], [6; 2], [11; 2]]
    }

    #[rustfmt::skip]
    fn input_3() -> [Vec<i32>; 3] {
        [
            vec![1, 2,          6, 7, 8,       11,      13, 14, 15,       ],
            vec![   2, 3, 4,       7,    9, 10,         13, 14,     16,   ],
            vec![         4, 5, 6, 7,    9,    11, 12,      14,         17],
        ]
    }

    fn expected_3_all() -> [[i32; 3]; 2] {
        [[7; 3], [14; 3]]
    }

    #[rustfmt::skip]
    fn expected_3_majority() -> [Vec<i32>; 8] {
        [
            vec![2; 2], vec![4; 2],  vec![6; 2],  vec![7; 3],
            vec![9; 2], vec![11; 2], vec![13; 2], vec![14; 3]
        ]
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

    fn expected_4_all() -> [[i32; 4]; 2] {
        [[13; 4], [27; 4]]
    }

    #[rustfmt::skip]
    fn expected_4_majority() -> [Vec<i32>; 10] {
        [
            vec![3; 3],  vec![6; 3],  vec![9; 3],  vec![12; 3], vec![13; 4],
            vec![16; 3], vec![19; 3], vec![22; 3], vec![26; 3], vec![27; 4],
        ]
    }

    #[rustfmt::skip]
    fn input_5() -> [Vec<i32>; 3] {
        [
            vec![1, 2, 3, 4, 5],
            vec![1, 2, 3, 4, 5],
            vec![1, 2, 3, 4, 5],
        ]
    }

    fn expected_5_all() -> [[i32; 3]; 5] {
        [[1; 3], [2; 3], [3; 3], [4; 3], [5; 3]]
    }

    fn expected_5_majority() -> [Vec<i32>; 5] {
        [vec![1; 2], vec![2; 2], vec![3; 2], vec![4; 2], vec![5; 2]]
    }

    #[test]
    fn resolve_1_all() {
        resolve_1(ResolveCondition::All);
    }

    #[test]
    fn resolve_1_majority() {
        resolve_1(ResolveCondition::Majority);
    }

    fn resolve_1(condition: ResolveCondition) {
        let data: Vec<_> = data_1().into_iter().map(Val).collect();

        let input = vec![data.clone()];
        let expected = data;

        test_resolve(input, expected, condition);
    }

    #[test]
    fn resolve_2_all() {
        test_resolve(
            resolve_input(input_2()),
            resolve_expected(expected_2()),
            ResolveCondition::All,
        );
    }

    #[test]
    fn resolve_2_majority() {
        test_resolve(
            resolve_input(input_2()),
            resolve_expected(expected_2()),
            ResolveCondition::Majority,
        );
    }

    #[test]
    fn resolve_3_all() {
        test_resolve(
            resolve_input(input_3()),
            resolve_expected(expected_3_all()),
            ResolveCondition::All,
        );
    }

    #[test]
    fn resolve_3_majority() {
        test_resolve(
            resolve_input(input_3()),
            resolve_expected(expected_3_majority()),
            ResolveCondition::Majority,
        );
    }

    #[test]
    fn resolve_4_all() {
        test_resolve(
            resolve_input(input_4()),
            resolve_expected(expected_4_all()),
            ResolveCondition::All,
        );
    }

    #[test]
    fn resolve_4_majority() {
        test_resolve(
            resolve_input(input_4()),
            resolve_expected(expected_4_majority()),
            ResolveCondition::Majority,
        );
    }

    #[test]
    fn resolve_5_all() {
        test_resolve(
            resolve_input(input_5()),
            resolve_expected(expected_5_all()),
            ResolveCondition::All,
        );
    }

    #[test]
    fn resolve_5_majority() {
        test_resolve(
            resolve_input(input_5()),
            resolve_expected(expected_5_majority()),
            ResolveCondition::Majority,
        );
    }

    fn resolve_input<I>(input: I) -> Vec<Vec<Val>>
    where
        I: IntoIterator,
        I::Item: IntoIterator<Item = i32>,
    {
        input
            .into_iter()
            .map(|items| items.into_iter().map(Val).collect())
            .collect()
    }

    fn resolve_expected<E>(expected: E) -> Vec<Val>
    where
        E: IntoIterator,
        E::Item: IntoIterator<Item = i32>,
    {
        expected
            .into_iter()
            .map(|items| items.into_iter().map(Val).next().unwrap())
            .collect()
    }

    #[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
    struct Val(i32);

    impl Resolve for Vec<Val> {
        fn resolve(values: Vec<Self>, condition: ResolveCondition) -> Self {
            let mut res = Resolver::resolve(
                values,
                &PartialEq::eq,
                &|x| PointIdType::NumId(x.0 as u64),
                condition,
            );
            res.sort_unstable();
            res
        }
    }

    fn test_resolve<T, E>(input: Vec<T>, expected: E, condition: ResolveCondition)
    where
        T: Resolve + Clone + PartialEq<E> + fmt::Debug,
        E: fmt::Debug,
    {
        assert_eq!(T::resolve(input, condition), expected);
    }
}
