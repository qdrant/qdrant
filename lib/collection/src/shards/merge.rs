use segment::types::ScoredPoint;

use crate::operations::types::Record;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum MergeCondition {
    All,
    Majority,
}

pub trait Merge: Sized {
    fn merge(responses: &[Self], condition: MergeCondition) -> Self;
}

impl Merge for Vec<Record> {
    fn merge(records: &[Self], condition: MergeCondition) -> Self {
        merge(records, |record| record.id, PartialEq::eq, condition)
    }
}

impl Merge for Vec<Vec<ScoredPoint>> {
    fn merge(batches: &[Self], condition: MergeCondition) -> Self {
        let batch_len = batches.first().map(Vec::len).unwrap_or(0);

        let mut output_batch = Vec::with_capacity(batch_len);

        let mut batches: Vec<_> = batches.iter().map(IntoIterator::into_iter).collect();

        for _ in 0..batch_len {
            let points: Vec<_> = batches
                .iter_mut()
                .filter_map(|batch| {
                    let mut points = batch.next()?.clone();
                    points.sort_unstable_by_key(|point| point.id);
                    Some(points)
                })
                .collect();

            let mut points = merge(&points, |point| point.id, scored_points_eq, condition);
            points.sort_unstable_by(|this, other| this.score.total_cmp(&other.score));

            output_batch.push(points);
        }

        output_batch
    }
}

fn scored_points_eq(this: &ScoredPoint, other: &ScoredPoint) -> bool {
    this.id == other.id
        && this.version == other.version
        && this.vector == other.vector
        && this.payload == other.payload
}

fn merge<'a, I, T, K, Key, E>(items: I, key: K, eq: E, condition: MergeCondition) -> Vec<T>
where
    I: IntoIterator,
    I::Item: IntoIterator<Item = &'a T>,
    T: Clone + 'a,
    K: Fn(&T) -> Key,
    Key: Copy + Ord,
    E: Fn(&T, &T) -> bool,
{
    let mut iter = Iter::new(items, key, condition);

    let input_width = iter.width();
    let input_len_hint = iter.len_hint();

    let mut unique: Vec<(&T, usize)> = Vec::with_capacity(input_width);
    let mut output: Vec<T> = Vec::with_capacity(input_len_hint);

    loop {
        let items = iter.next();

        if items.is_empty() {
            break;
        }

        let mut selected = None;
        let mut max_count = 0;

        for (index, item) in items.into_iter().copied().enumerate() {
            let count = match unique.iter_mut().find(|(other, _)| eq(other, item)) {
                None => {
                    unique.push((item, 1));
                    1
                }

                Some((_, count)) => {
                    *count += 1;
                    *count
                }
            };

            let is_condition_reached = match condition {
                MergeCondition::All => count == input_width,
                MergeCondition::Majority => count > input_width / 2,
            };

            if is_condition_reached {
                selected = Some(item);
                break;
            }

            let is_condition_valid = match condition {
                MergeCondition::All => unique.len() == 1,
                MergeCondition::Majority => {
                    max_count = max_count.max(count);

                    let remaining_items = items.len() - index - 1;
                    let majority = input_width / 2 + 1;

                    max_count + remaining_items >= majority
                }
            };

            if !is_condition_valid {
                break;
            }
        }

        unique.clear();

        if let Some(selected) = selected {
            output.push(selected.clone());
        }
    }

    output
}

#[derive(Clone, Debug)]
struct Iter<I, T, K> {
    iterators: Vec<I>,
    items: Vec<Option<T>>,
    key: K,
    condition: MergeCondition,
    items_with_min_key: Vec<T>,
    indices_with_min_key: Vec<usize>,
}

impl<I, T, K, Key> Iter<I, T, K>
where
    I: Iterator<Item = T>,
    T: Copy,
    K: Fn(T) -> Key,
    Key: Copy + Ord,
{
    pub fn new<II>(iterables: II, key: K, condition: MergeCondition) -> Self
    where
        II: IntoIterator,
        II::Item: IntoIterator<IntoIter = I>,
    {
        let mut iterators: Vec<_> = iterables.into_iter().map(IntoIterator::into_iter).collect();

        let items: Vec<_> = iterators.iter_mut().map(Iterator::next).collect();

        let items_with_min_key = Vec::with_capacity(iterators.len());
        let indices_with_min_key = Vec::with_capacity(iterators.len());

        Self {
            iterators,
            items,
            key,
            condition,
            items_with_min_key,
            indices_with_min_key,
        }
    }

    pub fn width(&self) -> usize {
        self.iterators.len()
    }

    pub fn len_hint(&self) -> usize {
        self.iterators
            .iter()
            .map(|iter| iter.size_hint())
            .fold(0, |len, (min, max)| len.max(max.unwrap_or(min)))
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> &[T] {
        loop {
            for &index in &self.indices_with_min_key {
                self.items[index] = self.iterators[index].next();
            }

            self.items_with_min_key.clear();
            self.indices_with_min_key.clear();

            let mut min_key = None;

            for (index, item) in self.items.iter().copied().enumerate() {
                let item = match item {
                    Some(item) => item,
                    None => continue,
                };

                let key = (self.key)(item);

                let (update_min_key_item, append_min_key_item) = match min_key {
                    Some(min_key) => (key < min_key, key <= min_key),
                    None => (true, true),
                };

                if update_min_key_item {
                    min_key = Some(key);
                    self.items_with_min_key.clear();
                    self.indices_with_min_key.clear();
                }

                if update_min_key_item || append_min_key_item {
                    self.items_with_min_key.push(item);
                    self.indices_with_min_key.push(index);
                }

                // TODO: Break the iteration if it's clear that merge condition can't be met?
                // TODO: Iteration of the loop should be cheap and The check seems way overcomplicated... :/

                /*
                let selected_items = self.items_with_min_key.len();
                let processed_items = index + 1;

                let is_condition_valid = match self.condition {
                    MergeCondition::All => selected_items == processed_items,
                    MergeCondition::Majority => {
                        let total_items = self.items.len();

                        let remaining_items = total_items - processed_items;
                        let majority = total_items / 2 + 1;

                        selected_items + remaining_items >= majority
                    }
                };

                if !is_condition_valid {
                    break;
                }
                */
            }

            if min_key.is_none() {
                break;
            }

            let is_condition_valid = match self.condition {
                MergeCondition::All => self.items_with_min_key.len() == self.items.len(),
                MergeCondition::Majority => self.items_with_min_key.len() > self.items.len() / 2,
            };

            if is_condition_valid {
                break;
            }
        }

        &self.items_with_min_key
    }
}

#[cfg(test)]
mod test {
    use std::fmt;

    use segment::types::{PointIdType, ScoreType, SeqNumberType};

    use super::*;

    #[rustfmt::skip]
    fn merge_scored_points_batch_4_data() -> [Vec<ScoredPoint>; 3] {
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
    fn merge_scored_points_batch_4_input() -> [Vec<Vec<ScoredPoint>>; 4] {
        let [batch1, batch2, batch3] = merge_scored_points_batch_4_data();

        [
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

    fn batch<const N: usize>(
        batch: &Vec<ScoredPoint>,
        mut actions: [Action; N],
    ) -> Vec<ScoredPoint> {
        let mut batch = batch.clone();

        actions.sort_unstable_by_key(Action::index);

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
                    batch[index - offset].version = 2;
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
        pub fn index(&self) -> usize {
            match self {
                &Self::Remove(index) => index,
                &Self::Modify(index) => index,
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
    fn merge_scored_points_batch_4_all() {
        let [mut batch1, mut batch2, mut batch3] = merge_scored_points_batch_4_data();

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

        test_merge(
            &merge_scored_points_batch_4_input(),
            [batch1, batch2, batch3],
            MergeCondition::All,
        );
    }

    #[test]
    fn merge_scored_points_batch_4_majority() {
        let [mut batch1, mut batch2, mut batch3] = merge_scored_points_batch_4_data();

        batch1.remove(3);
        batch1.remove(1);

        batch2.remove(3);
        batch2.remove(0);

        batch3.remove(5);

        test_merge(
            &merge_scored_points_batch_4_input(),
            [batch1, batch2, batch3],
            MergeCondition::Majority,
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

    #[test]
    fn merge_1_all() {
        test_merge_1(MergeCondition::All);
    }

    #[test]
    fn merge_1_majority() {
        test_merge_1(MergeCondition::Majority);
    }

    fn test_merge_1(condition: MergeCondition) {
        let data: Vec<_> = data_1().into_iter().map(Val).collect();

        let input = [data.clone()];
        let expected = data;

        test_merge(&input, expected, condition);
    }

    #[test]
    fn merge_2_all() {
        test_merge(
            &merge_input(input_2()),
            merge_expected(expected_2()),
            MergeCondition::All,
        );
    }

    #[test]
    fn merge_2_majority() {
        test_merge(
            &merge_input(input_2()),
            merge_expected(expected_2()),
            MergeCondition::Majority,
        );
    }

    #[test]
    fn merge_3_all() {
        test_merge(
            &merge_input(input_3()),
            merge_expected(expected_3_all()),
            MergeCondition::All,
        );
    }

    #[test]
    fn merge_3_majority() {
        test_merge(
            &merge_input(input_3()),
            merge_expected(expected_3_majority()),
            MergeCondition::Majority,
        );
    }

    #[test]
    fn merge_4_all() {
        test_merge(
            &merge_input(input_4()),
            merge_expected(expected_4_all()),
            MergeCondition::All,
        );
    }

    #[test]
    fn merge_4_majority() {
        test_merge(
            &merge_input(input_4()),
            merge_expected(expected_4_majority()),
            MergeCondition::Majority,
        );
    }

    fn merge_input<I>(input: I) -> Vec<Vec<Val>>
    where
        I: IntoIterator,
        I::Item: IntoIterator<Item = i32>,
    {
        input
            .into_iter()
            .map(|items| items.into_iter().map(Val).collect())
            .collect()
    }

    fn merge_expected<E>(expected: E) -> Vec<Val>
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

    impl Merge for Vec<Val> {
        fn merge(values: &[Self], condition: MergeCondition) -> Self {
            merge(values, |val| val.0, PartialEq::eq, condition)
        }
    }

    fn test_merge<'a, T, E>(input: &[T], expected: E, condition: MergeCondition)
    where
        T: Merge + Clone + PartialEq<E> + fmt::Debug,
        E: fmt::Debug,
    {
        assert_eq!(T::merge(input, condition), expected);
    }

    #[test]
    fn iter_1_all() {
        test_iter_1(MergeCondition::All);
    }

    #[test]
    fn iter_1_majority() {
        test_iter_1(MergeCondition::Majority);
    }

    fn test_iter_1(condition: MergeCondition) {
        let input = [data_1()];
        let expected: Vec<_> = data_1().into_iter().map(|item| [item]).collect();

        test_iter(input, expected, condition);
    }

    #[test]
    fn iter_2_all() {
        test_iter(input_2(), expected_2(), MergeCondition::All);
    }

    #[test]
    fn iter_2_majority() {
        test_iter(input_2(), expected_2(), MergeCondition::Majority);
    }

    #[test]
    fn iter_3_all() {
        test_iter(input_3(), expected_3_all(), MergeCondition::All);
    }

    #[test]
    fn iter_3_majority() {
        test_iter(input_3(), expected_3_majority(), MergeCondition::Majority);
    }

    #[test]
    fn iter_4_all() {
        test_iter(input_4(), expected_4_all(), MergeCondition::All);
    }

    #[test]
    fn iter_4_majority() {
        test_iter(input_4(), expected_4_majority(), MergeCondition::Majority);
    }

    fn test_iter<I, E>(input: I, expected: E, condition: MergeCondition)
    where
        I: IntoIterator,
        I::Item: IntoIterator,
        NestedItem<I>: Copy + Eq + Ord + fmt::Debug,
        E: IntoIterator + fmt::Debug,
        E::Item: AsRef<[NestedItem<I>]>,
        Vec<Vec<NestedItem<I>>>: PartialEq<E>,
    {
        let mut iter = Iter::new(input, |item| item, condition);
        let mut output: Vec<Vec<_>> = Vec::with_capacity(iter.len_hint());

        loop {
            match iter.next() {
                [] => break,
                items => output.push(items.into()),
            }
        }

        assert_eq!(output, expected);
    }

    type NestedItem<I> = <<I as IntoIterator>::Item as IntoIterator>::Item;
}
