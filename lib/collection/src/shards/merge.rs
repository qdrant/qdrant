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
        merge(records, |record| record.id, cmp::PartialEq::eq, condition)
    }
}

impl Merge for Vec<Vec<ScoredPoint>> {
    fn merge(batches: &[Self], condition: MergeCondition) -> Self {
        let batch_len = batches.first().map(Vec::len).unwrap_or(0);

        let mut output_batch = Vec::with_capacity(batch_len);

        let mut batches: Vec<_> = batches.iter().map(IntoIterator::into_iter).collect();

        for _ in 0..batch_len {
            let points = batches
                .iter_mut()
                .map(Iterator::next)
                .filter_map(|points| points);

            let points = merge(points, |point| point.id, scored_points_eq, condition);

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

        for &item in items {
            match unique.iter_mut().find(|(other, _)| eq(other, item)) {
                Some((_, count)) => {
                    *count += 1;

                    let is_condition_met = match condition {
                        MergeCondition::All => *count == input_width,
                        MergeCondition::Majority => *count > input_width / 2,
                    };

                    if is_condition_met {
                        selected = Some(item);
                        break;
                    }
                }

                None => {
                    unique.push((item, 1));

                    if condition == MergeCondition::All {
                        break;
                    }
                }
            }
        }

        if let Some(selected) = selected {
            output.push(selected.clone());
        }
    }

    output
}

#[derive(Clone, Debug)]
pub struct Iter<I, T, K> {
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
                // TODO: The check seems way overcomplicated... :/

                /*
                let selected_items = self.items_with_min_key.len();

                let is_condition_valid = match self.condition {
                    MergeCondition::All => selected_items > index,
                    MergeCondition::Majority => {
                        let total_items = self.items.len();

                        let remaining_items = total_items - index - 1;
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
