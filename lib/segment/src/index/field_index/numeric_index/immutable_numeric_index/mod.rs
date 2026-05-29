use std::ops::Bound;

use common::bitvec::{BitSliceExt as _, BitVec};

use super::Encodable;
use super::universal_numeric_index::UniversalNumericIndex;
use crate::index::field_index::histogram::Histogram;
use crate::index::field_index::immutable_point_to_values::ImmutablePointToValues;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::stored_point_to_values::StoredValue;

mod lifecycle;
mod read_ops;

pub struct ImmutableNumericIndex<T: Encodable + Numericable + StoredValue + Default> {
    pub(super) map: NumericKeySortedVec<T>,
    pub(super) histogram: Histogram<T>,
    pub(super) points_count: usize,
    pub(super) max_values_per_point: usize,
    pub(super) point_to_values: ImmutablePointToValues<T>,
    // Backing storage, source of state, persists deletions
    pub(super) storage: Box<UniversalNumericIndex<T>>,
    /// Snapshot of approximate RAM usage at construction time.
    /// Not refreshed on `remove_point`.
    pub(super) cached_ram_usage_bytes: usize,
}

pub(super) struct NumericKeySortedVec<T: Encodable + Numericable> {
    data: Vec<Point<T>>,
    deleted: BitVec,
    deleted_count: usize,
}

pub(super) struct NumericKeySortedVecIterator<'a, T: Encodable + Numericable> {
    set: &'a NumericKeySortedVec<T>,
    pub(super) start_index: usize,
    pub(super) end_index: usize,
}

impl<T: Encodable + Numericable> NumericKeySortedVec<T> {
    pub(super) fn ram_usage_bytes(&self) -> usize {
        let Self {
            data,
            deleted,
            deleted_count: _,
        } = self;
        data.capacity() * size_of::<Point<T>>() + deleted.capacity().div_ceil(u8::BITS as usize)
    }

    pub(super) fn from_btree_set(map: std::collections::BTreeSet<Point<T>>) -> Self {
        let result = Self {
            deleted: BitVec::repeat(false, map.len()),
            data: map.into_iter().collect(),
            deleted_count: 0,
        };
        // Invariant relied on by the iterators (see `next` / `next_back`).
        debug_assert_eq!(result.deleted.len(), result.data.len());
        result
    }

    pub(super) fn len(&self) -> usize {
        self.data.len() - self.deleted_count
    }

    pub(super) fn remove(&mut self, key: &Point<T>) -> bool {
        if let Ok(index) = self.data.binary_search(key)
            && let Some(is_deleted) = self.deleted.get_mut(index).as_deref_mut()
        {
            if !*is_deleted {
                self.deleted_count += 1;
                *is_deleted = true;
            }
            return true;
        }
        false
    }

    pub(super) fn values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> NumericKeySortedVecIterator<'_, T> {
        let start_index = self.find_start_index(start_bound);
        let end_index = self.find_end_index(start_index, end_bound);
        NumericKeySortedVecIterator {
            set: self,
            start_index,
            end_index,
        }
    }

    pub(super) fn find_start_index(&self, bound: Bound<Point<T>>) -> usize {
        match bound {
            Bound::Included(bound) => self.data.binary_search(&bound).unwrap_or_else(|idx| idx),
            Bound::Excluded(bound) => match self.data.binary_search(&bound) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            },
            Bound::Unbounded => 0,
        }
    }

    pub(super) fn find_end_index(&self, start: usize, bound: Bound<Point<T>>) -> usize {
        if start >= self.data.len() {
            // the range `end` should never be less than `start`
            return start;
        }
        match bound {
            Bound::Included(bound) => match self.data[start..].binary_search(&bound) {
                Ok(idx) => idx + 1 + start,
                Err(idx) => idx + start,
            },
            Bound::Excluded(bound) => {
                let end_bound = self.data[start..].binary_search(&bound);
                end_bound.unwrap_or_else(|idx| idx) + start
            }
            Bound::Unbounded => self.data.len(),
        }
    }
}

impl<T: Encodable + Numericable> Iterator for NumericKeySortedVecIterator<'_, T> {
    type Item = Point<T>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.start_index < self.end_index {
            let key = self.set.data[self.start_index];
            let deleted = self.set.deleted.get_bit(self.start_index).unwrap_or(true);
            self.start_index += 1;
            if deleted {
                continue;
            }
            return Some(key);
        }
        None
    }
}

impl<T: Encodable + Numericable> DoubleEndedIterator for NumericKeySortedVecIterator<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        while self.start_index < self.end_index {
            let key = self.set.data[self.end_index - 1];
            let deleted = self.set.deleted.get_bit(self.end_index - 1).unwrap_or(true);
            self.end_index -= 1;
            if deleted {
                continue;
            }
            return Some(key);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::ops::Bound;

    use super::*;
    use crate::types::FloatPayloadType;

    fn check_range(
        key_set: &NumericKeySortedVec<FloatPayloadType>,
        encoded_map: &BTreeSet<Point<FloatPayloadType>>,
        start_bound: Bound<Point<FloatPayloadType>>,
        end_bound: Bound<Point<FloatPayloadType>>,
    ) {
        let set1 = key_set
            .values_range(start_bound, end_bound)
            .collect::<Vec<_>>();

        let set2 = encoded_map
            .range((start_bound, end_bound))
            .cloned()
            .collect::<Vec<_>>();

        for (k1, k2) in set1.iter().zip(set2.iter()) {
            assert_eq!(k1, k2);
        }
    }

    fn check_ranges(
        key_set: &NumericKeySortedVec<FloatPayloadType>,
        encoded_map: &BTreeSet<Point<FloatPayloadType>>,
    ) {
        check_range(key_set, encoded_map, Bound::Unbounded, Bound::Unbounded);
        check_range(
            key_set,
            encoded_map,
            Bound::Unbounded,
            Bound::Included(Point::new(0.4, 2)),
        );
        check_range(
            key_set,
            encoded_map,
            Bound::Unbounded,
            Bound::Excluded(Point::new(0.4, 2)),
        );
        check_range(
            key_set,
            encoded_map,
            Bound::Included(Point::new(0.4, 2)),
            Bound::Unbounded,
        );
        check_range(
            key_set,
            encoded_map,
            Bound::Excluded(Point::new(0.4, 2)),
            Bound::Unbounded,
        );
        check_range(
            key_set,
            encoded_map,
            Bound::Included(Point::new(-5.0, 1)),
            Bound::Included(Point::new(5.0, 1)),
        );
        check_range(
            key_set,
            encoded_map,
            Bound::Included(Point::new(-5.0, 1)),
            Bound::Excluded(Point::new(5.0, 1)),
        );
        check_range(
            key_set,
            encoded_map,
            Bound::Excluded(Point::new(-5.0, 1)),
            Bound::Included(Point::new(5.0, 1)),
        );
        check_range(
            key_set,
            encoded_map,
            Bound::Excluded(Point::new(-5.0, 1)),
            Bound::Excluded(Point::new(5.0, 1)),
        );
        check_range(
            key_set,
            encoded_map,
            Bound::Included(Point::new(-5.0, 1000)),
            Bound::Included(Point::new(5.0, 1000)),
        );
        check_range(
            key_set,
            encoded_map,
            Bound::Excluded(Point::new(-5.0, 1000)),
            Bound::Excluded(Point::new(5.0, 1000)),
        );
        check_range(
            key_set,
            encoded_map,
            Bound::Excluded(Point::new(-50000.0, 1000)),
            Bound::Excluded(Point::new(50000.0, 1000)),
        );
    }

    #[test]
    fn test_numeric_index_key_set() {
        let pairs = [
            Point::new(0.0, 1),
            Point::new(0.0, 3),
            Point::new(-0.0, 2),
            Point::new(-0.0, 4),
            Point::new(0.4, 2),
            Point::new(-0.4, 3),
            Point::new(5.0, 1),
            Point::new(-5.0, 1),
            Point::new(f64::INFINITY, 0),
            Point::new(f64::NEG_INFINITY, 1),
            Point::new(f64::NEG_INFINITY, 2),
            Point::new(f64::NEG_INFINITY, 3),
        ];

        let mut set_byte: BTreeSet<Point<FloatPayloadType>> = pairs.iter().cloned().collect();
        let mut set_keys =
            NumericKeySortedVec::<FloatPayloadType>::from_btree_set(set_byte.clone());

        check_ranges(&set_keys, &set_byte);

        // test deletion and ranges after deletion
        let deleted_key = Point::new(0.4, 2);
        set_keys.remove(&deleted_key);
        set_byte.remove(&deleted_key);

        check_ranges(&set_keys, &set_byte);

        // test deletion and ranges after deletion
        let deleted_key = Point::new(-5.0, 1);
        set_keys.remove(&deleted_key);
        set_byte.remove(&deleted_key);

        check_ranges(&set_keys, &set_byte);
    }
}
