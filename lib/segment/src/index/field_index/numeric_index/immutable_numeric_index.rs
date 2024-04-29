use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;

use super::mutable_numeric_index::MutableNumericIndex;
use super::{Encodable, NumericIndex, HISTOGRAM_MAX_BUCKET_SIZE, HISTOGRAM_PRECISION};
use crate::common::operation_error::OperationResult;
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::index::field_index::histogram::{Histogram, Numericable, Point};
use crate::index::field_index::immutable_point_to_values::ImmutablePointToValues;

pub struct ImmutableNumericIndex<T: Encodable + Numericable + Default> {
    map: NumericKeySortedVec<T>,
    db_wrapper: DatabaseColumnWrapper,
    pub(super) histogram: Histogram<T>,
    pub(super) points_count: usize,
    pub(super) max_values_per_point: usize,
    point_to_values: ImmutablePointToValues<T>,
}

#[derive(Clone, PartialEq, Debug)]
pub(super) struct NumericIndexKey<T> {
    key: T,
    idx: PointOffsetType,
    deleted: bool,
}

struct NumericKeySortedVec<T: Encodable + Numericable> {
    data: Vec<NumericIndexKey<T>>,
    deleted_count: usize,
}

pub(super) struct NumericKeySortedVecIterator<'a, T: Encodable + Numericable> {
    set: &'a NumericKeySortedVec<T>,
    start_index: usize,
    end_index: usize,
}

impl<T: PartialEq + PartialOrd + Encodable + Numericable> NumericIndexKey<T> {
    pub(super) fn new(key: T, idx: PointOffsetType) -> Self {
        Self {
            key,
            idx,
            deleted: false,
        }
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        T::encode_key(&self.key, self.idx)
    }

    pub(super) fn decode(bytes: &[u8]) -> Self {
        let (idx, key) = T::decode_key(bytes);
        Self {
            key,
            idx,
            deleted: false,
        }
    }
}

impl<T> From<NumericIndexKey<T>> for Point<T> {
    fn from(key: NumericIndexKey<T>) -> Self {
        Point {
            val: key.key,
            idx: key.idx as usize,
        }
    }
}

impl<T> From<Point<T>> for NumericIndexKey<T> {
    fn from(key: Point<T>) -> Self {
        NumericIndexKey {
            key: key.val,
            idx: key.idx as PointOffsetType,
            deleted: false,
        }
    }
}

impl<T: PartialEq + PartialOrd + Encodable> PartialOrd for NumericIndexKey<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: PartialEq + PartialOrd + Encodable> Ord for NumericIndexKey<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.key.cmp_encoded(&other.key) {
            std::cmp::Ordering::Equal => self.idx.cmp(&other.idx),
            ord => ord,
        }
    }
}

impl<T: PartialEq + PartialOrd + Encodable> Eq for NumericIndexKey<T> {}

impl<T: Encodable + Numericable> NumericKeySortedVec<T> {
    fn from_btree_map(map: BTreeMap<Vec<u8>, u32>) -> Self {
        Self {
            data: map
                .keys()
                .cloned()
                .map(|b| NumericIndexKey::<T>::decode(&b))
                .collect(),
            deleted_count: 0,
        }
    }

    fn len(&self) -> usize {
        self.data.len() - self.deleted_count
    }

    fn remove(&mut self, key: NumericIndexKey<T>) -> bool {
        if let Ok(index) = self.data.binary_search(&key) {
            self.data[index].deleted = true;
            self.deleted_count += 1;
            true
        } else {
            false
        }
    }

    fn values_range(
        &self,
        start_bound: Bound<NumericIndexKey<T>>,
        end_bound: Bound<NumericIndexKey<T>>,
    ) -> NumericKeySortedVecIterator<'_, T> {
        let start_index = self.find_start_index(start_bound);
        let end_index = self.find_end_index(start_index, end_bound);
        NumericKeySortedVecIterator {
            set: self,
            start_index,
            end_index,
        }
    }

    fn find_start_index(&self, bound: Bound<NumericIndexKey<T>>) -> usize {
        match bound {
            Bound::Included(bound) => self.data.binary_search(&bound).unwrap_or_else(|idx| idx),
            Bound::Excluded(bound) => match self.data.binary_search(&bound) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            },
            Bound::Unbounded => 0,
        }
    }

    fn find_end_index(&self, start: usize, bound: Bound<NumericIndexKey<T>>) -> usize {
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

impl<'a, T: Encodable + Numericable> Iterator for NumericKeySortedVecIterator<'a, T> {
    type Item = NumericIndexKey<T>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.start_index < self.end_index {
            let key = self.set.data[self.start_index].clone();
            self.start_index += 1;
            if key.deleted {
                continue;
            }
            return Some(key);
        }
        None
    }
}

impl<'a, T: Encodable + Numericable> DoubleEndedIterator for NumericKeySortedVecIterator<'a, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        while self.start_index < self.end_index {
            let key = self.set.data[self.end_index - 1].clone();
            self.end_index -= 1;
            if key.deleted {
                continue;
            }
            return Some(key);
        }
        None
    }
}

impl<T: Encodable + Numericable + Default> ImmutableNumericIndex<T> {
    pub(super) fn new(db: Arc<RwLock<DB>>, field: &str) -> Self {
        let store_cf_name = NumericIndex::<T>::storage_cf_name(field);
        let db_wrapper = DatabaseColumnWrapper::new(db, &store_cf_name);
        Self {
            map: NumericKeySortedVec {
                data: Default::default(),
                deleted_count: 0,
            },
            db_wrapper,
            histogram: Histogram::new(HISTOGRAM_MAX_BUCKET_SIZE, HISTOGRAM_PRECISION),
            points_count: 0,
            max_values_per_point: 1,
            point_to_values: Default::default(),
        }
    }

    pub(super) fn get_db_wrapper(&self) -> &DatabaseColumnWrapper {
        &self.db_wrapper
    }

    pub(super) fn get_values(&self, idx: PointOffsetType) -> Option<&[T]> {
        self.point_to_values.get_values(idx)
    }

    pub(super) fn get_values_count(&self) -> usize {
        self.map.len()
    }

    pub(super) fn values_range(
        &self,
        start_bound: Bound<NumericIndexKey<T>>,
        end_bound: Bound<NumericIndexKey<T>>,
    ) -> impl Iterator<Item = PointOffsetType> + '_ {
        self.map
            .values_range(start_bound, end_bound)
            .map(|NumericIndexKey { idx, .. }| idx)
    }

    pub(super) fn orderable_values_range(
        &self,
        start_bound: Bound<NumericIndexKey<T>>,
        end_bound: Bound<NumericIndexKey<T>>,
    ) -> impl DoubleEndedIterator<Item = (T, PointOffsetType)> + '_ {
        self.map
            .values_range(start_bound, end_bound)
            .map(|NumericIndexKey { key, idx, .. }| (key, idx))
    }

    pub(super) fn load(&mut self) -> OperationResult<bool> {
        let mut mutable = MutableNumericIndex::<T> {
            map: Default::default(),
            db_wrapper: self.db_wrapper.clone(),
            histogram: Histogram::new(HISTOGRAM_MAX_BUCKET_SIZE, HISTOGRAM_PRECISION),
            points_count: 0,
            max_values_per_point: 0,
            point_to_values: Default::default(),
        };
        mutable.load()?;
        let MutableNumericIndex {
            map,
            histogram,
            points_count,
            max_values_per_point,
            point_to_values,
            ..
        } = mutable;

        self.map = NumericKeySortedVec::from_btree_map(map);
        self.histogram = histogram;
        self.points_count = points_count;
        self.max_values_per_point = max_values_per_point;

        self.point_to_values = ImmutablePointToValues::new(point_to_values);

        Ok(true)
    }

    pub(super) fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        if let Some(removed_values) = self.point_to_values.get_values(idx) {
            if !removed_values.is_empty() {
                self.points_count -= 1;
            }

            for value in removed_values {
                let key = NumericIndexKey::new(*value, idx);
                Self::remove_from_map(&mut self.map, &mut self.histogram, key);

                // update db
                let encoded = value.encode_key(idx);
                self.db_wrapper.remove(encoded)?;
            }
        }
        self.point_to_values.remove_point(idx);
        Ok(())
    }

    fn remove_from_map(
        map: &mut NumericKeySortedVec<T>,
        histogram: &mut Histogram<T>,
        key: NumericIndexKey<T>,
    ) {
        if map.remove(key.clone()) {
            histogram.remove(
                &key.into(),
                |x| Self::get_histogram_left_neighbor(map, x),
                |x| Self::get_histogram_right_neighbor(map, x),
            );
        }
    }

    fn get_histogram_left_neighbor(
        map: &NumericKeySortedVec<T>,
        point: &Point<T>,
    ) -> Option<Point<T>> {
        let key: NumericIndexKey<T> = point.clone().into();
        map.values_range(Bound::Unbounded, Bound::Excluded(key))
            .next_back()
            .map(|key| key.into())
    }

    fn get_histogram_right_neighbor(
        map: &NumericKeySortedVec<T>,
        point: &Point<T>,
    ) -> Option<Point<T>> {
        let key: NumericIndexKey<T> = point.clone().into();
        map.values_range(Bound::Excluded(key), Bound::Unbounded)
            .next()
            .map(|key| key.into())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::ops::Bound;
    use std::ops::Bound::{Excluded, Included, Unbounded};

    use super::*;
    use crate::types::FloatPayloadType;

    fn check_range(
        key_set: &NumericKeySortedVec<FloatPayloadType>,
        encoded_map: &BTreeMap<Vec<u8>, PointOffsetType>,
        start_bound: Bound<NumericIndexKey<FloatPayloadType>>,
        end_bound: Bound<NumericIndexKey<FloatPayloadType>>,
    ) {
        let set1 = key_set
            .values_range(start_bound.clone(), end_bound.clone())
            .collect::<Vec<_>>();

        let start_encoded_bound = match start_bound {
            Included(k) => Included(k.encode()),
            Excluded(k) => Excluded(k.encode()),
            Unbounded => Unbounded,
        };
        let end_encoded_bound = match end_bound {
            Included(k) => Included(k.encode()),
            Excluded(k) => Excluded(k.encode()),
            Unbounded => Unbounded,
        };
        let set2 = encoded_map
            .range((start_encoded_bound, end_encoded_bound))
            .map(|(b, _)| NumericIndexKey::<FloatPayloadType>::decode(b))
            .collect::<Vec<_>>();

        for (k1, k2) in set1.iter().zip(set2.iter()) {
            if k1.key.is_nan() && k2.key.is_nan() {
                assert_eq!(k1.idx, k2.idx);
            } else {
                assert_eq!(k1, k2);
            }
        }
    }

    fn check_ranges(
        key_set: &NumericKeySortedVec<FloatPayloadType>,
        encoded_map: &BTreeMap<Vec<u8>, PointOffsetType>,
    ) {
        check_range(key_set, encoded_map, Bound::Unbounded, Bound::Unbounded);
        check_range(
            key_set,
            encoded_map,
            Bound::Unbounded,
            Bound::Included(NumericIndexKey::new(0.4, 2)),
        );
        check_range(
            key_set,
            encoded_map,
            Bound::Unbounded,
            Bound::Excluded(NumericIndexKey::new(0.4, 2)),
        );
        check_range(
            key_set,
            encoded_map,
            Bound::Included(NumericIndexKey::new(0.4, 2)),
            Bound::Unbounded,
        );
        check_range(
            key_set,
            encoded_map,
            Bound::Excluded(NumericIndexKey::new(0.4, 2)),
            Bound::Unbounded,
        );
        check_range(
            key_set,
            encoded_map,
            Bound::Included(NumericIndexKey::new(-5.0, 1)),
            Bound::Included(NumericIndexKey::new(5.0, 1)),
        );
        check_range(
            key_set,
            encoded_map,
            Bound::Included(NumericIndexKey::new(-5.0, 1)),
            Bound::Excluded(NumericIndexKey::new(5.0, 1)),
        );
        check_range(
            key_set,
            encoded_map,
            Bound::Excluded(NumericIndexKey::new(-5.0, 1)),
            Bound::Included(NumericIndexKey::new(5.0, 1)),
        );
        check_range(
            key_set,
            encoded_map,
            Bound::Excluded(NumericIndexKey::new(-5.0, 1)),
            Bound::Excluded(NumericIndexKey::new(5.0, 1)),
        );
        check_range(
            key_set,
            encoded_map,
            Bound::Included(NumericIndexKey::new(-5.0, 1000)),
            Bound::Included(NumericIndexKey::new(5.0, 1000)),
        );
        check_range(
            key_set,
            encoded_map,
            Bound::Excluded(NumericIndexKey::new(-5.0, 1000)),
            Bound::Excluded(NumericIndexKey::new(5.0, 1000)),
        );
        check_range(
            key_set,
            encoded_map,
            Bound::Excluded(NumericIndexKey::new(-50000.0, 1000)),
            Bound::Excluded(NumericIndexKey::new(50000.0, 1000)),
        );
    }

    #[test]
    fn test_numeric_index_key_set() {
        let pairs = [
            NumericIndexKey::new(0.0, 1),
            NumericIndexKey::new(0.0, 3),
            NumericIndexKey::new(-0.0, 2),
            NumericIndexKey::new(-0.0, 4),
            NumericIndexKey::new(0.4, 2),
            NumericIndexKey::new(-0.4, 3),
            NumericIndexKey::new(5.0, 1),
            NumericIndexKey::new(-5.0, 1),
            NumericIndexKey::new(f64::NAN, 0),
            NumericIndexKey::new(f64::NAN, 2),
            NumericIndexKey::new(f64::NAN, 3),
            NumericIndexKey::new(f64::INFINITY, 0),
            NumericIndexKey::new(f64::NEG_INFINITY, 1),
            NumericIndexKey::new(f64::NEG_INFINITY, 2),
            NumericIndexKey::new(f64::NEG_INFINITY, 3),
        ];

        let encoded: Vec<(Vec<u8>, PointOffsetType)> = pairs
            .iter()
            .map(|k| (FloatPayloadType::encode_key(&k.key, k.idx), k.idx))
            .collect();

        let mut set_byte: BTreeMap<Vec<u8>, PointOffsetType> =
            BTreeMap::from_iter(encoded.iter().cloned());
        let mut set_keys =
            NumericKeySortedVec::<FloatPayloadType>::from_btree_map(set_byte.clone());

        check_ranges(&set_keys, &set_byte);

        // test deletion and ranges after deletion
        let deleted_key = NumericIndexKey::new(0.4, 2);
        set_keys.remove(deleted_key.clone());
        set_byte.remove(&deleted_key.encode());

        check_ranges(&set_keys, &set_byte);

        // test deletion and ranges after deletion
        let deleted_key = NumericIndexKey::new(-5.0, 1);
        set_keys.remove(deleted_key.clone());
        set_byte.remove(&deleted_key.encode());

        check_ranges(&set_keys, &set_byte);
    }

    #[test]
    fn test_numeric_index_key_sorting() {
        let pairs = [
            NumericIndexKey::new(0.0, 1),
            NumericIndexKey::new(0.0, 3),
            NumericIndexKey::new(-0.0, 2),
            NumericIndexKey::new(-0.0, 4),
            NumericIndexKey::new(0.4, 2),
            NumericIndexKey::new(-0.4, 3),
            NumericIndexKey::new(5.0, 1),
            NumericIndexKey::new(-5.0, 1),
            NumericIndexKey::new(f64::NAN, 0),
            NumericIndexKey::new(f64::NAN, 2),
            NumericIndexKey::new(f64::NAN, 3),
            NumericIndexKey::new(f64::INFINITY, 0),
            NumericIndexKey::new(f64::NEG_INFINITY, 1),
            NumericIndexKey::new(f64::NEG_INFINITY, 2),
            NumericIndexKey::new(f64::NEG_INFINITY, 3),
        ];

        let encoded: Vec<Vec<u8>> = pairs
            .iter()
            .map(|k| FloatPayloadType::encode_key(&k.key, k.idx))
            .collect();

        let set_byte: BTreeSet<Vec<u8>> = BTreeSet::from_iter(encoded.iter().cloned());
        let set_keys: BTreeSet<NumericIndexKey<FloatPayloadType>> =
            BTreeSet::from_iter(pairs.iter().cloned());

        for (b, k) in set_byte.iter().zip(set_keys.iter()) {
            let (decoded_id, decoded_float) = FloatPayloadType::decode_key(b.as_slice());
            if decoded_float.is_nan() && k.key.is_nan() {
                assert_eq!(decoded_id, k.idx);
            } else {
                assert_eq!(decoded_id, k.idx);
                assert_eq!(decoded_float, k.key);
            }
        }
    }
}
