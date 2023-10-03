use std::collections::BTreeMap;
use std::ops::{Bound, Range};
use std::sync::Arc;

use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;

use super::mutable_numeric_index::MutableNumericIndex;
use super::numeric_index_key::{Encodable, NumericIndexKey};
use super::{NumericIndex, HISTOGRAM_MAX_BUCKET_SIZE, HISTOGRAM_PRECISION};
use crate::common::operation_error::OperationResult;
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::index::field_index::histogram::{Histogram, Numericable};

pub struct ImmutableNumericIndex<T: Encodable + Numericable> {
    map: NumericKeySortedVec<T>,
    db_wrapper: DatabaseColumnWrapper,
    pub(super) histogram: Histogram<T>,
    pub(super) points_count: usize,
    pub(super) max_values_per_point: usize,
    point_to_values: Vec<Range<u32>>,
    point_to_values_container: Vec<T>,
}

#[derive(Default)]
struct NumericKeySortedVec<T: Encodable + Numericable> {
    data: Vec<NumericIndexKey<T>>,
    deleted_count: usize,
}

struct NumericKeySortedVecIterator<'a, T: Encodable + Numericable> {
    set: &'a NumericKeySortedVec<T>,
    start_index: usize,
    end_index: usize,
}

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

    fn remove(&mut self, key: NumericIndexKey<T>) {
        if let Ok(index) = self.data.binary_search(&key) {
            self.data[index].idx = PointOffsetType::MAX;
            self.deleted_count += 1;
        }
    }

    fn values_range(
        &self,
        start_bound: Bound<NumericIndexKey<T>>,
        end_bound: Bound<NumericIndexKey<T>>,
    ) -> impl Iterator<Item = NumericIndexKey<T>> + '_ {
        let start_index = self.find_bound_index(start_bound, 0);
        let end_index = self.find_bound_index(end_bound, self.data.len());
        NumericKeySortedVecIterator {
            set: self,
            start_index,
            end_index,
        }
    }

    fn find_bound_index(&self, bound: Bound<NumericIndexKey<T>>, unbounded_value: usize) -> usize {
        match bound {
            Bound::Included(bound) => self
                .data
                .binary_search_by(|key| match key.cmp(&bound) {
                    std::cmp::Ordering::Equal => std::cmp::Ordering::Greater,
                    ord => ord,
                })
                .unwrap_or_else(|idx| idx),
            Bound::Excluded(bound) => self.data.binary_search(&bound).unwrap_or_else(|idx| idx),
            Bound::Unbounded => unbounded_value,
        }
    }
}

impl<'a, T: Encodable + Numericable> Iterator for NumericKeySortedVecIterator<'a, T> {
    type Item = NumericIndexKey<T>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.start_index < self.end_index {
            let key = self.set.data[self.start_index].clone();
            self.start_index += 1;
            if key.idx == PointOffsetType::MAX {
                continue;
            }
            return Some(key);
        }
        None
    }
}

impl<T: Encodable + Numericable> ImmutableNumericIndex<T> {
    pub fn new(db: Arc<RwLock<DB>>, field: &str) -> Self {
        let store_cf_name = NumericIndex::<T>::storage_cf_name(field);
        let db_wrapper = DatabaseColumnWrapper::new(db, &store_cf_name);
        Self {
            map: Default::default(),
            db_wrapper,
            histogram: Histogram::new(HISTOGRAM_MAX_BUCKET_SIZE, HISTOGRAM_PRECISION),
            points_count: 0,
            max_values_per_point: 1,
            point_to_values: Default::default(),
            point_to_values_container: Default::default(),
        }
    }

    pub fn get_db_wrapper(&self) -> &DatabaseColumnWrapper {
        &self.db_wrapper
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<&[T]> {
        let range = self.point_to_values.get(idx as usize)?.clone();
        let range = range.start as usize..range.end as usize;
        Some(&self.point_to_values_container[range])
    }

    pub fn get_values_count(&self) -> usize {
        self.map.len()
    }

    pub fn values_range(
        &self,
        start_bound: Bound<NumericIndexKey<T>>,
        end_bound: Bound<NumericIndexKey<T>>,
    ) -> impl Iterator<Item = PointOffsetType> + '_ {
        self.map
            .values_range(start_bound, end_bound)
            .map(|NumericIndexKey { idx, .. }| idx)
    }

    pub fn load(&mut self) -> OperationResult<bool> {
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

        // flatten points-to-values map
        for values in point_to_values {
            let values = values.into_iter().collect::<Vec<_>>();
            let container_len = self.point_to_values_container.len() as u32;
            let range = container_len..container_len + values.len() as u32;
            self.point_to_values.push(range.clone());
            self.point_to_values_container.extend(values);
        }

        Ok(true)
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        if self.point_to_values.len() <= idx as usize {
            return Ok(());
        }

        let removed_values_range = self.point_to_values[idx as usize].clone();
        self.point_to_values[idx as usize] = Default::default();

        if !removed_values_range.is_empty() {
            self.points_count -= 1;
        }

        for value_index in removed_values_range {
            let value = self.point_to_values_container[value_index as usize];
            self.map.remove(NumericIndexKey::new(value, idx));

            // update db
            let encoded = value.encode_key(idx);
            self.db_wrapper.remove(encoded)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
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

        let start_bound = match start_bound {
            Included(k) => Included(k.encode()),
            Excluded(k) => Excluded(k.encode()),
            Unbounded => Unbounded,
        };
        let end_bound = match end_bound {
            Included(k) => Included(k.encode()),
            Excluded(k) => Excluded(k.encode()),
            Unbounded => Unbounded,
        };
        let set2 = encoded_map
            .range((start_bound, end_bound))
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
        check_range(&key_set, &encoded_map, Bound::Unbounded, Bound::Unbounded);
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
    }
}
