use std::collections::BTreeMap;
use std::ops::Bound;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;

use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;

use super::{Encodable, NumericIndex, HISTOGRAM_MAX_BUCKET_SIZE, HISTOGRAM_PRECISION};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::index::field_index::histogram::{Histogram, Numericable, Point};

pub struct MutableNumericIndex<T: Encodable + Numericable> {
    pub(super) map: BTreeMap<Vec<u8>, PointOffsetType>,
    pub(super) db_wrapper: DatabaseColumnWrapper,
    pub(super) histogram: Histogram<T>,
    pub(super) points_count: usize,
    pub(super) max_values_per_point: usize,
    pub(super) point_to_values: Vec<Vec<T>>,
}

impl<T: Encodable + Numericable> MutableNumericIndex<T> {
    pub fn new(db: Arc<RwLock<DB>>, field: &str) -> Self {
        let store_cf_name = NumericIndex::<T>::storage_cf_name(field);
        let db_wrapper = DatabaseColumnWrapper::new(db, &store_cf_name);
        Self {
            map: BTreeMap::new(),
            db_wrapper,
            histogram: Histogram::new(HISTOGRAM_MAX_BUCKET_SIZE, HISTOGRAM_PRECISION),
            points_count: 0,
            max_values_per_point: 0,
            point_to_values: Default::default(),
        }
    }

    pub fn get_db_wrapper(&self) -> &DatabaseColumnWrapper {
        &self.db_wrapper
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<&[T]> {
        self.point_to_values.get(idx as usize).map(|v| v.as_slice())
    }

    pub fn get_values_count(&self) -> usize {
        self.map.len()
    }

    pub fn values_range(
        &self,
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
    ) -> impl Iterator<Item = PointOffsetType> + '_ {
        self.map.range((start_bound, end_bound)).map(|(_, v)| *v)
    }

    pub fn values_range_rev(
        &self,
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
    ) -> impl Iterator<Item = PointOffsetType> + '_ {
        self.map
            .range((start_bound, end_bound))
            .rev()
            .map(|(_, v)| *v)
    }

    fn add_value(&mut self, id: PointOffsetType, value: T) -> OperationResult<()> {
        let key = value.encode_key(id);
        self.db_wrapper.put(&key, id.to_be_bytes())?;
        Self::add_to_map(&mut self.map, &mut self.histogram, key, id);
        Ok(())
    }

    pub fn add_many_to_list(
        &mut self,
        idx: PointOffsetType,
        values: impl IntoIterator<Item = T>,
    ) -> OperationResult<()> {
        if self.point_to_values.len() <= idx as usize {
            self.point_to_values.resize_with(idx as usize + 1, Vec::new)
        }
        let values: Vec<T> = values.into_iter().collect();
        for value in &values {
            self.add_value(idx, *value)?;
        }
        if !values.is_empty() {
            self.points_count += 1;
            self.max_values_per_point = self.max_values_per_point.max(values.len());
        }
        self.point_to_values[idx as usize] = values;
        Ok(())
    }

    pub fn load(&mut self) -> OperationResult<bool> {
        if !self.db_wrapper.has_column_family()? {
            return Ok(false);
        };

        for (key, value) in self.db_wrapper.lock_db().iter()? {
            let value_idx = u32::from_be_bytes(value.as_ref().try_into().unwrap());
            let (idx, value) = T::decode_key(&key);

            if idx != value_idx {
                return Err(OperationError::service_error("incorrect key value"));
            }

            if self.point_to_values.len() <= idx as usize {
                self.point_to_values.resize_with(idx as usize + 1, Vec::new)
            }

            self.point_to_values[idx as usize].push(value);

            Self::add_to_map(&mut self.map, &mut self.histogram, key.to_vec(), idx);
        }
        for values in &self.point_to_values {
            if !values.is_empty() {
                self.points_count += 1;
                self.max_values_per_point = self.max_values_per_point.max(values.len());
            }
        }
        Ok(true)
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        if self.point_to_values.len() <= idx as usize {
            return Ok(());
        }

        let removed_values = std::mem::take(&mut self.point_to_values[idx as usize]);

        for value in &removed_values {
            let key = value.encode_key(idx);
            self.db_wrapper.remove(&key)?;
            Self::remove_from_map(&mut self.map, &mut self.histogram, key);
        }

        if !removed_values.is_empty() {
            self.points_count -= 1;
        }

        Ok(())
    }

    fn add_to_map(
        map: &mut BTreeMap<Vec<u8>, PointOffsetType>,
        histogram: &mut Histogram<T>,
        key: Vec<u8>,
        id: PointOffsetType,
    ) {
        let existed_value = map.insert(key.clone(), id);
        // Histogram works with unique values (idx + value) only, so we need to
        // make sure that we don't add the same value twice.
        // key is a combination of value + idx, so we can use it to ensure than the pair is unique
        if existed_value.is_none() {
            histogram.insert(
                Self::key_to_histogram_point(&key),
                |x| Self::get_histogram_left_neighbor(map, x),
                |x| Self::get_histogram_right_neighbor(map, x),
            );
        }
    }

    fn remove_from_map(
        map: &mut BTreeMap<Vec<u8>, PointOffsetType>,
        histogram: &mut Histogram<T>,
        key: Vec<u8>,
    ) {
        let existed_val = map.remove(&key);
        if existed_val.is_some() {
            histogram.remove(
                &Self::key_to_histogram_point(&key),
                |x| Self::get_histogram_left_neighbor(map, x),
                |x| Self::get_histogram_right_neighbor(map, x),
            );
        }
    }

    fn key_to_histogram_point(key: &[u8]) -> Point<T> {
        let (decoded_idx, decoded_val) = T::decode_key(key);
        Point {
            val: decoded_val,
            idx: decoded_idx as usize,
        }
    }

    fn get_histogram_left_neighbor(
        map: &BTreeMap<Vec<u8>, PointOffsetType>,
        point: &Point<T>,
    ) -> Option<Point<T>> {
        let key = point.val.encode_key(point.idx as PointOffsetType);
        map.range((Unbounded, Excluded(key)))
            .next_back()
            .map(|(key, _)| Self::key_to_histogram_point(key))
    }

    fn get_histogram_right_neighbor(
        map: &BTreeMap<Vec<u8>, PointOffsetType>,
        point: &Point<T>,
    ) -> Option<Point<T>> {
        let key = point.val.encode_key(point.idx as PointOffsetType);
        map.range((Excluded(key), Unbounded))
            .next()
            .map(|(key, _)| Self::key_to_histogram_point(key))
    }
}
