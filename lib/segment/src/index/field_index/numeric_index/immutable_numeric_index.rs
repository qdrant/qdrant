use std::collections::BTreeMap;
use std::ops::{Bound, Range};
use std::sync::Arc;

use parking_lot::RwLock;
use rocksdb::DB;

use super::mutable_numeric_index::MutableNumericIndex;
use super::{Encodable, NumericIndex, HISTOGRAM_MAX_BUCKET_SIZE, HISTOGRAM_PRECISION};
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::entry::entry_point::OperationResult;
use crate::index::field_index::histogram::{Histogram, Numericable};
use crate::types::PointOffsetType;

pub struct ImmutableNumericIndex<T: Encodable + Numericable> {
    pub(super) map: BTreeMap<Vec<u8>, u32>,
    pub(super) db_wrapper: DatabaseColumnWrapper,
    pub(super) histogram: Histogram<T>,
    pub(super) points_count: usize,
    pub(super) max_values_per_point: usize,
    pub(super) point_to_values: Vec<Range<u32>>,
    pub(super) point_to_values_container: Vec<T>,
}

impl<T: Encodable + Numericable> ImmutableNumericIndex<T> {
    pub fn new(db: Arc<RwLock<DB>>, field: &str) -> Self {
        let store_cf_name = NumericIndex::<T>::storage_cf_name(field);
        let db_wrapper = DatabaseColumnWrapper::new(db, &store_cf_name);
        Self {
            map: BTreeMap::new(),
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
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
    ) -> impl Iterator<Item = PointOffsetType> + '_ {
        self.map.range((start_bound, end_bound)).map(|(_, v)| *v)
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

        self.map = map;
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
            // Actually remove value from container and get it
            let value = self.point_to_values_container[value_index as usize];
            let encoded = value.encode_key(idx);
            self.map.remove(&encoded);

            // update db
            self.db_wrapper.remove(encoded)?;
        }

        Ok(())
    }
}
