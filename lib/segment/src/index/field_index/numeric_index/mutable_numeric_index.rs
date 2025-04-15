use std::collections::BTreeSet;
use std::ops::Bound;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;

use super::{
    Encodable, HISTOGRAM_MAX_BUCKET_SIZE, HISTOGRAM_PRECISION, numeric_index_storage_cf_name,
};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::index::field_index::histogram::{Histogram, Numericable, Point};

pub struct MutableNumericIndex<T: Encodable + Numericable> {
    db_wrapper: DatabaseColumnScheduledDeleteWrapper,
    in_memory_index: InMemoryNumericIndex<T>,
}

// Numeric Index with insertions and deletions without persistence
pub struct InMemoryNumericIndex<T: Encodable + Numericable> {
    pub map: BTreeSet<Point<T>>,
    pub histogram: Histogram<T>,
    pub points_count: usize,
    pub max_values_per_point: usize,
    pub point_to_values: Vec<Vec<T>>,
}

impl<T: Encodable + Numericable> Default for InMemoryNumericIndex<T> {
    fn default() -> Self {
        Self {
            map: BTreeSet::new(),
            histogram: Histogram::new(HISTOGRAM_MAX_BUCKET_SIZE, HISTOGRAM_PRECISION),
            points_count: 0,
            max_values_per_point: 0,
            point_to_values: Default::default(),
        }
    }
}

impl<T: Encodable + Numericable + Default> FromIterator<(PointOffsetType, T)>
    for InMemoryNumericIndex<T>
{
    fn from_iter<I: IntoIterator<Item = (PointOffsetType, T)>>(iter: I) -> Self {
        let mut index = InMemoryNumericIndex::default();
        for pair in iter {
            let (idx, value) = pair;

            if index.point_to_values.len() <= idx as usize {
                index
                    .point_to_values
                    .resize_with(idx as usize + 1, Vec::new)
            }

            index.point_to_values[idx as usize].push(value);

            let key = Point::new(value, idx);
            InMemoryNumericIndex::add_to_map(&mut index.map, &mut index.histogram, key);
        }
        for values in &index.point_to_values {
            if !values.is_empty() {
                index.points_count += 1;
                index.max_values_per_point = index.max_values_per_point.max(values.len());
            }
        }
        index
    }
}

impl<T: Encodable + Numericable + Default> InMemoryNumericIndex<T> {
    pub fn check_values_any(&self, idx: PointOffsetType, check_fn: impl Fn(&T) -> bool) -> bool {
        self.point_to_values
            .get(idx as usize)
            .map(|values| values.iter().any(check_fn))
            .unwrap_or(false)
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = T> + '_>> {
        Some(Box::new(
            self.point_to_values
                .get(idx as usize)
                .map(|v| v.iter().cloned())?,
        ))
    }

    pub fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        self.point_to_values.get(idx as usize).map(Vec::len)
    }

    pub fn total_unique_values_count(&self) -> usize {
        self.map.len()
    }

    pub fn values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> impl Iterator<Item = PointOffsetType> {
        self.map
            .range((start_bound, end_bound))
            .map(|point| point.idx)
    }

    pub fn orderable_values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> impl DoubleEndedIterator<Item = (T, PointOffsetType)> + '_ {
        self.map
            .range((start_bound, end_bound))
            .map(|point| (point.val, point.idx))
    }

    pub fn add_many_to_list(&mut self, idx: PointOffsetType, values: Vec<T>) {
        if self.point_to_values.len() <= idx as usize {
            self.point_to_values.resize_with(idx as usize + 1, Vec::new)
        }
        for value in &values {
            let key = Point::new(*value, idx);
            Self::add_to_map(&mut self.map, &mut self.histogram, key);
        }
        if !values.is_empty() {
            self.points_count += 1;
            self.max_values_per_point = self.max_values_per_point.max(values.len());
        }
        self.point_to_values[idx as usize] = values;
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) {
        if let Some(values) = self.point_to_values.get_mut(idx as usize) {
            if !values.is_empty() {
                self.points_count = self.points_count.checked_sub(1).unwrap_or_default();
            }
            for value in values.iter() {
                let key = Point::new(*value, idx);
                Self::remove_from_map(&mut self.map, &mut self.histogram, key);
            }
            *values = Default::default();
        }
    }

    fn add_to_map(map: &mut BTreeSet<Point<T>>, histogram: &mut Histogram<T>, key: Point<T>) {
        let was_added = map.insert(key.clone());
        // Histogram works with unique values (idx + value) only, so we need to
        // make sure that we don't add the same value twice.
        // key is a combination of value + idx, so we can use it to ensure than the pair is unique
        if was_added {
            histogram.insert(
                key,
                |x| Self::get_histogram_left_neighbor(map, x.clone()),
                |x| Self::get_histogram_right_neighbor(map, x.clone()),
            );
        }
    }

    fn remove_from_map(map: &mut BTreeSet<Point<T>>, histogram: &mut Histogram<T>, key: Point<T>) {
        let was_removed = map.remove(&key);
        if was_removed {
            histogram.remove(
                &key,
                |x| Self::get_histogram_left_neighbor(map, x.clone()),
                |x| Self::get_histogram_right_neighbor(map, x.clone()),
            );
        }
    }

    fn get_histogram_left_neighbor(map: &BTreeSet<Point<T>>, key: Point<T>) -> Option<Point<T>> {
        map.range((Unbounded, Excluded(key))).next_back().cloned()
    }

    fn get_histogram_right_neighbor(map: &BTreeSet<Point<T>>, key: Point<T>) -> Option<Point<T>> {
        map.range((Excluded(key), Unbounded)).next().cloned()
    }

    pub fn get_histogram(&self) -> &Histogram<T> {
        &self.histogram
    }

    pub fn get_points_count(&self) -> usize {
        self.points_count
    }

    pub fn get_max_values_per_point(&self) -> usize {
        self.max_values_per_point
    }
}

impl<T: Encodable + Numericable + Default> MutableNumericIndex<T> {
    pub fn new_from_db_wrapper(db_wrapper: DatabaseColumnScheduledDeleteWrapper) -> Self {
        Self {
            db_wrapper,
            in_memory_index: InMemoryNumericIndex::default(),
        }
    }

    pub fn into_in_memory_index(self) -> InMemoryNumericIndex<T> {
        self.in_memory_index
    }

    pub fn get_db_wrapper(&self) -> &DatabaseColumnScheduledDeleteWrapper {
        &self.db_wrapper
    }

    pub fn new(db: Arc<RwLock<DB>>, field: &str) -> Self {
        let store_cf_name = numeric_index_storage_cf_name(field);
        let db_wrapper = DatabaseColumnScheduledDeleteWrapper::new(DatabaseColumnWrapper::new(
            db,
            &store_cf_name,
        ));
        Self {
            db_wrapper,
            in_memory_index: InMemoryNumericIndex::default(),
        }
    }

    pub fn load(&mut self) -> OperationResult<bool> {
        if !self.db_wrapper.has_column_family()? {
            return Ok(false);
        };

        self.in_memory_index = self
            .db_wrapper
            .lock_db()
            .iter()?
            .map(|(key, value)| {
                let value_idx =
                    u32::from_be_bytes(value.as_ref().try_into().map_err(|_| {
                        OperationError::service_error("incorrect numeric index value")
                    })?);
                let (idx, value) = T::decode_key(&key);
                if idx != value_idx {
                    return Err(OperationError::service_error(
                        "incorrect numeric index key-value pair",
                    ));
                }
                Ok((idx, value))
            })
            .collect::<Result<InMemoryNumericIndex<_>, OperationError>>()?;

        Ok(true)
    }

    pub fn add_many_to_list(
        &mut self,
        idx: PointOffsetType,
        values: Vec<T>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let mut hw_cell_wb = hw_counter
            .payload_index_io_write_counter()
            .write_back_counter();

        for value in &values {
            let key = value.encode_key(idx);
            self.db_wrapper.put(&key, idx.to_be_bytes())?;
            hw_cell_wb.incr_delta(size_of_val(&key) + size_of_val(&idx));
        }

        self.in_memory_index.add_many_to_list(idx, values);
        Ok(())
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        self.in_memory_index
            .get_values(idx)
            .map(|mut values| {
                values.try_for_each(|value| {
                    let key = value.encode_key(idx);
                    self.db_wrapper.remove(key)
                })
            })
            .transpose()?;
        self.in_memory_index.remove_point(idx);
        Ok(())
    }

    pub fn map(&self) -> &BTreeSet<Point<T>> {
        &self.in_memory_index.map
    }

    #[inline]
    pub fn total_unique_values_count(&self) -> usize {
        self.in_memory_index.total_unique_values_count()
    }
    #[inline]
    pub fn check_values_any(&self, idx: PointOffsetType, check_fn: impl Fn(&T) -> bool) -> bool {
        self.in_memory_index.check_values_any(idx, check_fn)
    }
    #[inline]
    pub fn get_points_count(&self) -> usize {
        self.in_memory_index.get_points_count()
    }
    #[inline]
    pub fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = T> + '_>> {
        self.in_memory_index.get_values(idx)
    }
    #[inline]
    pub fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        self.in_memory_index.values_count(idx)
    }
    #[inline]
    pub fn values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> impl Iterator<Item = PointOffsetType> {
        self.in_memory_index.values_range(start_bound, end_bound)
    }
    #[inline]
    pub fn orderable_values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> impl DoubleEndedIterator<Item = (T, PointOffsetType)> + '_ {
        self.in_memory_index
            .orderable_values_range(start_bound, end_bound)
    }
    #[inline]
    pub fn get_histogram(&self) -> &Histogram<T> {
        self.in_memory_index.get_histogram()
    }
    #[inline]
    pub fn get_max_values_per_point(&self) -> usize {
        self.in_memory_index.get_max_values_per_point()
    }
}
