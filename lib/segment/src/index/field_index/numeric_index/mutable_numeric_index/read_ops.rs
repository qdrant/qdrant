use std::ops::Bound;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::Blob;

use super::super::Encodable;
use super::super::numeric_index_read::NumericIndexRead;
use super::{InMemoryNumericIndex, MutableNumericIndex};
use crate::common::operation_error::OperationResult;
use crate::index::field_index::histogram::Histogram;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::on_disk_point_to_values::StoredValue;
use crate::index::payload_config::StorageType;

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

impl<T: Encodable + Numericable> InMemoryNumericIndex<T> {
    /// Approximate RAM usage in bytes.
    pub fn ram_usage_bytes(&self) -> usize {
        let Self {
            map,
            histogram,
            points_count: _,         // scalar
            max_values_per_point: _, // scalar
            point_to_values,
        } = self;

        // BTreeSet: ~3 pointers overhead per entry
        let btree_entry_overhead = std::mem::size_of::<usize>() * 3;
        let map_bytes = map.len() * (std::mem::size_of::<Point<T>>() + btree_entry_overhead);
        let histogram_bytes = histogram.ram_usage_bytes();
        let ptv_bytes: usize = point_to_values.capacity() * std::mem::size_of::<Vec<T>>()
            + point_to_values
                .iter()
                .map(|v| v.capacity() * std::mem::size_of::<T>())
                .sum::<usize>();
        map_bytes + histogram_bytes + ptv_bytes
    }
}

impl<T: Encodable + Numericable + Send + Sync + Default + StoredValue> NumericIndexRead<T>
    for MutableNumericIndex<T>
where
    Vec<T>: Blob,
{
    fn check_values_any(
        &self,
        idx: PointOffsetType,
        check_fn: impl Fn(&T) -> bool,
        _hw_counter: &HardwareCounterCell,
    ) -> bool {
        self.in_memory_index.check_values_any(idx, check_fn)
    }

    fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = T> + '_>> {
        self.in_memory_index.get_values(idx)
    }

    fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        self.in_memory_index.values_count(idx)
    }

    fn total_unique_values_count(&self) -> OperationResult<usize> {
        Ok(self.in_memory_index.total_unique_values_count())
    }

    fn values_range<'a>(
        &'a self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
        _hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<impl Iterator<Item = PointOffsetType> + 'a> {
        Ok(self.in_memory_index.values_range(start_bound, end_bound))
    }

    fn orderable_values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> OperationResult<impl DoubleEndedIterator<Item = (T, PointOffsetType)> + '_> {
        Ok(self
            .in_memory_index
            .orderable_values_range(start_bound, end_bound))
    }

    fn get_histogram(&self) -> &Histogram<T> {
        self.in_memory_index.get_histogram()
    }

    fn get_points_count(&self) -> usize {
        self.in_memory_index.get_points_count()
    }

    fn get_max_values_per_point(&self) -> usize {
        self.in_memory_index.get_max_values_per_point()
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Gridstore
    }

    /// Approximate RAM usage in bytes for in-memory index structures.
    fn ram_usage_bytes(&self) -> usize {
        let Self {
            storage: _, // disk-backed, accounted via files
            in_memory_index,
        } = self;
        in_memory_index.ram_usage_bytes()
    }

    fn telemetry_index_type(&self) -> &'static str {
        "mutable_numeric"
    }
}
