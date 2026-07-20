use std::ops::Bound;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::Blob;

use super::super::Encodable;
use super::super::numeric_index_read::NumericIndexRead;
use super::ImmutableNumericIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::histogram::Histogram;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::stored_point_to_values::StoredValue;
use crate::index::payload_config::StorageType;

impl<T: Encodable + Numericable + StoredValue + Default> ImmutableNumericIndex<T> {
    pub(super) fn compute_ram_usage_bytes(&self) -> usize {
        let Self {
            map,
            histogram,
            points_count: _,
            max_values_per_point: _,
            point_to_values,
            storage: _,
            cached_ram_usage_bytes: _,
        } = self;

        map.ram_usage_bytes() + histogram.ram_usage_bytes() + point_to_values.ram_usage_bytes()
    }
}

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default> NumericIndexRead<T>
    for ImmutableNumericIndex<T>
where
    Vec<T>: Blob,
{
    fn check_values_any(
        &self,
        idx: PointOffsetType,
        check_fn: impl Fn(&T) -> bool,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        Ok(self.point_to_values.check_values_any(idx, |v| check_fn(v)))
    }

    fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = T> + '_>> {
        let iter = self.point_to_values.get_values(idx)?;
        Some(Box::new(iter.copied()))
    }

    fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        self.point_to_values.get_values_count(idx)
    }

    fn total_unique_values_count(&self) -> OperationResult<usize> {
        Ok(self.map.len())
    }

    fn values_range<'a>(
        &'a self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
        _hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<impl Iterator<Item = PointOffsetType> + 'a> {
        Ok(self
            .map
            .values_range(start_bound, end_bound)
            .map(|Point { idx, .. }| idx))
    }

    fn orderable_values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> OperationResult<impl DoubleEndedIterator<Item = (T, PointOffsetType)> + '_> {
        Ok(self
            .map
            .values_range(start_bound, end_bound)
            .map(|Point { val, idx, .. }| (val, idx)))
    }

    /// Cheap `O(log n)` boundary search over the precomputed sorted vector.
    /// In-memory, so `hw_counter` is unused.
    fn values_range_size(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        let iterator = self.map.values_range(start_bound, end_bound);
        Ok(iterator.end_index - iterator.start_index)
    }

    fn get_histogram(&self) -> &Histogram<T> {
        &self.histogram
    }

    fn get_points_count(&self) -> usize {
        self.points_count
    }

    fn get_max_values_per_point(&self) -> usize {
        self.max_values_per_point
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Mmap {
            is_on_disk: self.storage.is_on_disk(),
        }
    }

    /// Approximate RAM usage in bytes for in-memory structures (cached at construction).
    fn ram_usage_bytes(&self) -> usize {
        self.cached_ram_usage_bytes
    }

    fn telemetry_index_type(&self) -> &'static str {
        "immutable_numeric"
    }
}
