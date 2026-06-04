use std::ops::Bound;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use gridstore::Blob;

use super::super::super::Encodable;
use super::super::super::numeric_index_read::NumericIndexRead;
use super::ReadOnlyAppendableNumericIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::histogram::Histogram;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::on_disk_point_to_values::StoredValue;
use crate::index::payload_config::StorageType;

impl<T: Encodable + Numericable + Send + Sync + Default + StoredValue, S: UniversalRead>
    NumericIndexRead<T> for ReadOnlyAppendableNumericIndex<T, S>
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
        // The on-disk format is Gridstore; the read-only-vs-mutable
        // distinction is not yet reflected in [`StorageType`].
        StorageType::Gridstore
    }

    fn ram_usage_bytes(&self) -> usize {
        self.in_memory_index.ram_usage_bytes()
    }

    fn telemetry_index_type(&self) -> &'static str {
        "read_only_appendable_numeric"
    }
}
