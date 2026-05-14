//! Read-path forwarding for [`NumericIndexInner`]: value lookups,
//! telemetry, RAM accounting, and `is_on_disk`.

use std::ops::Bound;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::Blob;

use super::super::Encodable;
use super::super::read_ops::NumericIndexRead;
use super::NumericIndexInner;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::stored_point_to_values::StoredValue;
use crate::telemetry::PayloadIndexTelemetry;

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default> NumericIndexInner<T>
where
    Vec<T>: Blob,
{
    pub fn check_values_any(
        &self,
        idx: PointOffsetType,
        check_fn: impl Fn(&T) -> bool,
        hw_counter: &HardwareCounterCell,
    ) -> bool {
        // FIXME: don't silently ignore mmap-read errors; promote the trait
        // method's return type to `OperationResult<bool>` and propagate.
        match self {
            NumericIndexInner::Mutable(index) => index.check_values_any(idx, check_fn, hw_counter),
            NumericIndexInner::Immutable(index) => {
                index.check_values_any(idx, check_fn, hw_counter)
            }
            NumericIndexInner::Mmap(index) => index.check_values_any(idx, check_fn, hw_counter),
        }
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = T> + '_>> {
        match self {
            NumericIndexInner::Mutable(index) => index.get_values(idx),
            NumericIndexInner::Immutable(index) => index.get_values(idx),
            NumericIndexInner::Mmap(index) => index.get_values(idx),
        }
    }

    pub fn point_ids_by_value<'a>(
        &'a self,
        value: T,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        let start = Bound::Included(Point::new(value, PointOffsetType::MIN));
        let end = Bound::Included(Point::new(value, PointOffsetType::MAX));
        Ok(match self {
            NumericIndexInner::Mutable(mutable) => {
                Box::new(mutable.values_range(start, end, hw_counter)?)
            }
            NumericIndexInner::Immutable(immutable) => {
                Box::new(immutable.values_range(start, end, hw_counter)?)
            }
            NumericIndexInner::Mmap(mmap) => Box::new(mmap.values_range(start, end, hw_counter)?),
        })
    }

    pub fn values_count(&self, idx: PointOffsetType) -> usize {
        match self {
            NumericIndexInner::Mutable(index) => index.values_count(idx).unwrap_or_default(),
            NumericIndexInner::Immutable(index) => index.values_count(idx).unwrap_or_default(),
            NumericIndexInner::Mmap(index) => index.values_count(idx).unwrap_or_default(),
        }
    }

    /// Maximum number of values per point
    ///
    /// # Warning
    ///
    /// Zero if the index is empty.
    pub fn max_values_per_point(&self) -> usize {
        match self {
            NumericIndexInner::Mutable(index) => index.get_max_values_per_point(),
            NumericIndexInner::Immutable(index) => index.get_max_values_per_point(),
            NumericIndexInner::Mmap(index) => index.get_max_values_per_point(),
        }
    }

    pub fn values_is_empty(&self, idx: PointOffsetType) -> bool {
        self.values_count(idx) == 0
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_count: self.get_points_count(),
            points_values_count: self.get_histogram().get_total_count(),
            histogram_bucket_size: Some(self.get_histogram().current_bucket_size()),
            index_type: match self {
                NumericIndexInner::Mutable(_) => "mutable_numeric",
                NumericIndexInner::Immutable(_) => "immutable_numeric",
                NumericIndexInner::Mmap(_) => "mmap_numeric",
            },
        }
    }

    /// Approximate RAM usage in bytes for in-memory structures.
    pub fn ram_usage_bytes(&self) -> usize {
        match self {
            NumericIndexInner::Mutable(index) => index.ram_usage_bytes(),
            NumericIndexInner::Immutable(index) => index.ram_usage_bytes(),
            NumericIndexInner::Mmap(index) => index.ram_usage_bytes(),
        }
    }

    pub fn is_on_disk(&self) -> bool {
        match self {
            NumericIndexInner::Mutable(_) => false,
            NumericIndexInner::Immutable(_) => false,
            NumericIndexInner::Mmap(index) => index.is_on_disk(),
        }
    }
}
