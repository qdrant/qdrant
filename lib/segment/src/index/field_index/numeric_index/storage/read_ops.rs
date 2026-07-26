//! [`NumericIndexRead`] dispatch for [`NumericIndexInner`].
//!
//! Forwards every read-path method to the active storage variant. Each
//! variant already implements [`NumericIndexRead`]; this impl just picks
//! the arm. `point_ids_by_value` is an enum-only convenience wrapper that
//! isn't part of the shared trait.

use std::ops::Bound;

use blobstore::Blob;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use super::super::Encodable;
use super::super::numeric_index_read::NumericIndexRead;
use super::NumericIndexInner;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::histogram::Histogram;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::on_disk_point_to_values::StoredValue;
use crate::index::payload_config::StorageType;

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default> NumericIndexRead<T>
    for NumericIndexInner<T>
where
    Vec<T>: Blob,
{
    fn check_values_any(
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
            NumericIndexInner::OnDisk(index) => index.check_values_any(idx, check_fn, hw_counter),
        }
    }

    fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = T> + '_>> {
        match self {
            NumericIndexInner::Mutable(index) => index.get_values(idx),
            NumericIndexInner::Immutable(index) => index.get_values(idx),
            NumericIndexInner::OnDisk(index) => index.get_values(idx),
        }
    }

    fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        match self {
            NumericIndexInner::Mutable(index) => index.values_count(idx),
            NumericIndexInner::Immutable(index) => index.values_count(idx),
            NumericIndexInner::OnDisk(index) => index.values_count(idx),
        }
    }

    fn total_unique_values_count(&self) -> OperationResult<usize> {
        match self {
            NumericIndexInner::Mutable(index) => index.total_unique_values_count(),
            NumericIndexInner::Immutable(index) => index.total_unique_values_count(),
            NumericIndexInner::OnDisk(index) => index.total_unique_values_count(),
        }
    }

    fn values_range<'a>(
        &'a self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<impl Iterator<Item = PointOffsetType> + 'a> {
        let boxed: Box<dyn Iterator<Item = PointOffsetType> + 'a> = match self {
            NumericIndexInner::Mutable(index) => {
                Box::new(index.values_range(start_bound, end_bound, hw_counter)?)
            }
            NumericIndexInner::Immutable(index) => {
                Box::new(index.values_range(start_bound, end_bound, hw_counter)?)
            }
            NumericIndexInner::OnDisk(index) => {
                Box::new(index.values_range(start_bound, end_bound, hw_counter)?)
            }
        };
        Ok(boxed)
    }

    fn orderable_values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> OperationResult<impl DoubleEndedIterator<Item = (T, PointOffsetType)> + '_> {
        let boxed: Box<dyn DoubleEndedIterator<Item = (T, PointOffsetType)> + '_> = match self {
            NumericIndexInner::Mutable(index) => {
                Box::new(index.orderable_values_range(start_bound, end_bound)?)
            }
            NumericIndexInner::Immutable(index) => {
                Box::new(index.orderable_values_range(start_bound, end_bound)?)
            }
            NumericIndexInner::OnDisk(index) => {
                Box::new(index.orderable_values_range(start_bound, end_bound)?)
            }
        };
        Ok(boxed)
    }

    fn get_histogram(&self) -> &Histogram<T> {
        match self {
            NumericIndexInner::Mutable(index) => index.get_histogram(),
            NumericIndexInner::Immutable(index) => index.get_histogram(),
            NumericIndexInner::OnDisk(index) => index.get_histogram(),
        }
    }

    fn get_points_count(&self) -> usize {
        match self {
            NumericIndexInner::Mutable(index) => index.get_points_count(),
            NumericIndexInner::Immutable(index) => index.get_points_count(),
            NumericIndexInner::OnDisk(index) => index.get_points_count(),
        }
    }

    fn get_max_values_per_point(&self) -> usize {
        match self {
            NumericIndexInner::Mutable(index) => index.get_max_values_per_point(),
            NumericIndexInner::Immutable(index) => index.get_max_values_per_point(),
            NumericIndexInner::OnDisk(index) => index.get_max_values_per_point(),
        }
    }

    fn storage_type(&self) -> StorageType {
        match self {
            NumericIndexInner::Mutable(index) => index.storage_type(),
            NumericIndexInner::Immutable(index) => index.storage_type(),
            NumericIndexInner::OnDisk(index) => index.storage_type(),
        }
    }

    /// Approximate RAM usage in bytes for in-memory structures.
    fn ram_usage_bytes(&self) -> usize {
        match self {
            NumericIndexInner::Mutable(index) => index.ram_usage_bytes(),
            NumericIndexInner::Immutable(index) => index.ram_usage_bytes(),
            NumericIndexInner::OnDisk(index) => index.ram_usage_bytes(),
        }
    }

    fn telemetry_index_type(&self) -> &'static str {
        match self {
            NumericIndexInner::Mutable(index) => index.telemetry_index_type(),
            NumericIndexInner::Immutable(index) => index.telemetry_index_type(),
            NumericIndexInner::OnDisk(index) => index.telemetry_index_type(),
        }
    }
}

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default> NumericIndexInner<T>
where
    Vec<T>: Blob,
{
    /// All point IDs that carry exactly `value`.
    ///
    /// Enum-only convenience wrapper around [`NumericIndexRead::values_range`]
    /// with the single-value bounds — not part of the shared trait.
    pub fn point_ids_by_value<'a>(
        &'a self,
        value: T,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<impl Iterator<Item = PointOffsetType> + 'a> {
        let start = Bound::Included(Point::new(value, PointOffsetType::MIN));
        let end = Bound::Included(Point::new(value, PointOffsetType::MAX));
        self.values_range(start, end, hw_counter)
    }

    pub fn is_on_disk(&self) -> bool {
        match self {
            NumericIndexInner::Mutable(_) => false,
            NumericIndexInner::Immutable(_) => false,
            NumericIndexInner::OnDisk(index) => index.is_on_disk(),
        }
    }
}
