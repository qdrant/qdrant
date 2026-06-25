//! [`NumericIndexRead`] and [`StreamRange`] dispatch for
//! [`ReadOnlyNumericIndexInner`].
//!
//! Forwards every read-path method to the active read-only storage
//! variant; the `values_is_empty` / `get_telemetry_data` helpers come
//! from the trait's default impls.

use std::ops::Bound;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use gridstore::Blob;

use super::super::super::numeric_index_read::NumericIndexRead;
use super::super::super::{Encodable, StreamRange, query};
use super::ReadOnlyNumericIndexInner;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::histogram::Histogram;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::on_disk_point_to_values::StoredValue;
use crate::index::payload_config::StorageType;
use crate::types::RangeInterface;

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default, S: UniversalRead>
    NumericIndexRead<T> for ReadOnlyNumericIndexInner<T, S>
where
    Vec<T>: Blob,
{
    fn check_values_any(
        &self,
        idx: PointOffsetType,
        check_fn: impl Fn(&T) -> bool,
        hw_counter: &HardwareCounterCell,
    ) -> bool {
        match self {
            ReadOnlyNumericIndexInner::Appendable(index) => {
                index.check_values_any(idx, check_fn, hw_counter)
            }
            ReadOnlyNumericIndexInner::OnDisk(index) => {
                index.check_values_any(idx, check_fn, hw_counter)
            }
        }
    }

    fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = T> + '_>> {
        match self {
            ReadOnlyNumericIndexInner::Appendable(index) => index.get_values(idx),
            ReadOnlyNumericIndexInner::OnDisk(index) => index.get_values(idx),
        }
    }

    fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        match self {
            ReadOnlyNumericIndexInner::Appendable(index) => index.values_count(idx),
            ReadOnlyNumericIndexInner::OnDisk(index) => index.values_count(idx),
        }
    }

    fn total_unique_values_count(&self) -> OperationResult<usize> {
        match self {
            ReadOnlyNumericIndexInner::Appendable(index) => index.total_unique_values_count(),
            ReadOnlyNumericIndexInner::OnDisk(index) => index.total_unique_values_count(),
        }
    }

    fn values_range<'a>(
        &'a self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<impl Iterator<Item = PointOffsetType> + 'a> {
        let boxed: Box<dyn Iterator<Item = PointOffsetType> + 'a> = match self {
            ReadOnlyNumericIndexInner::Appendable(index) => {
                Box::new(index.values_range(start_bound, end_bound, hw_counter)?)
            }
            ReadOnlyNumericIndexInner::OnDisk(index) => {
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
            ReadOnlyNumericIndexInner::Appendable(index) => {
                Box::new(index.orderable_values_range(start_bound, end_bound)?)
            }
            ReadOnlyNumericIndexInner::OnDisk(index) => {
                Box::new(index.orderable_values_range(start_bound, end_bound)?)
            }
        };
        Ok(boxed)
    }

    fn get_histogram(&self) -> &Histogram<T> {
        match self {
            ReadOnlyNumericIndexInner::Appendable(index) => index.get_histogram(),
            ReadOnlyNumericIndexInner::OnDisk(index) => index.get_histogram(),
        }
    }

    fn get_points_count(&self) -> usize {
        match self {
            ReadOnlyNumericIndexInner::Appendable(index) => index.get_points_count(),
            ReadOnlyNumericIndexInner::OnDisk(index) => index.get_points_count(),
        }
    }

    fn get_max_values_per_point(&self) -> usize {
        match self {
            ReadOnlyNumericIndexInner::Appendable(index) => index.get_max_values_per_point(),
            ReadOnlyNumericIndexInner::OnDisk(index) => index.get_max_values_per_point(),
        }
    }

    fn storage_type(&self) -> StorageType {
        match self {
            ReadOnlyNumericIndexInner::Appendable(index) => index.storage_type(),
            ReadOnlyNumericIndexInner::OnDisk(index) => index.storage_type(),
        }
    }

    fn ram_usage_bytes(&self) -> usize {
        match self {
            ReadOnlyNumericIndexInner::Appendable(index) => index.ram_usage_bytes(),
            ReadOnlyNumericIndexInner::OnDisk(index) => index.ram_usage_bytes(),
        }
    }

    fn telemetry_index_type(&self) -> &'static str {
        match self {
            ReadOnlyNumericIndexInner::Appendable(index) => index.telemetry_index_type(),
            ReadOnlyNumericIndexInner::OnDisk(index) => index.telemetry_index_type(),
        }
    }
}

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default, S: UniversalRead>
    StreamRange<T> for ReadOnlyNumericIndexInner<T, S>
where
    Vec<T>: Blob,
{
    fn stream_range(
        &self,
        range: &RangeInterface,
    ) -> OperationResult<impl DoubleEndedIterator<Item = (T, PointOffsetType)> + '_> {
        query::stream_range(self, range)
    }
}
