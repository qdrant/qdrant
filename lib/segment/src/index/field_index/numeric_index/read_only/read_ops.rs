//! [`NumericIndexRead`] and [`PayloadFieldIndexRead`] impls for
//! [`ReadOnlyNumericIndex`], forwarding to the inner storage-variant enum.

use std::ops::Bound;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use gridstore::Blob;

use super::super::numeric_index_read::NumericIndexRead;
use super::super::{Encodable, NumericIndexValue};
use super::ReadOnlyNumericIndex;
use crate::common::operation_error::OperationResult;
use crate::index::UniversalReadExt;
use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::field_index::histogram::Histogram;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::on_disk_point_to_values::StoredValue;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndexRead,
};
use crate::index::payload_config::StorageType;
use crate::types::{FieldCondition, PayloadKeyType};

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default, P, S: UniversalRead>
    NumericIndexRead<T> for ReadOnlyNumericIndex<T, P, S>
where
    Vec<T>: Blob,
{
    fn check_values_any(
        &self,
        idx: PointOffsetType,
        check_fn: impl Fn(&T) -> bool,
        hw_counter: &HardwareCounterCell,
    ) -> bool {
        self.inner.check_values_any(idx, check_fn, hw_counter)
    }

    fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = T> + '_>> {
        self.inner.get_values(idx)
    }

    fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        self.inner.values_count(idx)
    }

    fn total_unique_values_count(&self) -> OperationResult<usize> {
        self.inner.total_unique_values_count()
    }

    fn values_range<'a>(
        &'a self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<impl Iterator<Item = PointOffsetType> + 'a> {
        self.inner.values_range(start_bound, end_bound, hw_counter)
    }

    fn orderable_values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> OperationResult<impl DoubleEndedIterator<Item = (T, PointOffsetType)> + '_> {
        self.inner.orderable_values_range(start_bound, end_bound)
    }

    fn get_histogram(&self) -> &Histogram<T> {
        self.inner.get_histogram()
    }

    fn get_points_count(&self) -> usize {
        self.inner.get_points_count()
    }

    fn get_max_values_per_point(&self) -> usize {
        self.inner.get_max_values_per_point()
    }

    fn storage_type(&self) -> StorageType {
        self.inner.storage_type()
    }

    fn ram_usage_bytes(&self) -> usize {
        self.inner.ram_usage_bytes()
    }

    fn telemetry_index_type(&self) -> &'static str {
        self.inner.telemetry_index_type()
    }
}

impl<T: NumericIndexValue, P, S: UniversalReadExt> PayloadFieldIndexRead
    for ReadOnlyNumericIndex<T, P, S>
where
    Vec<T>: Blob,
{
    fn count_indexed_points(&self) -> OperationResult<usize> {
        self.inner.count_indexed_points()
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        self.inner.filter(condition, hw_counter)
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        self.inner.estimate_cardinality(condition, hw_counter)
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.inner.for_each_payload_block(threshold, key, f)
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> OperationResult<Option<ConditionCheckerEnum<'a>>> {
        self.inner.condition_checker(condition, hw_acc)
    }
}
