//! Read-path surface for the numeric-index module: the [`StreamRange`]
//! trait, the [`Range`] → index-key-bounds conversion, and the
//! [`PayloadFieldIndexRead`] implementation for [`NumericIndex`].

use std::ops::Bound;
use std::ops::Bound::{Excluded, Included, Unbounded};

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::Blob;
use serde_json::Value;

use super::{Encodable, NumericIndex};
use crate::common::operation_error::OperationResult;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::on_disk_point_to_values::StoredValue;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndexRead,
};
use crate::index::query_optimization::optimized_filter::DynConditionChecker;
use crate::types::{FieldCondition, PayloadKeyType, Range, RangeInterface};

pub trait StreamRange<T> {
    fn stream_range(
        &self,
        range: &RangeInterface,
    ) -> OperationResult<impl DoubleEndedIterator<Item = (T, PointOffsetType)> + '_>;
}

impl<T: Encodable + Numericable> Range<T> {
    pub(in crate::index::field_index::numeric_index) fn as_index_key_bounds(
        &self,
    ) -> (Bound<Point<T>>, Bound<Point<T>>) {
        let start_bound = match self {
            Range { gt: Some(gt), .. } => Excluded(Point::new(*gt, PointOffsetType::MAX)),
            Range { gte: Some(gte), .. } => Included(Point::new(*gte, PointOffsetType::MIN)),
            _ => Unbounded,
        };

        let end_bound = match self {
            Range { lt: Some(lt), .. } => Excluded(Point::new(*lt, PointOffsetType::MIN)),
            Range { lte: Some(lte), .. } => Included(Point::new(*lte, PointOffsetType::MAX)),
            _ => Unbounded,
        };

        (start_bound, end_bound)
    }
}

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default, P> PayloadFieldIndexRead
    for NumericIndex<T, P>
where
    Vec<T>: Blob,
{
    fn count_indexed_points(&self) -> usize {
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
    ) -> OperationResult<Option<DynConditionChecker<'a>>> {
        self.inner.condition_checker(condition, hw_acc)
    }

    fn special_check_condition(
        &self,
        condition: &FieldCondition,
        payload_value: &Value,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<bool>> {
        self.inner
            .special_check_condition(condition, payload_value, hw_counter)
    }
}
