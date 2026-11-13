//! Read-path surface for the numeric-index module: the [`StreamRange`]
//! trait, the [`Range`] → index-key-bounds conversion, and the
//! [`PayloadFieldIndexRead`] implementation for [`NumericIndex`].

use std::ops::Bound;
use std::ops::Bound::{Excluded, Included, Unbounded};

use blobstore::Blob;
use common::types::PointOffsetType;
use serde_json::Value;

use super::{Encodable, NumericIndex, NumericIndexValue};
use crate::common::operation_error::OperationResult;
use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndexRead,
};
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
        // A `Range` may carry both `gt` and `gte` (or both `lt` and `lte`), and
        // `Range::check_range` ANDs all four bounds. The index key bounds must
        // honor that intersection, so for each side we keep the tighter bound
        // instead of whichever happens to be listed first. `gt`/`lt` win ties so
        // an exact boundary value stays excluded, matching `check_range`.
        let start_bound = match (self.gt, self.gte) {
            (Some(gt), Some(gte)) if gt >= gte => Excluded(Point::new(gt, PointOffsetType::MAX)),
            (_, Some(gte)) => Included(Point::new(gte, PointOffsetType::MIN)),
            (Some(gt), None) => Excluded(Point::new(gt, PointOffsetType::MAX)),
            (None, None) => Unbounded,
        };

        let end_bound = match (self.lt, self.lte) {
            (Some(lt), Some(lte)) if lt <= lte => Excluded(Point::new(lt, PointOffsetType::MIN)),
            (_, Some(lte)) => Included(Point::new(lte, PointOffsetType::MAX)),
            (Some(lt), None) => Excluded(Point::new(lt, PointOffsetType::MIN)),
            (None, None) => Unbounded,
        };

        (start_bound, end_bound)
    }
}

impl<T: NumericIndexValue, P> PayloadFieldIndexRead for NumericIndex<T, P>
where
    Vec<T>: Blob,
{
    fn count_indexed_points(&self) -> OperationResult<usize> {
        self.inner.count_indexed_points()
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        self.inner.filter(condition)
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        self.inner.estimate_cardinality(condition)
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
    ) -> OperationResult<Option<ConditionCheckerEnum<'a>>> {
        self.inner.condition_checker(condition)
    }

    fn special_check_condition(
        &self,
        condition: &FieldCondition,
        payload_value: &Value,
    ) -> OperationResult<Option<bool>> {
        self.inner.special_check_condition(condition, payload_value)
    }
}
