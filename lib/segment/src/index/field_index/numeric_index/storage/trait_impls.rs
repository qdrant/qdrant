//! [`PayloadFieldIndex`], [`PayloadFieldIndexRead`], and [`StreamRange`]
//! implementations for [`NumericIndexInner`].
//!
//! The query logic (filtering, cardinality estimation, payload-block
//! iteration, condition checking) is shared with the read-only index via
//! the generic [`query`](super::super::query) helpers over
//! [`NumericIndexRead`]; the `PayloadFieldIndexRead` impl here just plugs
//! the dispatch enum into them.
//!
//! [`PayloadFieldIndex`]: crate::index::field_index::PayloadFieldIndex
//! [`PayloadFieldIndexRead`]: crate::index::field_index::PayloadFieldIndexRead
//! [`StreamRange`]: super::super::StreamRange

use std::path::PathBuf;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::Blob;

use super::super::numeric_index_read::NumericIndexRead;
use super::super::{Encodable, StreamRange, query};
use super::NumericIndexInner;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::numeric_point::Numericable;
use crate::index::field_index::stored_point_to_values::StoredValue;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, PayloadFieldIndexRead,
};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::types::{FieldCondition, PayloadKeyType, RangeInterface};

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default> PayloadFieldIndex
    for NumericIndexInner<T>
where
    Vec<T>: Blob,
{
    fn wipe(self) -> OperationResult<()> {
        match self {
            NumericIndexInner::Mutable(index) => index.wipe(),
            NumericIndexInner::Immutable(index) => index.wipe(),
            NumericIndexInner::Mmap(index) => index.wipe(),
        }
    }

    fn flusher(&self) -> Flusher {
        NumericIndexInner::flusher(self)
    }

    fn files(&self) -> Vec<PathBuf> {
        NumericIndexInner::files(self)
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        NumericIndexInner::immutable_files(self)
    }
}

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default> PayloadFieldIndexRead
    for NumericIndexInner<T>
where
    Vec<T>: Blob,
{
    fn count_indexed_points(&self) -> usize {
        self.get_points_count()
    }

    fn filter<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        query::filter(self, condition, hw_counter)
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        query::estimate_cardinality(self, condition, hw_counter)
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        query::for_each_payload_block(self, threshold, key, f)
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> Option<ConditionCheckerFn<'a>> {
        query::condition_checker(self, condition, hw_acc)
    }
}

impl<T> StreamRange<T> for NumericIndexInner<T>
where
    T: Encodable + Numericable + StoredValue + Send + Sync + Default,
    Vec<T>: Blob,
{
    fn stream_range(
        &self,
        range: &RangeInterface,
    ) -> OperationResult<impl DoubleEndedIterator<Item = (T, PointOffsetType)> + '_> {
        query::stream_range(self, range)
    }
}
