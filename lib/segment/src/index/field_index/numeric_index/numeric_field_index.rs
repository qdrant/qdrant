use common::types::PointOffsetType;
use itertools::Either;

use super::{NumericIndexInner, StreamRange};
use crate::common::operation_error::OperationResult;
use crate::data_types::order_by::OrderValue;
use crate::types::{FloatPayloadType, IntPayloadType, RangeInterface};

pub enum NumericFieldIndex<'a> {
    IntIndex(&'a NumericIndexInner<IntPayloadType>),
    FloatIndex(&'a NumericIndexInner<FloatPayloadType>),
}

impl<'a> StreamRange<OrderValue> for NumericFieldIndex<'a> {
    fn stream_range(
        &self,
        range: &RangeInterface,
    ) -> OperationResult<impl DoubleEndedIterator<Item = (OrderValue, PointOffsetType)> + '_> {
        Ok(match self {
            NumericFieldIndex::IntIndex(index) => Either::Left(
                index
                    .stream_range(range)?
                    .map(|(v, p)| (OrderValue::from(v), p)),
            ),
            NumericFieldIndex::FloatIndex(index) => Either::Right(
                index
                    .stream_range(range)?
                    .map(|(v, p)| (OrderValue::from(v), p)),
            ),
        })
    }
}

impl<'a> NumericFieldIndex<'a> {
    pub fn get_ordering_values(
        &self,
        idx: PointOffsetType,
    ) -> impl Iterator<Item = OrderValue> + 'a {
        match self {
            NumericFieldIndex::IntIndex(index) => Either::Left(
                index
                    .get_values(idx)
                    .into_iter()
                    .flatten()
                    .map(OrderValue::Int),
            ),
            NumericFieldIndex::FloatIndex(index) => Either::Right(
                index
                    .get_values(idx)
                    .into_iter()
                    .flatten()
                    .map(OrderValue::Float),
            ),
        }
    }
}

/// Read-only abstraction over a per-key numeric index.
///
/// Implemented by the appendable [`NumericFieldIndex`] today; a future
/// `ReadOnlySegment` will provide its own concrete numeric-index type with
/// the same shape, so order-by reads can share one implementation.
///
/// Returned iterators borrow from `&self` — callers hold them within the
/// scope of the borrow.
pub trait NumericFieldIndexRead {
    fn get_ordering_values(&self, idx: PointOffsetType) -> impl Iterator<Item = OrderValue> + '_;

    fn stream_range(
        &self,
        range: &RangeInterface,
    ) -> OperationResult<impl DoubleEndedIterator<Item = (OrderValue, PointOffsetType)> + '_>;
}

impl<'a> NumericFieldIndexRead for NumericFieldIndex<'a> {
    fn get_ordering_values(&self, idx: PointOffsetType) -> impl Iterator<Item = OrderValue> + '_ {
        NumericFieldIndex::get_ordering_values(self, idx)
    }

    fn stream_range(
        &self,
        range: &RangeInterface,
    ) -> OperationResult<impl DoubleEndedIterator<Item = (OrderValue, PointOffsetType)> + '_> {
        StreamRange::stream_range(self, range)
    }
}
