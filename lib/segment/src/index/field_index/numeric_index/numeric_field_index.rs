use common::types::PointOffsetType;
use itertools::Either;

use super::numeric_index_read::NumericIndexRead;
use super::{NumericIndexInner, ReadOnlyNumericIndexInner, StreamRange};
use crate::common::operation_error::OperationResult;
use crate::data_types::order_by::OrderValue;
use crate::types::{FloatPayloadType, IntPayloadType, RangeInterface};

/// Type-erased Int/Float numeric index view for order-by reads.
///
/// Generic over the two concrete storage backends — `I` serves the
/// `i64`-typed index, `F` the `f64`-typed one — so the writable and
/// read-only index hierarchies share one set of impls. Use the
/// [`NumericFieldIndex`] / [`ReadOnlyNumericFieldIndex`] aliases rather
/// than spelling out the backend types.
pub enum NumericFieldIndexView<'a, I, F> {
    IntIndex(&'a I),
    FloatIndex(&'a F),
}

/// [`NumericFieldIndexView`] over the writable storage enum.
pub type NumericFieldIndex<'a> = NumericFieldIndexView<
    'a,
    NumericIndexInner<IntPayloadType>,
    NumericIndexInner<FloatPayloadType>,
>;

/// [`NumericFieldIndexView`] over the read-only storage enum.
pub type ReadOnlyNumericFieldIndex<'a, S> = NumericFieldIndexView<
    'a,
    ReadOnlyNumericIndexInner<IntPayloadType, S>,
    ReadOnlyNumericIndexInner<FloatPayloadType, S>,
>;

impl<'a, I, F> StreamRange<OrderValue> for NumericFieldIndexView<'a, I, F>
where
    I: NumericIndexRead<IntPayloadType> + StreamRange<IntPayloadType>,
    F: NumericIndexRead<FloatPayloadType> + StreamRange<FloatPayloadType>,
{
    fn stream_range(
        &self,
        range: &RangeInterface,
    ) -> OperationResult<impl DoubleEndedIterator<Item = (OrderValue, PointOffsetType)> + '_> {
        Ok(match self {
            NumericFieldIndexView::IntIndex(index) => Either::Left(
                index
                    .stream_range(range)?
                    .map(|(v, p)| (OrderValue::from(v), p)),
            ),
            NumericFieldIndexView::FloatIndex(index) => Either::Right(
                index
                    .stream_range(range)?
                    .map(|(v, p)| (OrderValue::from(v), p)),
            ),
        })
    }
}

impl<'a, I, F> NumericFieldIndexView<'a, I, F>
where
    I: NumericIndexRead<IntPayloadType>,
    F: NumericIndexRead<FloatPayloadType>,
{
    pub fn get_ordering_values(
        &self,
        idx: PointOffsetType,
    ) -> impl Iterator<Item = OrderValue> + 'a {
        match self {
            NumericFieldIndexView::IntIndex(index) => Either::Left(
                index
                    .get_values(idx)
                    .into_iter()
                    .flatten()
                    .map(OrderValue::Int),
            ),
            NumericFieldIndexView::FloatIndex(index) => Either::Right(
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
/// Implemented by [`NumericFieldIndexView`] over any pair of storage
/// backends, so the writable and read-only index hierarchies (and a
/// future `ReadOnlySegment`) share one order-by implementation.
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

impl<'a, I, F> NumericFieldIndexRead for NumericFieldIndexView<'a, I, F>
where
    I: NumericIndexRead<IntPayloadType> + StreamRange<IntPayloadType>,
    F: NumericIndexRead<FloatPayloadType> + StreamRange<FloatPayloadType>,
{
    fn get_ordering_values(&self, idx: PointOffsetType) -> impl Iterator<Item = OrderValue> + '_ {
        NumericFieldIndexView::get_ordering_values(self, idx)
    }

    fn stream_range(
        &self,
        range: &RangeInterface,
    ) -> OperationResult<impl DoubleEndedIterator<Item = (OrderValue, PointOffsetType)> + '_> {
        StreamRange::stream_range(self, range)
    }
}
