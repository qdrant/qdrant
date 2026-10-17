use common::types::PointOffsetType;
use itertools::Either;

use super::point_runs::PointRuns;
use super::{ElementMap, ElementMapRead};
use crate::common::operation_error::OperationResult;
use crate::data_types::order_by::OrderValue;
use crate::index::field_index::numeric_index::{NumericFieldIndex, NumericFieldIndexRead};
use crate::types::RangeInterface;

/// Order-by view of a per-element numeric index, over points.
///
/// `inner` is the numeric view of the inner index, keyed by the offsets of `element_map`.
pub struct NestedNumericView<'a, N, M> {
    inner: N,
    element_map: &'a M,
}

impl<'a, N, M> NestedNumericView<'a, N, M> {
    pub fn new(inner: N, element_map: &'a M) -> Self {
        Self { inner, element_map }
    }
}

impl<N: NumericFieldIndexRead, M: ElementMapRead> NumericFieldIndexRead
    for NestedNumericView<'_, N, M>
{
    fn get_ordering_values(&self, idx: PointOffsetType) -> impl Iterator<Item = OrderValue> + '_ {
        self.element_map
            .point_elements(idx)
            .flat_map(|element| self.inner.get_ordering_values(element))
    }

    fn stream_range(
        &self,
        range: &RangeInterface,
    ) -> OperationResult<impl DoubleEndedIterator<Item = (OrderValue, PointOffsetType)> + '_> {
        Ok(PointRuns::new(
            self.inner.stream_range(range)?,
            self.element_map,
        ))
    }
}

/// Order-by view of any writable field index: a flattened one, or a per-element one.
pub enum NumericView<'a> {
    Plain(NumericFieldIndex<'a>),
    Nested(NestedNumericView<'a, NumericFieldIndex<'a>, ElementMap>),
}

impl NumericFieldIndexRead for NumericView<'_> {
    fn get_ordering_values(&self, idx: PointOffsetType) -> impl Iterator<Item = OrderValue> + '_ {
        match self {
            NumericView::Plain(index) => Either::Left(index.get_ordering_values(idx)),
            NumericView::Nested(index) => Either::Right(index.get_ordering_values(idx)),
        }
    }

    fn stream_range(
        &self,
        range: &RangeInterface,
    ) -> OperationResult<impl DoubleEndedIterator<Item = (OrderValue, PointOffsetType)> + '_> {
        Ok(match self {
            NumericView::Plain(index) => {
                Either::Left(NumericFieldIndexRead::stream_range(index, range)?)
            }
            NumericView::Nested(index) => Either::Right(index.stream_range(range)?),
        })
    }
}
