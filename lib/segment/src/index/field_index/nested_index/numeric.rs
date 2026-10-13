use atomic_refcell::AtomicRef;
use common::types::PointOffsetType;
use itertools::Either;

use super::ElementMap;
use crate::common::operation_error::OperationResult;
use crate::data_types::order_by::OrderValue;
use crate::index::field_index::numeric_index::{NumericFieldIndex, NumericFieldIndexRead};
use crate::types::RangeInterface;

/// Order-by view of a per-element numeric index, over points.
pub struct NestedNumericView<'a> {
    inner: NumericFieldIndex<'a>,
    element_map: AtomicRef<'a, ElementMap>,
}

impl<'a> NestedNumericView<'a> {
    pub fn new(inner: NumericFieldIndex<'a>, element_map: AtomicRef<'a, ElementMap>) -> Self {
        Self { inner, element_map }
    }
}

impl NumericFieldIndexRead for NestedNumericView<'_> {
    fn get_ordering_values(&self, idx: PointOffsetType) -> impl Iterator<Item = OrderValue> + '_ {
        self.element_map
            .point_elements(idx)
            .flat_map(|element| self.inner.get_ordering_values(element))
    }

    fn stream_range(
        &self,
        range: &RangeInterface,
    ) -> OperationResult<impl DoubleEndedIterator<Item = (OrderValue, PointOffsetType)> + '_> {
        // Elements of a point are not ordered like points, and a value can repeat across
        // elements, so restore the `(value, point)` order and uniqueness of the flattened
        // index by materializing the range.
        let mut values: Vec<_> = self
            .inner
            .stream_range(range)?
            .filter_map(|(value, element)| {
                let point = self.element_map.element_point(element)?;
                Some((value, point))
            })
            .collect();
        values.sort_unstable();
        values.dedup();
        Ok(values.into_iter())
    }
}

/// Order-by view of any writable field index: a flattened one, or a per-element one.
pub enum NumericView<'a> {
    Plain(NumericFieldIndex<'a>),
    Nested(NestedNumericView<'a>),
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
