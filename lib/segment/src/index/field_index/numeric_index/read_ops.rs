//! Read-path helpers shared across the numeric-index module: the
//! [`StreamRange`] trait and the [`Range`] → index-key-bounds conversion
//! used to translate query ranges into `Point` bounds.

use std::ops::Bound;
use std::ops::Bound::{Excluded, Included, Unbounded};

use common::types::PointOffsetType;

use super::Encodable;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::types::{Range, RangeInterface};

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
