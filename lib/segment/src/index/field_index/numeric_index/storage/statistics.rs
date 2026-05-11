//! Histogram-driven cardinality estimation and point-count helpers.
//!
//! Used by [`super::trait_impls`] (`estimate_cardinality`,
//! `for_each_payload_block`) and by tests. Heavy enough to live apart
//! from the simple match-and-forward dispatch in [`super`].

use std::cmp::{max, min};
use std::ops::Bound;
use std::ops::Bound::{Excluded, Included, Unbounded};

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::Blob;

use super::super::Encodable;
use super::NumericIndexInner;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::CardinalityEstimation;
use crate::index::field_index::histogram::Histogram;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::stat_tools::estimate_multi_value_selection_cardinality;
use crate::index::field_index::stored_point_to_values::StoredValue;
use crate::types::RangeInterface;

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default> NumericIndexInner<T>
where
    Vec<T>: Blob,
{
    pub(super) fn get_histogram(&self) -> &Histogram<T> {
        match self {
            NumericIndexInner::Mutable(index) => index.get_histogram(),
            NumericIndexInner::Immutable(index) => index.get_histogram(),
            NumericIndexInner::Mmap(index) => index.get_histogram(),
        }
    }

    pub(in crate::index::field_index::numeric_index) fn get_points_count(&self) -> usize {
        match self {
            NumericIndexInner::Mutable(index) => index.get_points_count(),
            NumericIndexInner::Immutable(index) => index.get_points_count(),
            NumericIndexInner::Mmap(index) => index.get_points_count(),
        }
    }

    pub(super) fn total_unique_values_count(&self) -> OperationResult<usize> {
        Ok(match self {
            NumericIndexInner::Mutable(index) => index.total_unique_values_count(),
            NumericIndexInner::Immutable(index) => index.total_unique_values_count(),
            NumericIndexInner::Mmap(index) => index.total_unique_values_count()?,
        })
    }

    pub(in crate::index::field_index::numeric_index) fn range_cardinality(
        &self,
        range: &RangeInterface,
    ) -> OperationResult<CardinalityEstimation> {
        let max_values_per_point = self.max_values_per_point();
        if max_values_per_point == 0 {
            return Ok(CardinalityEstimation::exact(0));
        }

        let range = match range {
            RangeInterface::Float(float_range) => float_range.map(|float| T::from_f64(float.0)),
            RangeInterface::DateTime(datetime_range) => {
                datetime_range.map(|dt| T::from_u128(dt.timestamp() as u128))
            }
        };

        let lbound = if let Some(lte) = range.lte {
            Included(lte)
        } else if let Some(lt) = range.lt {
            Excluded(lt)
        } else {
            Unbounded
        };

        let gbound = if let Some(gte) = range.gte {
            Included(gte)
        } else if let Some(gt) = range.gt {
            Excluded(gt)
        } else {
            Unbounded
        };

        let histogram_estimation = self.get_histogram().estimate(gbound, lbound);
        let min_estimation = histogram_estimation.0;
        let max_estimation = histogram_estimation.2;

        let total_values = self.total_unique_values_count()?;
        // Example: points_count = 1000, total values = 2000, values_count = 500
        // min = max(1, 500 - (2000 - 1000)) = 1
        // exp = 500 / (2000 / 1000) = 250
        // max = min(1000, 500) = 500

        // Example: points_count = 1000, total values = 1200, values_count = 500
        // min = max(1, 500 - (1200 - 1000)) = 300
        // exp = 500 / (1200 / 1000) = 416
        // max = min(1000, 500) = 500
        // Note: max_values_per_point is never zero here because we check it above
        let expected_min = max(
            min_estimation / max_values_per_point,
            max(
                min(1, min_estimation),
                min_estimation.saturating_sub(total_values - self.get_points_count()),
            ),
        );
        let expected_max = min(self.get_points_count(), max_estimation);

        let estimation = estimate_multi_value_selection_cardinality(
            self.get_points_count(),
            total_values,
            histogram_estimation.1,
        )
        .round() as usize;

        Ok(CardinalityEstimation {
            primary_clauses: vec![],
            min: expected_min,
            exp: min(expected_max, max(estimation, expected_min)),
            max: expected_max,
        })
    }

    /// Tries to estimate the amount of points for a given key.
    pub fn estimate_points(
        &self,
        value: &T,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        let start = Bound::Included(Point::new(*value, PointOffsetType::MIN));
        let end = Bound::Included(Point::new(*value, PointOffsetType::MAX));

        hw_counter
            .payload_index_io_read_counter()
            // We have to do 2 times binary search in mmap and immutable storage.
            .incr_delta(2 * ((self.total_unique_values_count()? as f32).log2().ceil() as usize));

        Ok(match &self {
            NumericIndexInner::Mutable(mutable) => {
                let mut iter = mutable.map().range((start, end));
                let first = iter.next();
                let last = iter.next_back();

                match (first, last) {
                    (Some(_), None) => 1,
                    (Some(start), Some(end)) => (start.idx..end.idx).len(),
                    (None, _) => 0,
                }
            }
            NumericIndexInner::Immutable(immutable) => {
                let range_size = immutable.values_range_size(start, end);
                if range_size == 0 {
                    return Ok(0);
                }
                let avg_values_per_point =
                    self.total_unique_values_count()? as f32 / self.get_points_count() as f32;
                (range_size as f32 / avg_values_per_point).max(1.0).round() as usize
            }
            NumericIndexInner::Mmap(mmap) => {
                let range_size = mmap.values_range_size(start, end)?;
                if range_size == 0 {
                    return Ok(0);
                }
                let avg_values_per_point =
                    self.total_unique_values_count()? as f32 / self.get_points_count() as f32;
                (range_size as f32 / avg_values_per_point).max(1.0).round() as usize
            }
        })
    }
}
