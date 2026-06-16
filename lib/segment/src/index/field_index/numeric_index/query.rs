//! Generic query helpers over [`NumericIndexRead`]: cardinality
//! estimation, filtering, payload-block iteration, condition checking,
//! and ordered range streaming.
//!
//! These free functions are written purely against the
//! [`NumericIndexRead`] interface, so every index variant — writable,
//! read-only, or the dispatch enums — can reuse the same query logic
//! without duplicating it.

use std::cmp::{max, min};
use std::ops::Bound;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::str::FromStr;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use itertools::Either;
use ordered_float::OrderedFloat;
use uuid::Uuid;

use super::Encodable;
use super::numeric_index_read::NumericIndexRead;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::on_disk_point_to_values::StoredValue;
use crate::index::field_index::stat_tools::estimate_multi_value_selection_cardinality;
use crate::index::field_index::utils::check_boundaries;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition, PrimaryCondition};
use crate::index::query_optimization::optimized_filter::ConditionChecker;
use crate::types::{
    FieldCondition, Match, MatchValue, PayloadKeyType, Range, RangeInterface, ValueVariants,
};

/// Histogram-driven cardinality estimation for a range condition.
pub(super) fn range_cardinality<T, I>(
    index: &I,
    range: &RangeInterface,
) -> OperationResult<CardinalityEstimation>
where
    T: Encodable + Numericable + StoredValue + Send + Sync + Default,
    I: NumericIndexRead<T>,
{
    let max_values_per_point = index.get_max_values_per_point();
    if max_values_per_point == 0 {
        return Ok(CardinalityEstimation::exact(0));
    }

    let range = match range {
        RangeInterface::Float(float_range) => T::from_f64_range(*float_range),
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

    let histogram_estimation = index.get_histogram().estimate(gbound, lbound);
    let min_estimation = histogram_estimation.0;
    let max_estimation = histogram_estimation.2;

    let total_values = index.total_unique_values_count()?;
    // Note: max_values_per_point is never zero here because we check it above
    let expected_min = max(
        min_estimation / max_values_per_point,
        max(
            min(1, min_estimation),
            min_estimation.saturating_sub(total_values - index.get_points_count()),
        ),
    );
    let expected_max = min(index.get_points_count(), max_estimation);

    let estimation = estimate_multi_value_selection_cardinality(
        index.get_points_count(),
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

/// Estimate the number of points carrying exactly `value`.
pub(super) fn estimate_points<T, I>(
    index: &I,
    value: &T,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize>
where
    T: Encodable + Numericable + StoredValue + Send + Sync + Default,
    I: NumericIndexRead<T>,
{
    let start = Bound::Included(Point::new(*value, PointOffsetType::MIN));
    let end = Bound::Included(Point::new(*value, PointOffsetType::MAX));

    hw_counter
        .payload_index_io_read_counter()
        // We have to do 2 times binary search in mmap and immutable storage.
        .incr_delta(2 * ((index.total_unique_values_count()? as f32).log2().ceil() as usize));

    let range_size = index.values_range_size(start, end, hw_counter)?;
    if range_size == 0 {
        return Ok(0);
    }
    let avg_values_per_point =
        index.total_unique_values_count()? as f32 / index.get_points_count() as f32;
    Ok((range_size as f32 / avg_values_per_point).max(1.0).round() as usize)
}

/// Point iterator for a `match`/`range` field condition.
///
/// Returns `Ok(None)` when the condition is not one a numeric index can
/// serve.
pub(super) fn filter<'a, T, I>(
    index: &'a I,
    condition: &FieldCondition,
    hw_counter: &'a HardwareCounterCell,
) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>>
where
    T: Encodable + Numericable + StoredValue + Send + Sync + Default,
    I: NumericIndexRead<T>,
{
    if let Some(Match::Value(MatchValue {
        value: ValueVariants::String(keyword),
    })) = &condition.r#match
    {
        let keyword = keyword.as_str();

        if let Ok(uuid) = Uuid::from_str(keyword) {
            let value = T::from_u128(uuid.as_u128());
            let start = Bound::Included(Point::new(value, PointOffsetType::MIN));
            let end = Bound::Included(Point::new(value, PointOffsetType::MAX));
            return Ok(Some(Box::new(index.values_range(start, end, hw_counter)?)));
        }
    }

    let Some(range_cond) = condition.range.as_ref() else {
        return Ok(None);
    };

    let (start_bound, end_bound) = match range_cond {
        RangeInterface::Float(float_range) => T::from_f64_range(*float_range),
        RangeInterface::DateTime(datetime_range) => {
            datetime_range.map(|dt| T::from_u128(dt.timestamp() as u128))
        }
    }
    .as_index_key_bounds();

    // map.range
    // Panics if range start > end. Panics if range start == end and both bounds are Excluded.
    if !check_boundaries(&start_bound, &end_bound) {
        return Ok(Some(Box::new(std::iter::empty())));
    }

    Ok(Some(Box::new(index.values_range(
        start_bound,
        end_bound,
        hw_counter,
    )?)))
}

/// Cardinality estimation for a `match`/`range` field condition.
pub(super) fn estimate_cardinality<T, I>(
    index: &I,
    condition: &FieldCondition,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<Option<CardinalityEstimation>>
where
    T: Encodable + Numericable + StoredValue + Send + Sync + Default,
    I: NumericIndexRead<T>,
{
    if let Some(Match::Value(MatchValue {
        value: ValueVariants::String(keyword),
    })) = &condition.r#match
    {
        let keyword = keyword.as_str();
        if let Ok(uuid) = Uuid::from_str(keyword) {
            let key = T::from_u128(uuid.as_u128());

            let estimated_count = estimate_points(index, &key, hw_counter)?;
            return Ok(Some(
                CardinalityEstimation::exact(estimated_count)
                    .with_primary_clause(PrimaryCondition::Condition(Box::new(condition.clone()))),
            ));
        }
    }

    condition
        .range
        .as_ref()
        .map(|range| {
            let mut cardinality = range_cardinality(index, range)?;
            cardinality
                .primary_clauses
                .push(PrimaryCondition::Condition(Box::new(condition.clone())));
            Ok(cardinality)
        })
        .transpose()
}

/// Iterate histogram-balanced payload blocks of at least `threshold` size.
pub(super) fn for_each_payload_block<T, I>(
    index: &I,
    threshold: usize,
    key: PayloadKeyType,
    f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
) -> OperationResult<()>
where
    T: Encodable + Numericable + StoredValue + Send + Sync + Default,
    I: NumericIndexRead<T>,
{
    let collect_blocks = || -> OperationResult<Vec<PayloadBlockCondition>> {
        let mut lower_bound = Unbounded;
        let mut pre_lower_bound: Option<Bound<T>> = None;
        let mut payload_conditions = Vec::new();

        let value_per_point =
            index.total_unique_values_count()? as f64 / index.get_points_count() as f64;
        let effective_threshold = (threshold as f64 * value_per_point) as usize;

        loop {
            let upper_bound = index
                .get_histogram()
                .get_range_by_size(lower_bound, effective_threshold / 2);

            if let Some(pre_lower_bound) = pre_lower_bound {
                let range = Range {
                    lt: match upper_bound {
                        Excluded(val) => Some(OrderedFloat(val.to_f64())),
                        Included(_) | Unbounded => None,
                    },
                    gt: match pre_lower_bound {
                        Excluded(val) => Some(OrderedFloat(val.to_f64())),
                        Included(_) | Unbounded => None,
                    },
                    gte: match pre_lower_bound {
                        Included(val) => Some(OrderedFloat(val.to_f64())),
                        Excluded(_) | Unbounded => None,
                    },
                    lte: match upper_bound {
                        Included(val) => Some(OrderedFloat(val.to_f64())),
                        Excluded(_) | Unbounded => None,
                    },
                };
                let cardinality = range_cardinality(index, &RangeInterface::Float(range))?;
                let condition = PayloadBlockCondition {
                    condition: FieldCondition::new_range(key.clone(), range),
                    cardinality: cardinality.exp,
                };

                payload_conditions.push(condition);
            } else if upper_bound == Unbounded {
                // One block covers all points
                payload_conditions.push(PayloadBlockCondition {
                    condition: FieldCondition::new_range(
                        key.clone(),
                        Range {
                            gte: None,
                            lte: None,
                            lt: None,
                            gt: None,
                        },
                    ),
                    cardinality: index.get_points_count(),
                });
            }

            pre_lower_bound = Some(lower_bound);

            lower_bound = match upper_bound {
                Included(val) => Excluded(val),
                Excluded(val) => Excluded(val),
                Unbounded => break,
            };
        }
        Ok(payload_conditions)
    };

    collect_blocks()?.into_iter().try_for_each(f)
}

/// Build a per-point checker closure for a `range` field condition, if the
/// index can serve it.
pub(super) fn condition_checker<'a, T, I>(
    index: &'a I,
    condition: &FieldCondition,
    hw_acc: HwMeasurementAcc,
) -> Option<Box<dyn ConditionChecker + 'a>>
where
    T: Encodable + Numericable + StoredValue + Send + Sync + Default,
    I: NumericIndexRead<T>,
{
    // Destructure explicitly (no `..`) so a new field added to
    // `FieldCondition` forces this function to be revisited.
    let FieldCondition {
        key: _,
        r#match: _,
        range,
        geo_radius: _,
        geo_bounding_box: _,
        geo_polygon: _,
        values_count: _,
        is_empty: _,
        is_null: _,
    } = condition;

    let range = range.as_ref()?;
    // Convert the range bounds into the index's storage type `T`.
    // `T::from_f64_range` / `T::from_u128` are total functions provided by
    // `Numericable`, so every numeric variant (Int / Float / Datetime /
    // Uuid) can serve any `RangeInterface` shape. For integer `T`, the
    // float-range conversion rounds each bound *away* from the matching
    // set so fractional bounds keep their `f64`-comparison semantics.
    let typed_range = match range {
        RangeInterface::Float(float_range) => T::from_f64_range(*float_range),
        RangeInterface::DateTime(datetime_range) => {
            datetime_range.map(|dt| T::from_u128(dt.timestamp() as u128))
        }
    };

    let hw_counter = hw_acc.get_counter_cell();
    Some(Box::new(move |point_id: PointOffsetType| {
        Ok(index.check_values_any(
            point_id,
            |value| typed_range.check_range(*value),
            &hw_counter,
        ))
    }))
}

/// Stream `(value, point)` pairs of the given range in ascending order.
///
/// The iterator is double-ended, so callers can also walk it in
/// descending order.
pub(super) fn stream_range<'a, T, I>(
    index: &'a I,
    range: &RangeInterface,
) -> OperationResult<impl DoubleEndedIterator<Item = (T, PointOffsetType)> + 'a>
where
    T: Encodable + Numericable + StoredValue + Send + Sync + Default,
    I: NumericIndexRead<T>,
{
    let range = match range {
        RangeInterface::Float(float_range) => T::from_f64_range(*float_range),
        RangeInterface::DateTime(datetime_range) => {
            datetime_range.map(|dt| T::from_u128(dt.timestamp() as u128))
        }
    };
    let (start_bound, end_bound) = range.as_index_key_bounds();

    // map.range
    // Panics if range start > end. Panics if range start == end and both bounds are Excluded.
    if !check_boundaries(&start_bound, &end_bound) {
        return Ok(Either::Left(std::iter::empty()));
    }

    Ok(Either::Right(
        index.orderable_values_range(start_bound, end_bound)?,
    ))
}
