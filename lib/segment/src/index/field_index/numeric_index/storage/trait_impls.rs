//! [`PayloadFieldIndex`], [`PayloadFieldIndexRead`], and [`StreamRange`]
//! implementations for [`NumericIndexInner`].
//!
//! The substantial query logic — filter range conversion, payload-block
//! iteration, condition checking, and ordered streaming — lives here.
//! Cardinality helpers are in [`super::statistics`]; simple
//! match-and-forward dispatch is in [`super`].
//!
//! [`PayloadFieldIndex`]: crate::index::field_index::PayloadFieldIndex
//! [`PayloadFieldIndexRead`]: crate::index::field_index::PayloadFieldIndexRead
//! [`StreamRange`]: super::super::StreamRange

use std::ops::Bound;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::path::PathBuf;
use std::str::FromStr;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::either_variant::EitherVariant;
use common::types::PointOffsetType;
use gridstore::Blob;
use ordered_float::OrderedFloat;
use uuid::Uuid;

use super::super::{Encodable, StreamRange};
use super::NumericIndexInner;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::numeric_point::Numericable;
use crate::index::field_index::stored_point_to_values::StoredValue;
use crate::index::field_index::utils::check_boundaries;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, PayloadFieldIndexRead,
    PrimaryCondition,
};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::types::{
    FieldCondition, Match, MatchValue, PayloadKeyType, Range, RangeInterface, ValueVariants,
};

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
        if let Some(Match::Value(MatchValue {
            value: ValueVariants::String(keyword),
        })) = &condition.r#match
        {
            let keyword = keyword.as_str();

            if let Ok(uuid) = Uuid::from_str(keyword) {
                let value = T::from_u128(uuid.as_u128());
                return Ok(Some(self.point_ids_by_value(value, hw_counter)?));
            }
        }

        let Some(range_cond) = condition.range.as_ref() else {
            return Ok(None);
        };

        let (start_bound, end_bound) = match range_cond {
            RangeInterface::Float(float_range) => float_range.map(|float| T::from_f64(float.0)),
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

        let result: Box<dyn Iterator<Item = PointOffsetType> + 'a> = match self {
            NumericIndexInner::Mutable(index) => {
                Box::new(index.values_range(start_bound, end_bound))
            }
            NumericIndexInner::Immutable(index) => {
                Box::new(index.values_range(start_bound, end_bound))
            }

            NumericIndexInner::Mmap(index) => {
                Box::new(index.values_range(start_bound, end_bound, hw_counter)?)
            }
        };

        Ok(Some(result))
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        if let Some(Match::Value(MatchValue {
            value: ValueVariants::String(keyword),
        })) = &condition.r#match
        {
            let keyword = keyword.as_str();
            if let Ok(uuid) = Uuid::from_str(keyword) {
                let key = T::from_u128(uuid.as_u128());

                let estimated_count = self.estimate_points(&key, hw_counter)?;
                return Ok(Some(
                    CardinalityEstimation::exact(estimated_count).with_primary_clause(
                        PrimaryCondition::Condition(Box::new(condition.clone())),
                    ),
                ));
            }
        }

        condition
            .range
            .as_ref()
            .map(|range| {
                let mut cardinality = self.range_cardinality(range)?;
                cardinality
                    .primary_clauses
                    .push(PrimaryCondition::Condition(Box::new(condition.clone())));
                Ok(cardinality)
            })
            .transpose()
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        let inner = || -> OperationResult<Vec<PayloadBlockCondition>> {
            let mut lower_bound = Unbounded;
            let mut pre_lower_bound: Option<Bound<T>> = None;
            let mut payload_conditions = Vec::new();

            let value_per_point =
                self.total_unique_values_count()? as f64 / self.get_points_count() as f64;
            let effective_threshold = (threshold as f64 * value_per_point) as usize;

            loop {
                let upper_bound = self
                    .get_histogram()
                    .get_range_by_size(lower_bound, effective_threshold / 2);

                if let Some(pre_lower_bound) = pre_lower_bound {
                    let range = Range {
                        lt: match upper_bound {
                            Excluded(val) => Some(OrderedFloat(val.to_f64())),
                            _ => None,
                        },
                        gt: match pre_lower_bound {
                            Excluded(val) => Some(OrderedFloat(val.to_f64())),
                            _ => None,
                        },
                        gte: match pre_lower_bound {
                            Included(val) => Some(OrderedFloat(val.to_f64())),
                            _ => None,
                        },
                        lte: match upper_bound {
                            Included(val) => Some(OrderedFloat(val.to_f64())),
                            _ => None,
                        },
                    };
                    let cardinality = self.range_cardinality(&RangeInterface::Float(range))?;
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
                        cardinality: self.get_points_count(),
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

        inner()?.into_iter().try_for_each(f)
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> Option<ConditionCheckerFn<'a>> {
        // Destructure explicitly (no `..`) so a new field added to
        // `FieldCondition` forces this method to be revisited.
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
        // Same conversion `filter` already uses — `T::from_f64` /
        // `T::from_u128` are total functions provided by `Numericable`,
        // so every numeric variant (Int / Float / Datetime / Uuid)
        // can serve any `RangeInterface` shape. This brings
        // `condition_checker` in line with `filter` (the legacy
        // helpers were stricter, but the schema layer normally
        // prevents cross-type range queries from reaching here).
        let typed_range = match range {
            RangeInterface::Float(float_range) => float_range.map(|float| T::from_f64(float.0)),
            RangeInterface::DateTime(datetime_range) => {
                datetime_range.map(|dt| T::from_u128(dt.timestamp() as u128))
            }
        };

        let hw_counter = hw_acc.get_counter_cell();
        Some(Box::new(move |point_id: PointOffsetType| {
            self.check_values_any(
                point_id,
                |value| typed_range.check_range(*value),
                &hw_counter,
            )
        }))
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
        let range = match range {
            RangeInterface::Float(float_range) => float_range.map(|float| T::from_f64(float.0)),
            RangeInterface::DateTime(datetime_range) => {
                datetime_range.map(|dt| T::from_u128(dt.timestamp() as u128))
            }
        };
        let (start_bound, end_bound) = range.as_index_key_bounds();

        // map.range
        // Panics if range start > end. Panics if range start == end and both bounds are Excluded.
        if !check_boundaries(&start_bound, &end_bound) {
            return Ok(EitherVariant::A(std::iter::empty()));
        }

        Ok(match self {
            NumericIndexInner::Mutable(index) => {
                EitherVariant::B(index.orderable_values_range(start_bound, end_bound))
            }
            NumericIndexInner::Immutable(index) => {
                EitherVariant::C(index.orderable_values_range(start_bound, end_bound))
            }
            NumericIndexInner::Mmap(index) => {
                EitherVariant::D(index.orderable_values_range(start_bound, end_bound)?)
            }
        })
    }
}
