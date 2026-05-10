use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::types::PointOffsetType;
use ordered_float::OrderedFloat;

use super::match_converter::get_match_checkers;
use crate::index::field_index::{FieldIndex, FieldIndexRead};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::types::{
    DateTimePayloadType, FieldCondition, FloatPayloadType, IntPayloadType, Range, RangeInterface,
};

pub(super) fn field_condition_index<'a>(
    index: &'a FieldIndex,
    field_condition: &FieldCondition,
    hw_acc: HwMeasurementAcc,
) -> Option<ConditionCheckerFn<'a>> {
    // Try the polymorphic dispatch first. As variants migrate their
    // condition handling into `PayloadFieldIndexRead::condition_checker`,
    // this short-circuits more cases. The legacy match below shrinks
    // until it can be deleted entirely (see
    // docs/plans/field-index-read-trait/condition-checker-migration/).
    if let Some(checker) = index.condition_checker(field_condition, hw_acc.clone()) {
        return Some(checker);
    }

    match field_condition {
        FieldCondition {
            r#match: Some(cond_match),
            ..
        } => get_match_checkers(index, cond_match.clone(), hw_acc),

        FieldCondition {
            range: Some(cond), ..
        } => get_range_checkers(index, *cond, hw_acc),

        FieldCondition {
            key: _,
            r#match: None,
            range: None,
            // Geo / is_empty / is_null conditions are served via
            // `condition_checker` on the relevant typed indexes
            // (GeoMapIndex / NullIndex). If we got here, no
            // condition_checker fired for this field's indexes —
            // returning `None` falls back to the payload check.
            geo_radius: _,
            geo_bounding_box: _,
            geo_polygon: _,
            // We can't use index for this condition, since some indices don't count values,
            // like boolean index, where [true, true, true] is the same as [true]. Count should be 3 but they think is 1.
            //
            // TODO: Try to use the indices that actually support counting values.
            values_count: _,
            is_empty: _,
            is_null: _,
        } => None,
    }
}

fn get_range_checkers(
    index: &FieldIndex,
    range: RangeInterface,
    hw_acc: HwMeasurementAcc,
) -> Option<ConditionCheckerFn<'_>> {
    match range {
        RangeInterface::Float(range) => get_float_range_checkers(index, range, hw_acc),
        RangeInterface::DateTime(range) => get_datetime_range_checkers(index, range, hw_acc),
    }
}

fn get_float_range_checkers(
    index: &FieldIndex,
    range: Range<OrderedFloat<FloatPayloadType>>,
    hw_acc: HwMeasurementAcc,
) -> Option<ConditionCheckerFn<'_>> {
    let hw_counter = hw_acc.get_counter_cell();
    match index {
        FieldIndex::IntIndex(num_index) => {
            let range = range.map(|f| f.0 as IntPayloadType);
            Some(Box::new(move |point_id: PointOffsetType| {
                num_index.check_values_any(point_id, |value| range.check_range(*value), &hw_counter)
            }))
        }
        FieldIndex::FloatIndex(num_index) => Some(Box::new(move |point_id: PointOffsetType| {
            num_index.check_values_any(
                point_id,
                |value| range.check_range(OrderedFloat(*value)),
                &hw_counter,
            )
        })),
        FieldIndex::BoolIndex(_)
        | FieldIndex::DatetimeIndex(_)
        | FieldIndex::FullTextIndex(_)
        | FieldIndex::GeoIndex(_)
        | FieldIndex::IntMapIndex(_)
        | FieldIndex::KeywordIndex(_)
        | FieldIndex::UuidIndex(_)
        | FieldIndex::UuidMapIndex(_)
        | FieldIndex::NullIndex(_) => None,
    }
}

fn get_datetime_range_checkers(
    index: &FieldIndex,
    range: Range<DateTimePayloadType>,
    hw_acc: HwMeasurementAcc,
) -> Option<ConditionCheckerFn<'_>> {
    match index {
        FieldIndex::DatetimeIndex(num_index) => {
            let range = range.map(|dt| dt.timestamp());
            let hw_counter = hw_acc.get_counter_cell();
            Some(Box::new(move |point_id: PointOffsetType| {
                num_index.check_values_any(point_id, |value| range.check_range(*value), &hw_counter)
            }))
        }
        FieldIndex::BoolIndex(_)
        | FieldIndex::FloatIndex(_)
        | FieldIndex::FullTextIndex(_)
        | FieldIndex::GeoIndex(_)
        | FieldIndex::IntIndex(_)
        | FieldIndex::IntMapIndex(_)
        | FieldIndex::KeywordIndex(_)
        | FieldIndex::UuidIndex(_)
        | FieldIndex::UuidMapIndex(_)
        | FieldIndex::NullIndex(_) => None,
    }
}
