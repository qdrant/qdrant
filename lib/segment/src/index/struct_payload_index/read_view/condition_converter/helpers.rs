use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::types::PointOffsetType;
use ordered_float::OrderedFloat;

use super::match_converter::get_match_checkers;
use crate::index::field_index::null_index::NullIndex;
use crate::index::field_index::{FieldIndex, FieldIndexRead};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::types::{
    DateTimePayloadType, FieldCondition, FloatPayloadType, GeoBoundingBox, GeoPolygon, GeoRadius,
    IntPayloadType, Range, RangeInterface,
};

pub(super) fn field_condition_index<'a>(
    index: &'a FieldIndex,
    field_condition: &FieldCondition,
    hw_acc: HwMeasurementAcc,
) -> Option<ConditionCheckerFn<'a>> {
    match field_condition {
        FieldCondition {
            r#match: Some(cond_match),
            ..
        } => get_match_checkers(index, cond_match.clone(), hw_acc),

        FieldCondition {
            range: Some(cond), ..
        } => get_range_checkers(index, *cond, hw_acc),

        FieldCondition {
            geo_radius: Some(geo_radius),
            ..
        } => get_geo_radius_checkers(index, *geo_radius, hw_acc),

        FieldCondition {
            geo_bounding_box: Some(geo_bounding_box),
            ..
        } => get_geo_bounding_box_checkers(index, *geo_bounding_box, hw_acc),

        FieldCondition {
            geo_polygon: Some(geo_polygon),
            ..
        } => get_geo_polygon_checkers(index, geo_polygon.clone(), hw_acc),

        FieldCondition {
            is_empty: Some(is_empty),
            ..
        } => get_is_empty_checker(index, *is_empty),

        FieldCondition {
            is_null: Some(is_null),
            ..
        } => get_is_null_checker(index, *is_null),

        FieldCondition {
            key: _,
            r#match: None,
            range: None,
            geo_radius: None,
            geo_bounding_box: None,
            geo_polygon: None,
            // We can't use index for this condition, since some indices don't count values,
            // like boolean index, where [true, true, true] is the same as [true]. Count should be 3 but they think is 1.
            //
            // TODO: Try to use the indices that actually support counting values.
            values_count: _,
            is_empty: None,
            is_null: None,
        } => None,
    }
}

fn get_geo_polygon_checkers(
    index: &FieldIndex,
    geo_polygon: GeoPolygon,
    hw_acc: HwMeasurementAcc,
) -> Option<ConditionCheckerFn<'_>> {
    let polygon_wrapper = geo_polygon.convert();
    let hw_counter = hw_acc.get_counter_cell();
    match index {
        FieldIndex::GeoIndex(geo_index) => Some(Box::new(move |point_id: PointOffsetType| {
            geo_index.check_values_any(point_id, &hw_counter, |value| {
                polygon_wrapper.check_point(value)
            })
        })),
        FieldIndex::BoolIndex(_)
        | FieldIndex::DatetimeIndex(_)
        | FieldIndex::FloatIndex(_)
        | FieldIndex::FullTextIndex(_)
        | FieldIndex::IntIndex(_)
        | FieldIndex::IntMapIndex(_)
        | FieldIndex::KeywordIndex(_)
        | FieldIndex::UuidIndex(_)
        | FieldIndex::UuidMapIndex(_)
        | FieldIndex::NullIndex(_) => None,
    }
}

fn get_geo_radius_checkers(
    index: &FieldIndex,
    geo_radius: GeoRadius,
    hw_acc: HwMeasurementAcc,
) -> Option<ConditionCheckerFn<'_>> {
    let hw_counter = hw_acc.get_counter_cell();
    match index {
        FieldIndex::GeoIndex(geo_index) => Some(Box::new(move |point_id: PointOffsetType| {
            geo_index.check_values_any(point_id, &hw_counter, |value| geo_radius.check_point(value))
        })),
        FieldIndex::BoolIndex(_)
        | FieldIndex::DatetimeIndex(_)
        | FieldIndex::FloatIndex(_)
        | FieldIndex::FullTextIndex(_)
        | FieldIndex::IntIndex(_)
        | FieldIndex::IntMapIndex(_)
        | FieldIndex::KeywordIndex(_)
        | FieldIndex::UuidIndex(_)
        | FieldIndex::UuidMapIndex(_)
        | FieldIndex::NullIndex(_) => None,
    }
}

fn get_geo_bounding_box_checkers(
    index: &FieldIndex,
    geo_bounding_box: GeoBoundingBox,
    hw_acc: HwMeasurementAcc,
) -> Option<ConditionCheckerFn<'_>> {
    let hw_counter = hw_acc.get_counter_cell();
    match index {
        FieldIndex::GeoIndex(geo_index) => Some(Box::new(move |point_id: PointOffsetType| {
            geo_index.check_values_any(point_id, &hw_counter, |value| {
                geo_bounding_box.check_point(value)
            })
        })),
        FieldIndex::BoolIndex(_)
        | FieldIndex::DatetimeIndex(_)
        | FieldIndex::FloatIndex(_)
        | FieldIndex::FullTextIndex(_)
        | FieldIndex::IntIndex(_)
        | FieldIndex::IntMapIndex(_)
        | FieldIndex::KeywordIndex(_)
        | FieldIndex::UuidIndex(_)
        | FieldIndex::UuidMapIndex(_)
        | FieldIndex::NullIndex(_) => None,
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

pub(super) fn get_is_empty_indexes(
    indexes: &[FieldIndex],
) -> (Option<&NullIndex>, Option<&FieldIndex>) {
    let mut primary_null_index: Option<&NullIndex> = None;
    let mut fallback_index: Option<&FieldIndex> = None;

    for index in indexes {
        match index {
            FieldIndex::NullIndex(null_index) => {
                primary_null_index = Some(null_index);
            }
            _ => {
                fallback_index = Some(index);
            }
        }
    }

    (primary_null_index, fallback_index)
}

pub(super) fn get_null_index_is_empty_checker(
    null_index: &NullIndex,
    is_empty: bool,
) -> ConditionCheckerFn<'_> {
    Box::new(move |point_id: PointOffsetType| null_index.values_is_empty(point_id) == is_empty)
}

pub(super) fn get_fallback_is_empty_checker<'a>(
    index: &'a FieldIndex,
    is_empty: bool,
    fallback_checker: ConditionCheckerFn<'a>,
) -> ConditionCheckerFn<'a> {
    Box::new(move |point_id: PointOffsetType| {
        if index.values_is_empty(point_id) {
            // If value is empty in index, it can still be non-empty in payload
            fallback_checker(point_id) == is_empty
        } else {
            // Value IS in index, so we can trust the index
            // If `is_empty` is true, we should return false, because the value is not empty
            !is_empty
        }
    })
}

/// Get a checker that checks if the field is empty
///
/// * `index` - index to check
/// * `is_empty` - if the field should be empty
fn get_is_empty_checker(index: &FieldIndex, is_empty: bool) -> Option<ConditionCheckerFn<'_>> {
    match index {
        FieldIndex::NullIndex(null_index) => {
            Some(get_null_index_is_empty_checker(null_index, is_empty))
        }
        FieldIndex::IntIndex(_)
        | FieldIndex::DatetimeIndex(_)
        | FieldIndex::IntMapIndex(_)
        | FieldIndex::KeywordIndex(_)
        | FieldIndex::FloatIndex(_)
        | FieldIndex::GeoIndex(_)
        | FieldIndex::FullTextIndex(_)
        | FieldIndex::BoolIndex(_)
        | FieldIndex::UuidIndex(_)
        | FieldIndex::UuidMapIndex(_) => None,
    }
}

pub(super) fn get_is_null_checker(
    index: &FieldIndex,
    is_null: bool,
) -> Option<ConditionCheckerFn<'_>> {
    match index {
        FieldIndex::NullIndex(null_index) => Some(Box::new(move |point_id: PointOffsetType| {
            null_index.values_is_null(point_id) == is_null
        })),
        FieldIndex::IntIndex(_)
        | FieldIndex::DatetimeIndex(_)
        | FieldIndex::IntMapIndex(_)
        | FieldIndex::KeywordIndex(_)
        | FieldIndex::FloatIndex(_)
        | FieldIndex::GeoIndex(_)
        | FieldIndex::FullTextIndex(_)
        | FieldIndex::BoolIndex(_)
        | FieldIndex::UuidIndex(_)
        | FieldIndex::UuidMapIndex(_) => None,
    }
}
