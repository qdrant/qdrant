use std::collections::HashMap;

use ahash::AHashSet;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use match_converter::get_match_checkers;
use ordered_float::OrderedFloat;
use serde_json::Value;

use crate::index::field_index::FieldIndex;
use crate::index::field_index::null_index::MutableNullIndex;
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::payload_storage::query_checker::{
    check_field_condition, check_is_empty_condition, check_is_null_condition, check_payload,
    select_nested_indexes,
};
use crate::types::{
    Condition, DateTimePayloadType, FieldCondition, FloatPayloadType, GeoBoundingBox, GeoPolygon,
    GeoRadius, IntPayloadType, OwnedPayloadRef, PayloadContainer, Range, RangeInterface,
};
use crate::vector_storage::VectorStorage;

mod match_converter;

impl StructPayloadIndex {
    pub fn condition_converter<'a>(
        &'a self,
        condition: &'a Condition,
        payload_provider: PayloadProvider,
        hw_counter: &HardwareCounterCell,
    ) -> ConditionCheckerFn<'a> {
        let id_tracker = self.id_tracker.borrow();
        let field_indexes = &self.field_indexes;
        match condition {
            Condition::Field(field_condition) => field_indexes
                .get(&field_condition.key)
                .and_then(|indexes| {
                    indexes.iter().find_map(move |index| {
                        let hw_acc = hw_counter.new_accumulator();
                        field_condition_index(index, field_condition, hw_acc)
                    })
                })
                .unwrap_or_else(|| {
                    let hw = hw_counter.fork();
                    Box::new(move |point_id| {
                        payload_provider.with_payload(
                            point_id,
                            |payload| {
                                check_field_condition(field_condition, &payload, field_indexes, &hw)
                            },
                            &hw,
                        )
                    })
                }),
            // Use dedicated null index for `is_empty` check if it is available
            // Otherwise we might use another index just to check if a field is not empty, if we
            // don't have an indexed value we must still check the payload to see if its empty
            Condition::IsEmpty(is_empty) => {
                let field_indexes = field_indexes.get(&is_empty.is_empty.key);

                let (primary_null_index, fallback_index) = field_indexes
                    .map(|field_indexes| get_is_empty_indexes(field_indexes))
                    .unwrap_or((None, None));

                if let Some(null_index) = primary_null_index {
                    get_null_index_is_empty_checker(null_index, true)
                } else {
                    // Fallback to reading payload, in case we don't yet have null-index
                    let hw = hw_counter.fork();
                    let fallback = Box::new(move |point_id| {
                        payload_provider.with_payload(
                            point_id,
                            |payload| check_is_empty_condition(is_empty, &payload),
                            &hw,
                        )
                    });

                    if let Some(fallback_index) = fallback_index {
                        get_fallback_is_empty_checker(fallback_index, true, fallback)
                    } else {
                        fallback
                    }
                }
            }

            Condition::IsNull(is_null) => {
                let field_indexes = field_indexes.get(&is_null.is_null.key);

                let is_null_checker = field_indexes.and_then(|field_indexes| {
                    field_indexes
                        .iter()
                        .find_map(|index| get_is_null_checker(index, true))
                });

                if let Some(checker) = is_null_checker {
                    checker
                } else {
                    // Fallback to reading payload
                    let hw = hw_counter.fork();
                    Box::new(move |point_id| {
                        payload_provider.with_payload(
                            point_id,
                            |payload| check_is_null_condition(is_null, &payload),
                            &hw,
                        )
                    })
                }
            }
            // ToDo: It might be possible to make this condition faster by using `VisitedPool` instead of HashSet
            Condition::HasId(has_id) => {
                let segment_ids: AHashSet<_> = has_id
                    .has_id
                    .iter()
                    .filter_map(|external_id| id_tracker.internal_id(*external_id))
                    .collect();
                Box::new(move |point_id| segment_ids.contains(&point_id))
            }
            Condition::HasVector(has_vector) => {
                if let Some(vector_storage) =
                    self.vector_storages.get(&has_vector.has_vector).cloned()
                {
                    Box::new(move |point_id| !vector_storage.borrow().is_deleted_vector(point_id))
                } else {
                    Box::new(|_point_id| false)
                }
            }
            Condition::Nested(nested) => {
                // Select indexes for nested fields. Trim nested part from key, so
                // that nested condition can address fields without nested part.

                // Example:
                // Index for field `nested.field` will be stored under key `nested.field`
                // And we have a query:
                // {
                //   "nested": {
                //     "path": "nested",
                //     "filter": {
                //         ...
                //         "match": {"key": "field", "value": "value"}
                //     }
                //   }

                // In this case we want to use `nested.field`, but we only have `field` in query.
                // Therefore we need to trim `nested` part from key. So that query executor
                // can address proper index for nested field.
                let nested_path = nested.array_key();

                let nested_indexes = select_nested_indexes(&nested_path, field_indexes);

                let hw = hw_counter.fork();
                Box::new(move |point_id| {
                    payload_provider.with_payload(
                        point_id,
                        |payload| {
                            let field_values = payload.get_value(&nested_path);

                            for value in field_values {
                                if let Value::Object(object) = value {
                                    let get_payload = || OwnedPayloadRef::from(object);
                                    if check_payload(
                                        Box::new(get_payload),
                                        // None because has_id in nested is not supported. So retrieving
                                        // IDs through the tracker would always return None.
                                        None,
                                        // Same as above, nested conditions don't support has_vector.
                                        &HashMap::new(),
                                        &nested.nested.filter,
                                        point_id,
                                        &nested_indexes,
                                        &hw,
                                    ) {
                                        // If at least one nested object matches, return true
                                        return true;
                                    }
                                }
                            }
                            false
                        },
                        &hw,
                    )
                })
            }
            Condition::CustomIdChecker(cond) => {
                let segment_ids: AHashSet<_> = id_tracker
                    .iter_external()
                    .filter(|&point_id| cond.0.check(point_id))
                    .filter_map(|external_id| id_tracker.internal_id(external_id))
                    .collect();

                Box::new(move |internal_id| segment_ids.contains(&internal_id))
            }
            Condition::Filter(_) => unreachable!(),
        }
    }
}

pub fn field_condition_index<'a>(
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
        } => get_range_checkers(index, cond.clone(), hw_acc),

        FieldCondition {
            geo_radius: Some(geo_radius),
            ..
        } => get_geo_radius_checkers(index, geo_radius.clone(), hw_acc),

        FieldCondition {
            geo_bounding_box: Some(geo_bounding_box),
            ..
        } => get_geo_bounding_box_checkers(index, geo_bounding_box.clone(), hw_acc),

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

pub fn get_geo_polygon_checkers(
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

pub fn get_geo_radius_checkers(
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

pub fn get_geo_bounding_box_checkers(
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

pub fn get_range_checkers(
    index: &FieldIndex,
    range: RangeInterface,
    hw_acc: HwMeasurementAcc,
) -> Option<ConditionCheckerFn<'_>> {
    match range {
        RangeInterface::Float(range) => get_float_range_checkers(index, range, hw_acc),
        RangeInterface::DateTime(range) => get_datetime_range_checkers(index, range, hw_acc),
    }
}

pub fn get_float_range_checkers(
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

pub fn get_datetime_range_checkers(
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

fn get_is_empty_indexes(
    indexes: &[FieldIndex],
) -> (Option<&MutableNullIndex>, Option<&FieldIndex>) {
    let mut primary_null_index: Option<&MutableNullIndex> = None;
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

fn get_null_index_is_empty_checker(
    null_index: &MutableNullIndex,
    is_empty: bool,
) -> ConditionCheckerFn<'_> {
    Box::new(move |point_id: PointOffsetType| null_index.values_is_empty(point_id) == is_empty)
}

fn get_fallback_is_empty_checker<'a>(
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

fn get_is_null_checker(index: &FieldIndex, is_null: bool) -> Option<ConditionCheckerFn<'_>> {
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
