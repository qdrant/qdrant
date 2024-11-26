use std::collections::{HashMap, HashSet};

use common::types::PointOffsetType;
use match_converter::get_match_checkers;
use serde_json::Value;

use crate::index::field_index::FieldIndex;
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
    ) -> ConditionCheckerFn<'a> {
        let id_tracker = self.id_tracker.borrow();
        let field_indexes = &self.field_indexes;
        match condition {
            Condition::Field(field_condition) => field_indexes
                .get(&field_condition.key)
                .and_then(|indexes| {
                    indexes
                        .iter()
                        .find_map(|index| field_condition_index(index, field_condition))
                })
                .unwrap_or_else(|| {
                    Box::new(move |point_id| {
                        payload_provider.with_payload(point_id, |payload| {
                            check_field_condition(field_condition, &payload, field_indexes)
                        })
                    })
                }),
            // We can use index for `is_empty` condition effectively only when it is not empty.
            // If the index says it is "empty", we still need to check the payload.
            Condition::IsEmpty(is_empty) => {
                let first_field_index = field_indexes
                    .get(&is_empty.is_empty.key)
                    .and_then(|indexes| indexes.first());

                let fallback = Box::new(move |point_id| {
                    payload_provider.with_payload(point_id, |payload| {
                        check_is_empty_condition(is_empty, &payload)
                    })
                });

                match first_field_index {
                    Some(index) => get_is_empty_checker(index, fallback),
                    None => fallback,
                }
            }

            Condition::IsNull(is_null) => Box::new(move |point_id| {
                payload_provider.with_payload(point_id, |payload| {
                    check_is_null_condition(is_null, &payload)
                })
            }),
            // ToDo: It might be possible to make this condition faster by using `VisitedPool` instead of HashSet
            Condition::HasId(has_id) => {
                let segment_ids: HashSet<_> = has_id
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

                Box::new(move |point_id| {
                    payload_provider.with_payload(point_id, |payload| {
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
                                ) {
                                    // If at least one nested object matches, return true
                                    return true;
                                }
                            }
                        }
                        false
                    })
                })
            }
            Condition::CustomIdChecker(cond) => {
                let segment_ids: HashSet<_> = id_tracker
                    .iter_external()
                    .filter(|&point_id| cond.check(point_id))
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
) -> Option<ConditionCheckerFn<'a>> {
    match field_condition {
        FieldCondition {
            r#match: Some(cond_match),
            ..
        } => get_match_checkers(index, cond_match.clone()),

        FieldCondition {
            range: Some(cond), ..
        } => get_range_checkers(index, cond.clone()),

        FieldCondition {
            geo_radius: Some(geo_radius),
            ..
        } => get_geo_radius_checkers(index, geo_radius.clone()),

        FieldCondition {
            geo_bounding_box: Some(geo_bounding_box),
            ..
        } => get_geo_bounding_box_checkers(index, geo_bounding_box.clone()),

        FieldCondition {
            geo_polygon: Some(geo_polygon),
            ..
        } => get_geo_polygon_checkers(index, geo_polygon.clone()),

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
        } => None,
    }
}

pub fn get_geo_polygon_checkers(
    index: &FieldIndex,
    geo_polygon: GeoPolygon,
) -> Option<ConditionCheckerFn> {
    let polygon_wrapper = geo_polygon.convert();
    match index {
        FieldIndex::GeoIndex(geo_index) => Some(Box::new(move |point_id: PointOffsetType| {
            geo_index.check_values_any(point_id, |value| polygon_wrapper.check_point(value))
        })),
        FieldIndex::BoolIndex(_)
        | FieldIndex::DatetimeIndex(_)
        | FieldIndex::FloatIndex(_)
        | FieldIndex::FullTextIndex(_)
        | FieldIndex::IntIndex(_)
        | FieldIndex::IntMapIndex(_)
        | FieldIndex::KeywordIndex(_)
        | FieldIndex::UuidIndex(_)
        | FieldIndex::UuidMapIndex(_) => None,
    }
}

pub fn get_geo_radius_checkers(
    index: &FieldIndex,
    geo_radius: GeoRadius,
) -> Option<ConditionCheckerFn> {
    match index {
        FieldIndex::GeoIndex(geo_index) => Some(Box::new(move |point_id: PointOffsetType| {
            geo_index.check_values_any(point_id, |value| geo_radius.check_point(value))
        })),
        FieldIndex::BoolIndex(_)
        | FieldIndex::DatetimeIndex(_)
        | FieldIndex::FloatIndex(_)
        | FieldIndex::FullTextIndex(_)
        | FieldIndex::IntIndex(_)
        | FieldIndex::IntMapIndex(_)
        | FieldIndex::KeywordIndex(_)
        | FieldIndex::UuidIndex(_)
        | FieldIndex::UuidMapIndex(_) => None,
    }
}

pub fn get_geo_bounding_box_checkers(
    index: &FieldIndex,
    geo_bounding_box: GeoBoundingBox,
) -> Option<ConditionCheckerFn> {
    match index {
        FieldIndex::GeoIndex(geo_index) => Some(Box::new(move |point_id: PointOffsetType| {
            geo_index.check_values_any(point_id, |value| geo_bounding_box.check_point(value))
        })),
        FieldIndex::BoolIndex(_)
        | FieldIndex::DatetimeIndex(_)
        | FieldIndex::FloatIndex(_)
        | FieldIndex::FullTextIndex(_)
        | FieldIndex::IntIndex(_)
        | FieldIndex::IntMapIndex(_)
        | FieldIndex::KeywordIndex(_)
        | FieldIndex::UuidIndex(_)
        | FieldIndex::UuidMapIndex(_) => None,
    }
}

pub fn get_range_checkers(index: &FieldIndex, range: RangeInterface) -> Option<ConditionCheckerFn> {
    match range {
        RangeInterface::Float(range) => get_float_range_checkers(index, range),
        RangeInterface::DateTime(range) => get_datetime_range_checkers(index, range),
    }
}

pub fn get_float_range_checkers(
    index: &FieldIndex,
    range: Range<FloatPayloadType>,
) -> Option<ConditionCheckerFn> {
    match index {
        FieldIndex::IntIndex(num_index) => {
            let range = range.map(|f| f as IntPayloadType);
            Some(Box::new(move |point_id: PointOffsetType| {
                num_index.check_values_any(point_id, |value| range.check_range(*value))
            }))
        }
        FieldIndex::FloatIndex(num_index) => Some(Box::new(move |point_id: PointOffsetType| {
            num_index.check_values_any(point_id, |value| range.check_range(*value))
        })),
        FieldIndex::BoolIndex(_)
        | FieldIndex::DatetimeIndex(_)
        | FieldIndex::FullTextIndex(_)
        | FieldIndex::GeoIndex(_)
        | FieldIndex::IntMapIndex(_)
        | FieldIndex::KeywordIndex(_)
        | FieldIndex::UuidIndex(_)
        | FieldIndex::UuidMapIndex(_) => None,
    }
}

pub fn get_datetime_range_checkers(
    index: &FieldIndex,
    range: Range<DateTimePayloadType>,
) -> Option<ConditionCheckerFn> {
    match index {
        FieldIndex::DatetimeIndex(num_index) => {
            let range = range.map(|dt| dt.timestamp());
            Some(Box::new(move |point_id: PointOffsetType| {
                num_index.check_values_any(point_id, |value| range.check_range(*value))
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
        | FieldIndex::UuidMapIndex(_) => None,
    }
}

/// Get a checker that checks if the field is empty
///
/// * `index` - index to check first
/// * `fallback` - Check if it is empty using plain payload
#[inline]
fn get_is_empty_checker<'a>(
    index: &'a FieldIndex,
    fallback: ConditionCheckerFn<'a>,
) -> ConditionCheckerFn<'a> {
    Box::new(move |point_id: PointOffsetType| {
        // Counting on the short-circuit of the `&&` operator
        // Only check the fallback if the index seems to be empty
        index.values_is_empty(point_id) && fallback(point_id)
    })
}
