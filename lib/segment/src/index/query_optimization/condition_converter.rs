use std::collections::HashSet;

use common::types::PointOffsetType;
use serde_json::Value;

use crate::common::utils::IndexesMap;
use crate::id_tracker::IdTrackerSS;
use crate::index::field_index::FieldIndex;
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::payload_storage::query_checker::{
    check_field_condition, check_is_empty_condition, check_is_null_condition, check_payload,
    select_nested_indexes,
};
use crate::types::{
    AnyVariants, Condition, DateTimePayloadType, FieldCondition, FloatPayloadType, GeoBoundingBox,
    GeoPolygon, GeoRadius, IntPayloadType, Match, MatchAny, MatchExcept, MatchText, MatchValue,
    OwnedPayloadRef, PayloadContainer, Range, RangeInterface, ValueVariants,
};

pub fn condition_converter<'a>(
    condition: &'a Condition,
    field_indexes: &'a IndexesMap,
    payload_provider: PayloadProvider,
    id_tracker: &IdTrackerSS,
) -> ConditionCheckerFn<'a> {
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
                    let field_values = payload.get_value(&nested_path).values();

                    for value in field_values {
                        if let Value::Object(object) = value {
                            let get_payload = || OwnedPayloadRef::from(object);
                            if check_payload(
                                Box::new(get_payload),
                                // None because has_id in nested is not supported. So retrieving
                                // IDs through the tracker would always return None.
                                None,
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
        Condition::Filter(_) => unreachable!(),
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
            values_count: _, // No applicable index for values_count
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
            geo_index.get_values(point_id).map_or(false, |values| {
                values
                    .iter()
                    .any(|geo_point| polygon_wrapper.check_point(geo_point))
            })
        })),
        _ => None,
    }
}

pub fn get_geo_radius_checkers(
    index: &FieldIndex,
    geo_radius: GeoRadius,
) -> Option<ConditionCheckerFn> {
    match index {
        FieldIndex::GeoIndex(geo_index) => Some(Box::new(move |point_id: PointOffsetType| {
            geo_index.get_values(point_id).map_or(false, |values| {
                values
                    .iter()
                    .any(|geo_point| geo_radius.check_point(geo_point))
            })
        })),
        _ => None,
    }
}

pub fn get_geo_bounding_box_checkers(
    index: &FieldIndex,
    geo_bounding_box: GeoBoundingBox,
) -> Option<ConditionCheckerFn> {
    match index {
        FieldIndex::GeoIndex(geo_index) => Some(Box::new(move |point_id: PointOffsetType| {
            match geo_index.get_values(point_id) {
                None => false,
                Some(values) => values
                    .iter()
                    .any(|geo_point| geo_bounding_box.check_point(geo_point)),
            }
        })),
        _ => None,
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
                num_index
                    .get_values(point_id)
                    .is_some_and(|values| values.iter().copied().any(|i| range.check_range(i)))
            }))
        }
        FieldIndex::FloatIndex(num_index) => Some(Box::new(move |point_id: PointOffsetType| {
            num_index
                .get_values(point_id)
                .is_some_and(|values| values.iter().copied().any(|f| range.check_range(f)))
        })),
        _ => None,
    }
}

pub fn get_datetime_range_checkers(
    index: &FieldIndex,
    range: Range<DateTimePayloadType>,
) -> Option<ConditionCheckerFn> {
    match index {
        FieldIndex::DatetimeIndex(num_index) => {
            let range = range.map(|ts| ts.timestamp_micros());
            Some(Box::new(move |point_id: PointOffsetType| {
                num_index
                    .get_values(point_id)
                    .is_some_and(|values| values.iter().copied().any(|i| range.check_range(i)))
            }))
        }
        _ => None,
    }
}

pub fn get_match_checkers(index: &FieldIndex, cond_match: Match) -> Option<ConditionCheckerFn> {
    match cond_match {
        Match::Value(MatchValue {
            value: value_variant,
        }) => match (value_variant, index) {
            (ValueVariants::Keyword(keyword), FieldIndex::KeywordIndex(index)) => {
                Some(Box::new(move |point_id: PointOffsetType| {
                    index
                        .get_values(point_id)
                        .map_or(false, |values| values.iter().any(|k| k == &keyword))
                }))
            }
            (ValueVariants::Integer(value), FieldIndex::IntMapIndex(index)) => {
                Some(Box::new(move |point_id: PointOffsetType| {
                    index
                        .get_values(point_id)
                        .map_or(false, |values| values.iter().any(|i| i == &value))
                }))
            }
            (ValueVariants::Bool(is_true), FieldIndex::BinaryIndex(index)) => {
                Some(Box::new(move |point_id: PointOffsetType| {
                    if is_true {
                        index.values_has_true(point_id)
                    } else {
                        index.values_has_false(point_id)
                    }
                }))
            }
            _ => None,
        },
        Match::Text(MatchText { text }) => match index {
            FieldIndex::FullTextIndex(full_text_index) => {
                let parsed_query = full_text_index.parse_query(&text);
                Some(Box::new(move |point_id: PointOffsetType| {
                    full_text_index
                        .get_doc(point_id)
                        .map_or(false, |doc| parsed_query.check_match(doc))
                }))
            }
            _ => None,
        },
        Match::Any(MatchAny { any }) => match (any, index) {
            (AnyVariants::Keywords(list), FieldIndex::KeywordIndex(index)) => {
                Some(Box::new(move |point_id: PointOffsetType| {
                    index.get_values(point_id).map_or(false, |values| {
                        values
                            .iter()
                            .any(|k| list.iter().any(|s| s.as_str() == k.as_ref()))
                    })
                }))
            }
            (AnyVariants::Integers(list), FieldIndex::IntMapIndex(index)) => {
                Some(Box::new(move |point_id: PointOffsetType| {
                    index
                        .get_values(point_id)
                        .map_or(false, |values| values.iter().any(|i| list.contains(i)))
                }))
            }
            _ => None,
        },
        Match::Except(MatchExcept { except }) => match (except, index) {
            (AnyVariants::Keywords(list), FieldIndex::KeywordIndex(index)) => {
                Some(Box::new(move |point_id: PointOffsetType| {
                    index.get_values(point_id).map_or(false, |values| {
                        values
                            .iter()
                            .any(|k| !list.iter().any(|s| s.as_str() == k.as_ref()))
                    })
                }))
            }
            (AnyVariants::Integers(list), FieldIndex::IntMapIndex(index)) => {
                Some(Box::new(move |point_id: PointOffsetType| {
                    index
                        .get_values(point_id)
                        .map_or(false, |values| values.iter().any(|i| !list.contains(i)))
                }))
            }
            (_, index) => Some(Box::new(|point_id: PointOffsetType| {
                // If there is any other value of any other index, then it's a match
                index.values_count(point_id) > 0
            })),
        },
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
