use std::collections::HashSet;

use crate::id_tracker::IdTrackerSS;
use crate::index::field_index::FieldIndex;
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::index::query_optimization::optimizer::IndexesMap;
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::payload_storage::query_checker::{
    check_field_condition, check_is_empty_condition, check_is_null_condition,
};
use crate::types::{
    AnyVariants, Condition, FieldCondition, FloatPayloadType, GeoBoundingBox, GeoRadius, Match,
    MatchAny, MatchText, MatchValue, PointOffsetType, Range, ValueVariants,
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
                    .filter_map(|index| field_condition_index(index, field_condition))
                    .next()
            })
            .unwrap_or_else(|| {
                Box::new(move |point_id| {
                    payload_provider.with_payload(point_id, |payload| {
                        check_field_condition(field_condition, &payload)
                    })
                })
            }),
        // ToDo: It might be possible to make this condition faster by using index to check
        //       if there is any value. But if value if not found,
        //       it does not mean that there are no values in payload
        Condition::IsEmpty(is_empty) => Box::new(move |point_id| {
            payload_provider.with_payload(point_id, |payload| {
                check_is_empty_condition(is_empty, &payload)
            })
        }),
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
        Condition::Filter(_) => unreachable!(),
        Condition::Nested(_) => unreachable!(),
    }
}

pub fn field_condition_index<'a>(
    index: &'a FieldIndex,
    field_condition: &FieldCondition,
) -> Option<ConditionCheckerFn<'a>> {
    if let Some(checker) = field_condition
        .r#match
        .clone()
        .and_then(|cond| get_match_checkers(index, cond))
    {
        return Some(checker);
    }

    if let Some(checker) = field_condition
        .range
        .clone()
        .and_then(|cond| get_range_checkers(index, cond))
    {
        return Some(checker);
    }

    if let Some(checker) = field_condition
        .geo_radius
        .clone()
        .and_then(|cond| get_geo_radius_checkers(index, cond))
    {
        return Some(checker);
    }

    if let Some(checker) = field_condition
        .geo_bounding_box
        .clone()
        .and_then(|cond| get_geo_bounding_box_checkers(index, cond))
    {
        return Some(checker);
    }

    None
}

pub fn get_geo_radius_checkers(
    index: &FieldIndex,
    geo_radius: GeoRadius,
) -> Option<ConditionCheckerFn> {
    match index {
        FieldIndex::GeoIndex(geo_index) => Some(Box::new(move |point_id: PointOffsetType| {
            match geo_index.get_values(point_id) {
                None => false,
                Some(values) => values
                    .iter()
                    .any(|geo_point| geo_radius.check_point(geo_point.lon, geo_point.lat)),
            }
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
                    .any(|geo_point| geo_bounding_box.check_point(geo_point.lon, geo_point.lat)),
            }
        })),
        _ => None,
    }
}

pub fn get_range_checkers(index: &FieldIndex, range: Range) -> Option<ConditionCheckerFn> {
    match index {
        FieldIndex::IntIndex(num_index) => Some(Box::new(move |point_id: PointOffsetType| {
            match num_index.get_values(point_id) {
                None => false,
                Some(values) => values
                    .iter()
                    .copied()
                    .any(|i| range.check_range(i as FloatPayloadType)),
            }
        })),
        FieldIndex::FloatIndex(num_index) => Some(Box::new(move |point_id: PointOffsetType| {
            match num_index.get_values(point_id) {
                None => false,
                Some(values) => values.iter().copied().any(|i| range.check_range(i)),
            }
        })),
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
                    match index.get_values(point_id) {
                        None => false,
                        Some(values) => values.iter().any(|k| k == &keyword),
                    }
                }))
            }
            (ValueVariants::Integer(value), FieldIndex::IntMapIndex(index)) => {
                Some(Box::new(move |point_id: PointOffsetType| {
                    match index.get_values(point_id) {
                        None => false,
                        Some(values) => values.iter().any(|i| i == &value),
                    }
                }))
            }
            _ => None,
        },
        Match::Text(MatchText { text }) => match index {
            FieldIndex::FullTextIndex(full_text_index) => {
                let parsed_query = full_text_index.parse_query(&text);
                Some(Box::new(
                    move |point_id: PointOffsetType| match full_text_index.get_doc(point_id) {
                        None => false,
                        Some(doc) => parsed_query.check_match(doc),
                    },
                ))
            }
            _ => None,
        },
        Match::Any(MatchAny { any }) => match (any, index) {
            (AnyVariants::Keywords(list), FieldIndex::KeywordIndex(index)) => {
                Some(Box::new(move |point_id: PointOffsetType| {
                    match index.get_values(point_id) {
                        None => false,
                        Some(values) => values.iter().any(|k| list.contains(k)),
                    }
                }))
            }
            (AnyVariants::Integers(list), FieldIndex::IntMapIndex(index)) => {
                Some(Box::new(move |point_id: PointOffsetType| {
                    match index.get_values(point_id) {
                        None => false,
                        Some(values) => values.iter().any(|i| list.contains(i)),
                    }
                }))
            }
            _ => None,
        },
    }
}
