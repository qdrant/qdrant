use std::collections::HashMap;

use itertools::Itertools;

use crate::common::utils::JsonPathPayload;
use crate::index::field_index::FieldIndex;
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::index::query_optimization::optimizer::IndexesMap;
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::payload_storage::nested_query_checker::{
    check_nested_is_empty_condition, check_nested_is_null_condition, nested_check_field_condition,
};
use crate::types::{
    AnyVariants, Condition, FieldCondition, FloatPayloadType, GeoBoundingBox, GeoRadius, Match,
    MatchAny, MatchText, MatchValue, PointOffsetType, Range, ValueVariants,
};

/// Payload element index
pub type ElemIndex = usize;

/// Given a point_id, returns the list of indices in the payload matching the condition
pub type NestedMatchingIndicesFn<'a> = Box<dyn Fn(PointOffsetType) -> Vec<ElemIndex> + 'a>;

/// Merge several nested condition results into a single regular condition checker
///
/// return a single condition checker that will return true if all nested condition checkers for the point_id
pub fn merge_nested_matching_indices(
    nested_checkers: Vec<NestedMatchingIndicesFn>,
) -> ConditionCheckerFn {
    Box::new(move |point_id: PointOffsetType| {
        let matches = find_indices_matching_all_conditions(point_id, &nested_checkers);
        // if any of the nested path is matching for each nested condition
        // then the point_id matches and a matching `ConditionCheckerFn can be returned
        !matches.is_empty()
    })
}

/// Apply `point_id` to `nested_checkers` and return the list of indices in the payload matching all conditions
pub fn find_indices_matching_all_conditions(
    point_id: PointOffsetType,
    nested_checkers: &Vec<NestedMatchingIndicesFn>,
) -> Vec<ElemIndex> {
    let condition_len = nested_checkers.len();
    let matches: HashMap<ElemIndex, usize> =
        nested_checkers
            .iter()
            .flat_map(|f| f(point_id))
            .fold(HashMap::new(), |mut acc, inner| {
                *acc.entry(inner).or_insert(0) += 1;
                acc
            });
    // gather all indices that have matched all musts conditions
    matches
        .iter()
        .filter(|(_, &count)| count == condition_len)
        .map(|(index, _)| *index)
        .collect()
}

pub fn nested_conditions_converter<'a>(
    conditions: &'a [Condition],
    field_indexes: &'a IndexesMap,
    payload_provider: PayloadProvider,
    nested_path: JsonPathPayload,
) -> Vec<NestedMatchingIndicesFn<'a>> {
    conditions
        .iter()
        .map(|condition| {
            nested_condition_converter(
                condition,
                field_indexes,
                payload_provider.clone(),
                nested_path.clone(),
            )
        })
        .collect()
}

pub fn nested_condition_converter<'a>(
    condition: &'a Condition,
    field_indexes: &'a IndexesMap,
    payload_provider: PayloadProvider,
    nested_path: JsonPathPayload,
) -> NestedMatchingIndicesFn<'a> {
    match condition {
        Condition::Field(field_condition) => {
            let full_path = nested_path.extend(&field_condition.key);
            field_indexes
                .get(&full_path.path)
                .and_then(|indexes| {
                    indexes
                        .iter()
                        .find_map(|index| nested_field_condition_index(index, field_condition))
                })
                .unwrap_or_else(|| {
                    Box::new(move |point_id| {
                        payload_provider.with_payload(point_id, |payload| {
                            nested_check_field_condition(field_condition, &payload, &nested_path)
                        })
                    })
                })
        }
        Condition::IsEmpty(is_empty) => Box::new(move |point_id| {
            payload_provider.with_payload(point_id, |payload| {
                check_nested_is_empty_condition(&nested_path, is_empty, &payload)
            })
        }),
        Condition::IsNull(is_null) => Box::new(move |point_id| {
            payload_provider.with_payload(point_id, |payload| {
                check_nested_is_null_condition(&nested_path, is_null, &payload)
            })
        }),
        Condition::HasId(_) => {
            // No support for has_id in nested queries
            Box::new(move |_| vec![])
        }
        Condition::Nested(nested) => {
            Box::new(move |point_id| {
                // TODO should & must_not (does it make sense?)
                match &nested.filter().must {
                    None => vec![],
                    Some(musts_conditions) => {
                        let full_path = nested_path.extend(&nested.array_key());
                        let matching_indices = nested_conditions_converter(
                            musts_conditions,
                            field_indexes,
                            payload_provider.clone(),
                            full_path,
                        );
                        find_indices_matching_all_conditions(point_id, &matching_indices)
                    }
                }
            })
        }
        Condition::Filter(_) => unreachable!(),
    }
}

/// Returns a checker function that will return the index of the payload elements
/// matching the condition for the given point_id
pub fn nested_field_condition_index<'a>(
    index: &'a FieldIndex,
    field_condition: &FieldCondition,
) -> Option<NestedMatchingIndicesFn<'a>> {
    if let Some(checker) = field_condition
        .r#match
        .clone()
        .and_then(|cond| get_nested_match_checkers(index, cond))
    {
        return Some(checker);
    }

    if let Some(checker) = field_condition
        .range
        .clone()
        .and_then(|cond| get_nested_range_checkers(index, cond))
    {
        return Some(checker);
    }

    if let Some(checker) = field_condition
        .geo_radius
        .clone()
        .and_then(|cond| get_nested_geo_radius_checkers(index, cond))
    {
        return Some(checker);
    }

    if let Some(checker) = field_condition
        .geo_bounding_box
        .clone()
        .and_then(|cond| get_nested_geo_bounding_box_checkers(index, cond))
    {
        return Some(checker);
    }

    None
}

pub fn get_nested_geo_radius_checkers(
    index: &FieldIndex,
    geo_radius: GeoRadius,
) -> Option<NestedMatchingIndicesFn> {
    match index {
        FieldIndex::GeoIndex(geo_index) => Some(Box::new(move |point_id: PointOffsetType| {
            match geo_index.get_values(point_id) {
                None => vec![],
                Some(values) => values
                    .iter()
                    .positions(|geo_point| geo_radius.check_point(geo_point.lon, geo_point.lat))
                    .collect(),
            }
        })),
        _ => None,
    }
}

pub fn get_nested_geo_bounding_box_checkers(
    index: &FieldIndex,
    geo_bounding_box: GeoBoundingBox,
) -> Option<NestedMatchingIndicesFn> {
    match index {
        FieldIndex::GeoIndex(geo_index) => Some(Box::new(move |point_id: PointOffsetType| {
            match geo_index.get_values(point_id) {
                None => vec![],
                Some(values) => values
                    .iter()
                    .positions(|geo_point| {
                        geo_bounding_box.check_point(geo_point.lon, geo_point.lat)
                    })
                    .collect(),
            }
        })),
        _ => None,
    }
}

pub fn get_nested_range_checkers(
    index: &FieldIndex,
    range: Range,
) -> Option<NestedMatchingIndicesFn> {
    match index {
        FieldIndex::IntIndex(num_index) => Some(Box::new(move |point_id: PointOffsetType| {
            match num_index.get_values(point_id) {
                None => vec![],
                Some(values) => values
                    .iter()
                    .copied()
                    .positions(|i| range.check_range(i as FloatPayloadType))
                    .collect(),
            }
        })),
        FieldIndex::FloatIndex(num_index) => Some(Box::new(move |point_id: PointOffsetType| {
            match num_index.get_values(point_id) {
                None => vec![],
                Some(values) => values
                    .iter()
                    .copied()
                    .positions(|i| range.check_range(i))
                    .collect(),
            }
        })),
        _ => None,
    }
}

pub fn get_nested_match_checkers(
    index: &FieldIndex,
    cond_match: Match,
) -> Option<NestedMatchingIndicesFn> {
    match cond_match {
        Match::Value(MatchValue {
            value: value_variant,
        }) => match (value_variant, index) {
            (ValueVariants::Keyword(keyword), FieldIndex::KeywordIndex(index)) => {
                Some(Box::new(move |point_id: PointOffsetType| {
                    match index.get_values(point_id) {
                        None => vec![],
                        Some(values) => values.iter().positions(|k| k == &keyword).collect(),
                    }
                }))
            }
            (ValueVariants::Integer(value), FieldIndex::IntMapIndex(index)) => {
                Some(Box::new(move |point_id: PointOffsetType| {
                    match index.get_values(point_id) {
                        None => vec![],
                        Some(values) => values.iter().positions(|i| i == &value).collect(),
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
                        None => vec![],
                        Some(doc) => {
                            let res = parsed_query.check_match(doc);
                            // Not sure it is entirely correct
                            if res {
                                vec![0]
                            } else {
                                vec![]
                            }
                        }
                    },
                ))
            }
            _ => None,
        },
        Match::Any(MatchAny { any }) => match (any, index) {
            (AnyVariants::Keywords(list), FieldIndex::KeywordIndex(index)) => {
                Some(Box::new(move |point_id: PointOffsetType| {
                    match index.get_values(point_id) {
                        None => vec![],
                        Some(values) => values.iter().positions(|k| list.contains(k)).collect(),
                    }
                }))
            }
            (AnyVariants::Integers(list), FieldIndex::IntMapIndex(index)) => {
                Some(Box::new(move |point_id: PointOffsetType| {
                    match index.get_values(point_id) {
                        None => vec![],
                        Some(values) => values.iter().positions(|i| list.contains(i)).collect(),
                    }
                }))
            }
            _ => None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_matching_merge_nested_matching_indices() {
        let matching_indices_fn: Vec<NestedMatchingIndicesFn> = vec![
            Box::new(|_point_id: PointOffsetType| vec![]),
            Box::new(|_point_id: PointOffsetType| vec![]),
            Box::new(|_point_id: PointOffsetType| vec![]),
        ];

        let merged = merge_nested_matching_indices(matching_indices_fn);
        // none of the conditions are matching anything
        let result: bool = merged(0);
        assert!(!result);
    }

    #[test]
    fn single_matching_merge_merge_nested_matching_indices() {
        let matching_indices_fn: Vec<NestedMatchingIndicesFn> = vec![
            Box::new(|_point_id: PointOffsetType| vec![0]),
            Box::new(|_point_id: PointOffsetType| vec![0]),
            Box::new(|_point_id: PointOffsetType| vec![0]),
        ];

        let merged = merge_nested_matching_indices(matching_indices_fn);
        let result: bool = merged(0);
        assert!(result);
    }

    #[test]
    fn single_non_matching_merge_nested_matching_indices() {
        let matching_indices_fn: Vec<NestedMatchingIndicesFn> = vec![
            Box::new(|_point_id: PointOffsetType| vec![0]),
            Box::new(|_point_id: PointOffsetType| vec![0]),
            Box::new(|_point_id: PointOffsetType| vec![1]),
        ];
        let merged = merge_nested_matching_indices(matching_indices_fn);
        // does not because all the checkers are not matching the same path
        let result: bool = merged(0);
        assert!(!result);
    }

    #[test]
    fn many_matching_merge_nested_matching_indices() {
        let matching_indices_fn: Vec<NestedMatchingIndicesFn> = vec![
            Box::new(|_point_id: PointOffsetType| vec![0, 1]),
            Box::new(|_point_id: PointOffsetType| vec![0, 1]),
            Box::new(|_point_id: PointOffsetType| vec![0]),
        ];

        let merged = merge_nested_matching_indices(matching_indices_fn);
        // still matching because of the path '0' matches all conditions
        let result: bool = merged(0);
        assert!(result);
    }
}
