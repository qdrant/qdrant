use std::collections::{HashMap, HashSet};

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
    MatchAny, MatchText, MatchValue, NestedContainer, PointOffsetType, Range, ValueVariants,
};

/// Payload element index
pub type ElemIndex = usize;

/// Given a point_id, returns the list of nested indices matching the condition and the total number of nested elements in the payload
pub type NestedMatchingIndicesFn<'a> = Box<dyn Fn(PointOffsetType) -> (Vec<ElemIndex>, usize) + 'a>;

/// Merge several nested condition results into a single regular condition checker
///
/// return a single condition checker that will return true if all nested condition checkers for the point_id
pub fn merge_nested_matching_indices(
    nested_checkers: Vec<NestedMatchingIndicesFn>,
    nested_negate: bool,
) -> ConditionCheckerFn {
    Box::new(move |point_id: PointOffsetType| {
        if nested_negate {
            let not_matching = find_indices_matching_none_conditions(point_id, &nested_checkers).0;
            // if they are no nested path not matching ANY nested conditions
            not_matching.is_empty()
        } else {
            let matches = find_indices_matching_all_conditions(point_id, &nested_checkers).0;
            // if any of the nested path is matching for ALL nested condition
            !matches.is_empty()
        }
    })
}

/// Apply `point_id` to `nested_checkers` and return the list of indices in the payload matching all conditions
pub fn find_indices_matching_all_conditions(
    point_id: PointOffsetType,
    nested_checkers: &Vec<NestedMatchingIndicesFn>,
) -> (Vec<ElemIndex>, usize) {
    let condition_len = nested_checkers.len();
    let mut len = 0;
    let matches: HashMap<ElemIndex, usize> = nested_checkers
        .iter()
        .flat_map(|f| {
            let (matches, len_) = f(point_id);
            len = len_; // all len are the same for all
            matches
        })
        .fold(HashMap::new(), |mut acc, inner| {
            *acc.entry(inner).or_insert(0) += 1;
            acc
        });
    // gather all indices that have matched all conditions
    let match_all = matches
        .iter()
        .filter(|(_, &count)| count == condition_len)
        .map(|(index, _)| *index)
        .collect();
    (match_all, len)
}

/// Apply `point_id` to `nested_checkers` and return the list of indices in the payload matching none of the conditions
pub fn find_indices_matching_none_conditions(
    point_id: PointOffsetType,
    nested_checkers: &[NestedMatchingIndicesFn],
) -> (Vec<ElemIndex>, usize) {
    let mut not_matching = vec![];
    let mut len = 0;
    // find all indices that have matched at least one condition
    let match_at_least_once: HashSet<ElemIndex> = nested_checkers
        .iter()
        .flat_map(|f| {
            let (matches, len_) = f(point_id);
            len = len_; // all len are the same for all
            matches
        })
        .fold(HashSet::new(), |mut acc, inner| {
            acc.insert(inner);
            acc
        });

    if len == 0 {
        return (not_matching, len);
    }

    for i in 1..len {
        // index is 0 based, so we need to substract 1 from len based index
        let index = i - 1;
        if !match_at_least_once.contains(&index) {
            not_matching.push(index);
        }
    }
    (not_matching, len)
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
            Box::new(move |_| (vec![], 0))
        }
        Condition::Nested(nested) => {
            Box::new(move |point_id| {
                // `should` is not supported in nested queries as it corresponds to a non nested `should`
                let must_matching = check_nested_must(
                    point_id,
                    nested,
                    field_indexes,
                    payload_provider.clone(),
                    nested_path.clone(),
                );
                let must_not_matching = check_nested_must_not(
                    point_id,
                    nested,
                    field_indexes,
                    payload_provider.clone(),
                    nested_path.clone(),
                );

                // combine results of `must` and `must_not`
                match (must_matching, must_not_matching) {
                    (None, None) => (vec![], 0),
                    (Some(must_matching), None) => must_matching,
                    (None, Some(must_not_matching)) => must_not_matching,
                    (Some((must_matching, len_1)), Some((must_not_matching, len_2))) => {
                        debug_assert_eq!(len_1, len_2);
                        // return intersection of `must_matching` and `must_not_matching`
                        let intersection = must_matching
                            .into_iter()
                            .filter(|index| must_not_matching.contains(index))
                            .collect();
                        (intersection, len_1)
                    }
                }
            })
        }
        Condition::Filter(_) => unreachable!(),
    }
}

fn check_nested_must(
    point_id: PointOffsetType,
    nested: &NestedContainer,
    field_indexes: &IndexesMap,
    payload_provider: PayloadProvider,
    nested_path: JsonPathPayload,
) -> Option<(Vec<ElemIndex>, usize)> {
    match &nested.filter().must {
        None => None,
        Some(musts_conditions) => {
            let full_path = nested_path.extend(&nested.array_key());
            let matching_indices = nested_conditions_converter(
                musts_conditions,
                field_indexes,
                payload_provider,
                full_path,
            );
            let matches = find_indices_matching_all_conditions(point_id, &matching_indices);
            Some(matches)
        }
    }
}

fn check_nested_must_not(
    point_id: PointOffsetType,
    nested: &NestedContainer,
    field_indexes: &IndexesMap,
    payload_provider: PayloadProvider,
    nested_path: JsonPathPayload,
) -> Option<(Vec<ElemIndex>, usize)> {
    match &nested.filter().must_not {
        None => None,
        Some(musts_not_conditions) => {
            let full_path = nested_path.extend(&nested.array_key());
            let matching_indices = nested_conditions_converter(
                musts_not_conditions,
                field_indexes,
                payload_provider,
                full_path,
            );
            let matches = find_indices_matching_none_conditions(point_id, &matching_indices);
            Some(matches)
        }
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
                None => (vec![], 0),
                Some(values) => {
                    let len = values.len();
                    let matching = values
                        .iter()
                        .positions(|geo_point| geo_radius.check_point(geo_point.lon, geo_point.lat))
                        .collect();
                    (matching, len)
                }
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
                None => (vec![], 0),
                Some(values) => {
                    let len = values.len();
                    let matching = values
                        .iter()
                        .positions(|geo_point| {
                            geo_bounding_box.check_point(geo_point.lon, geo_point.lat)
                        })
                        .collect();
                    (matching, len)
                }
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
                None => (vec![], 0),
                Some(values) => {
                    let len = values.len();
                    let matching = values
                        .iter()
                        .copied()
                        .positions(|i| range.check_range(i as FloatPayloadType))
                        .collect();
                    (matching, len)
                }
            }
        })),
        FieldIndex::FloatIndex(num_index) => Some(Box::new(move |point_id: PointOffsetType| {
            match num_index.get_values(point_id) {
                None => (vec![], 0),
                Some(values) => {
                    let len = values.len();
                    let matching = values
                        .iter()
                        .copied()
                        .positions(|i| range.check_range(i))
                        .collect();
                    (matching, len)
                }
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
                        None => (vec![], 0),
                        Some(values) => {
                            let len = values.len();
                            let matching = values.iter().positions(|k| k == &keyword).collect();
                            (matching, len)
                        }
                    }
                }))
            }
            (ValueVariants::Integer(value), FieldIndex::IntMapIndex(index)) => {
                Some(Box::new(move |point_id: PointOffsetType| {
                    match index.get_values(point_id) {
                        None => (vec![], 0),
                        Some(values) => {
                            let len = values.len();
                            let matching = values.iter().positions(|i| i == &value).collect();
                            (matching, len)
                        }
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
                        None => (vec![], 0),
                        Some(doc) => {
                            let res = parsed_query.check_match(doc);
                            // Not sure it is entirely correct
                            if res {
                                (vec![0], 1)
                            } else {
                                (vec![], 0)
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
                        None => (vec![], 0),
                        Some(values) => {
                            let len = values.len();
                            let matching = values.iter().positions(|k| list.contains(k)).collect();
                            (matching, len)
                        }
                    }
                }))
            }
            (AnyVariants::Integers(list), FieldIndex::IntMapIndex(index)) => {
                Some(Box::new(move |point_id: PointOffsetType| {
                    match index.get_values(point_id) {
                        None => (vec![], 0),
                        Some(values) => {
                            let len = values.len();
                            let matching = values.iter().positions(|i| list.contains(i)).collect();
                            (matching, len)
                        }
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
            Box::new(|_point_id: PointOffsetType| (vec![], 0)),
            Box::new(|_point_id: PointOffsetType| (vec![], 0)),
            Box::new(|_point_id: PointOffsetType| (vec![], 0)),
        ];

        let merged = merge_nested_matching_indices(matching_indices_fn, false);
        // none of the conditions are matching anything
        let result: bool = merged(0);
        assert!(!result);
    }

    #[test]
    fn single_matching_merge_merge_nested_matching_indices() {
        let matching_indices_fn: Vec<NestedMatchingIndicesFn> = vec![
            Box::new(|_point_id: PointOffsetType| (vec![0], 1)),
            Box::new(|_point_id: PointOffsetType| (vec![0], 1)),
            Box::new(|_point_id: PointOffsetType| (vec![0], 1)),
        ];

        let merged = merge_nested_matching_indices(matching_indices_fn, false);
        let result: bool = merged(0);
        assert!(result);
    }

    #[test]
    fn single_non_matching_merge_nested_matching_indices() {
        let matching_indices_fn: Vec<NestedMatchingIndicesFn> = vec![
            Box::new(|_point_id: PointOffsetType| (vec![0], 1)),
            Box::new(|_point_id: PointOffsetType| (vec![0], 1)),
            Box::new(|_point_id: PointOffsetType| (vec![1], 1)),
        ];
        let merged = merge_nested_matching_indices(matching_indices_fn, false);
        // does not because all the checkers are not matching the same path
        let result: bool = merged(0);
        assert!(!result);
    }

    #[test]
    fn many_matching_merge_nested_matching_indices() {
        let matching_indices_fn: Vec<NestedMatchingIndicesFn> = vec![
            Box::new(|_point_id: PointOffsetType| (vec![0, 1], 2)),
            Box::new(|_point_id: PointOffsetType| (vec![0, 1], 2)),
            Box::new(|_point_id: PointOffsetType| (vec![0], 1)),
        ];

        let merged = merge_nested_matching_indices(matching_indices_fn, false);
        // still matching because of the path '0' matches all conditions
        let result: bool = merged(0);
        assert!(result);
    }
}
