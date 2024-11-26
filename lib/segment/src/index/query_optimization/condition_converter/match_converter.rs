use common::types::PointOffsetType;
use indexmap::IndexSet;
use uuid::Uuid;

use crate::index::field_index::FieldIndex;
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::payload_storage::condition_checker::INDEXSET_ITER_THRESHOLD;
use crate::types::{
    AnyVariants, Match, MatchAny, MatchExcept, MatchText, MatchValue, ValueVariants,
};

pub fn get_match_checkers(index: &FieldIndex, cond_match: Match) -> Option<ConditionCheckerFn> {
    match cond_match {
        Match::Value(MatchValue { value }) => get_match_value_checker(value, index),
        Match::Text(MatchText { text }) => get_match_text_checker(text, index),
        Match::Any(MatchAny { any }) => get_match_any_checker(any, index),
        Match::Except(MatchExcept { except }) => get_match_except_checker(except, index),
    }
}

fn get_match_value_checker(
    value_variant: ValueVariants,
    index: &FieldIndex,
) -> Option<ConditionCheckerFn> {
    match (value_variant, index) {
        (ValueVariants::String(keyword), FieldIndex::KeywordIndex(index)) => {
            Some(Box::new(move |point_id: PointOffsetType| {
                index.check_values_any(point_id, |k| k == keyword)
            }))
        }
        (ValueVariants::String(value), FieldIndex::UuidMapIndex(index)) => {
            let uuid = Uuid::parse_str(&value).map(|uuid| uuid.as_u128()).ok()?;
            Some(Box::new(move |point_id: PointOffsetType| {
                index.check_values_any(point_id, |i| i == &uuid)
            }))
        }
        (ValueVariants::Integer(value), FieldIndex::IntMapIndex(index)) => {
            Some(Box::new(move |point_id: PointOffsetType| {
                index.check_values_any(point_id, |i| *i == value)
            }))
        }
        (ValueVariants::Bool(is_true), FieldIndex::BoolIndex(index)) => {
            Some(Box::new(move |point_id: PointOffsetType| {
                if is_true {
                    index.values_has_true(point_id)
                } else {
                    index.values_has_false(point_id)
                }
            }))
        }
        (ValueVariants::Bool(_), FieldIndex::DatetimeIndex(_))
        | (ValueVariants::Bool(_), FieldIndex::FloatIndex(_))
        | (ValueVariants::Bool(_), FieldIndex::FullTextIndex(_))
        | (ValueVariants::Bool(_), FieldIndex::GeoIndex(_))
        | (ValueVariants::Bool(_), FieldIndex::IntIndex(_))
        | (ValueVariants::Bool(_), FieldIndex::IntMapIndex(_))
        | (ValueVariants::Bool(_), FieldIndex::KeywordIndex(_))
        | (ValueVariants::Bool(_), FieldIndex::UuidIndex(_))
        | (ValueVariants::Bool(_), FieldIndex::UuidMapIndex(_))
        | (ValueVariants::Integer(_), FieldIndex::BoolIndex(_))
        | (ValueVariants::Integer(_), FieldIndex::DatetimeIndex(_))
        | (ValueVariants::Integer(_), FieldIndex::FloatIndex(_))
        | (ValueVariants::Integer(_), FieldIndex::FullTextIndex(_))
        | (ValueVariants::Integer(_), FieldIndex::GeoIndex(_))
        | (ValueVariants::Integer(_), FieldIndex::IntIndex(_))
        | (ValueVariants::Integer(_), FieldIndex::KeywordIndex(_))
        | (ValueVariants::Integer(_), FieldIndex::UuidIndex(_))
        | (ValueVariants::Integer(_), FieldIndex::UuidMapIndex(_))
        | (ValueVariants::String(_), FieldIndex::BoolIndex(_))
        | (ValueVariants::String(_), FieldIndex::DatetimeIndex(_))
        | (ValueVariants::String(_), FieldIndex::FloatIndex(_))
        | (ValueVariants::String(_), FieldIndex::FullTextIndex(_))
        | (ValueVariants::String(_), FieldIndex::GeoIndex(_))
        | (ValueVariants::String(_), FieldIndex::IntIndex(_))
        | (ValueVariants::String(_), FieldIndex::IntMapIndex(_))
        | (ValueVariants::String(_), FieldIndex::UuidIndex(_)) => None,
    }
}

fn get_match_any_checker(
    any_variant: AnyVariants,
    index: &FieldIndex,
) -> Option<ConditionCheckerFn> {
    match (any_variant, index) {
        (AnyVariants::Strings(list), FieldIndex::KeywordIndex(index)) => {
            if list.len() < INDEXSET_ITER_THRESHOLD {
                Some(Box::new(move |point_id: PointOffsetType| {
                    index.check_values_any(point_id, |value| {
                        list.iter().any(|s| s.as_str() == value)
                    })
                }))
            } else {
                Some(Box::new(move |point_id: PointOffsetType| {
                    index.check_values_any(point_id, |value| list.contains(value))
                }))
            }
        }
        (AnyVariants::Strings(list), FieldIndex::UuidMapIndex(index)) => {
            let list = list
                .iter()
                .map(|s| Uuid::parse_str(s).map(|uuid| uuid.as_u128()).ok())
                .collect::<Option<IndexSet<_>>>()?;

            if list.len() < INDEXSET_ITER_THRESHOLD {
                Some(Box::new(move |point_id: PointOffsetType| {
                    index.check_values_any(point_id, |value| list.iter().any(|i| i == value))
                }))
            } else {
                Some(Box::new(move |point_id: PointOffsetType| {
                    index.check_values_any(point_id, |value| list.contains(value))
                }))
            }
        }
        (AnyVariants::Integers(list), FieldIndex::IntMapIndex(index)) => {
            if list.len() < INDEXSET_ITER_THRESHOLD {
                Some(Box::new(move |point_id: PointOffsetType| {
                    index.check_values_any(point_id, |value| list.iter().any(|i| i == value))
                }))
            } else {
                Some(Box::new(move |point_id: PointOffsetType| {
                    index.check_values_any(point_id, |value| list.contains(value))
                }))
            }
        }
        (AnyVariants::Integers(_), FieldIndex::BoolIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::DatetimeIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::FloatIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::FullTextIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::GeoIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::IntIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::KeywordIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::UuidIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::UuidMapIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::BoolIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::DatetimeIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::FloatIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::FullTextIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::GeoIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::IntIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::IntMapIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::UuidIndex(_)) => None,
    }
}

fn get_match_except_checker(except: AnyVariants, index: &FieldIndex) -> Option<ConditionCheckerFn> {
    let checker: Option<ConditionCheckerFn> = match (except, index) {
        (AnyVariants::Strings(list), FieldIndex::KeywordIndex(index)) => {
            if list.len() < INDEXSET_ITER_THRESHOLD {
                Some(Box::new(move |point_id: PointOffsetType| {
                    index.check_values_any(point_id, |value| {
                        !list.iter().any(|s| s.as_str() == value)
                    })
                }))
            } else {
                Some(Box::new(move |point_id: PointOffsetType| {
                    index.check_values_any(point_id, |value| !list.contains(value))
                }))
            }
        }
        (AnyVariants::Strings(list), FieldIndex::UuidMapIndex(index)) => {
            let list = list
                .iter()
                .map(|s| Uuid::parse_str(s).map(|uuid| uuid.as_u128()).ok())
                .collect::<Option<IndexSet<_>>>()?;

            if list.len() < INDEXSET_ITER_THRESHOLD {
                Some(Box::new(move |point_id: PointOffsetType| {
                    index.check_values_any(point_id, |value| !list.iter().any(|i| i == value))
                }))
            } else {
                Some(Box::new(move |point_id: PointOffsetType| {
                    index.check_values_any(point_id, |value| !list.contains(value))
                }))
            }
        }
        (AnyVariants::Integers(list), FieldIndex::IntMapIndex(index)) => {
            if list.len() < INDEXSET_ITER_THRESHOLD {
                Some(Box::new(move |point_id: PointOffsetType| {
                    index.check_values_any(point_id, |value| !list.iter().any(|i| i == value))
                }))
            } else {
                Some(Box::new(move |point_id: PointOffsetType| {
                    index.check_values_any(point_id, |value| !list.contains(value))
                }))
            }
        }
        (AnyVariants::Strings(_), FieldIndex::IntIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::DatetimeIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::IntMapIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::FloatIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::GeoIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::FullTextIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::BoolIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::UuidIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::IntIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::DatetimeIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::KeywordIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::FloatIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::GeoIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::FullTextIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::BoolIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::UuidIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::UuidMapIndex(_)) => None,
    };

    if checker.is_none() {
        return Some(Box::new(|point_id: PointOffsetType| {
            // If there is any other value of any other index, then it's a match
            index.values_count(point_id) > 0
        }));
    };

    checker
}

fn get_match_text_checker(text: String, index: &FieldIndex) -> Option<ConditionCheckerFn> {
    match index {
        FieldIndex::FullTextIndex(full_text_index) => {
            let parsed_query = full_text_index.parse_query(&text);
            Some(Box::new(move |point_id: PointOffsetType| {
                full_text_index.check_match(&parsed_query, point_id)
            }))
        }
        FieldIndex::BoolIndex(_)
        | FieldIndex::DatetimeIndex(_)
        | FieldIndex::FloatIndex(_)
        | FieldIndex::GeoIndex(_)
        | FieldIndex::IntIndex(_)
        | FieldIndex::IntMapIndex(_)
        | FieldIndex::KeywordIndex(_)
        | FieldIndex::UuidIndex(_)
        | FieldIndex::UuidMapIndex(_) => None,
    }
}
