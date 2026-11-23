//! Zero-IO metadata predicates describing which [`FieldCondition`] shapes each
//! field index variant can serve via `filter_into` (posting materialization),
//! `condition_checker` (per-point checking), and `special_check_condition`.
//!
//! Shared between [`FieldIndex`](super::FieldIndex) and
//! [`ReadOnlyFieldIndex`](super::ReadOnlyFieldIndex) so both writable and
//! read-only views answer [`FieldIndexRead`] queries identically without
//! reading index data or triggering cold materialization.
//!
//! Every helper destructures [`FieldCondition`] and [`Match`] exhaustively (no
//! `..` or wildcard arms) so adding a new condition or match variant forces
//! these predicates to be updated alongside the index implementations.

use std::str::FromStr;

use uuid::Uuid;

use crate::types::{
    AnyVariants, FieldCondition, Match, MatchAny, MatchExcept, MatchPhrase, MatchPrefix,
    MatchSubstring, MatchText, MatchTextAny, MatchValue, ValueVariants,
};

pub(super) fn numeric_supports_posting_filter(condition: &FieldCondition) -> bool {
    let FieldCondition {
        key: _,
        r#match,
        range,
        geo_radius: _,
        geo_bounding_box: _,
        geo_polygon: _,
        values_count: _,
        is_empty: _,
        is_null: _,
    } = condition;

    if let Some(Match::Value(MatchValue {
        value: ValueVariants::String(keyword),
    })) = r#match
        && Uuid::from_str(keyword).is_ok()
    {
        return true;
    }

    range.is_some()
}

pub(super) fn numeric_supports_condition_checker(condition: &FieldCondition) -> bool {
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

    range.is_some()
}

pub(super) fn int_map_supports_posting_filter(condition: &FieldCondition) -> bool {
    let FieldCondition {
        key: _,
        r#match,
        range: _,
        geo_radius: _,
        geo_bounding_box: _,
        geo_polygon: _,
        values_count: _,
        is_empty: _,
        is_null: _,
    } = condition;

    let Some(cond_match) = r#match.as_ref() else {
        return false;
    };
    match cond_match {
        Match::Value(MatchValue {
            value: ValueVariants::Integer(_),
        })
        | Match::Any(MatchAny {
            any: AnyVariants::Integers(_),
        })
        | Match::Except(MatchExcept {
            except: AnyVariants::Integers(_),
        }) => true,
        Match::Any(MatchAny {
            any: AnyVariants::Strings(keywords),
        }) => keywords.is_empty(),
        Match::Value(MatchValue {
            value: ValueVariants::String(_) | ValueVariants::Bool(_),
        })
        | Match::Except(MatchExcept {
            except: AnyVariants::Strings(_),
        })
        | Match::Text(MatchText { text: _ })
        | Match::TextAny(MatchTextAny { text_any: _ })
        | Match::Phrase(MatchPhrase { phrase: _ })
        | Match::Prefix(MatchPrefix { prefix: _ })
        | Match::Substring(MatchSubstring { substring: _ }) => false,
    }
}

pub(super) fn int_map_supports_condition_checker(condition: &FieldCondition) -> bool {
    let FieldCondition {
        key: _,
        r#match,
        range: _,
        geo_radius: _,
        geo_bounding_box: _,
        geo_polygon: _,
        values_count: _,
        is_empty: _,
        is_null: _,
    } = condition;

    let Some(cond_match) = r#match.as_ref() else {
        return false;
    };
    match cond_match {
        Match::Value(MatchValue {
            value: ValueVariants::Integer(_),
        })
        | Match::Any(MatchAny {
            any: AnyVariants::Integers(_),
        })
        | Match::Except(MatchExcept {
            except: AnyVariants::Integers(_),
        }) => true,
        Match::Value(MatchValue {
            value: ValueVariants::String(_) | ValueVariants::Bool(_),
        })
        | Match::Any(MatchAny {
            any: AnyVariants::Strings(_),
        })
        | Match::Except(MatchExcept {
            except: AnyVariants::Strings(_),
        })
        | Match::Text(MatchText { text: _ })
        | Match::TextAny(MatchTextAny { text_any: _ })
        | Match::Phrase(MatchPhrase { phrase: _ })
        | Match::Prefix(MatchPrefix { prefix: _ })
        | Match::Substring(MatchSubstring { substring: _ }) => false,
    }
}

pub(super) fn str_map_supports_posting_filter(
    condition: &FieldCondition,
    has_prefix_index: bool,
) -> bool {
    let FieldCondition {
        key: _,
        r#match,
        range: _,
        geo_radius: _,
        geo_bounding_box: _,
        geo_polygon: _,
        values_count: _,
        is_empty: _,
        is_null: _,
    } = condition;

    let Some(cond_match) = r#match.as_ref() else {
        return false;
    };
    match cond_match {
        Match::Value(MatchValue {
            value: ValueVariants::String(_),
        })
        | Match::Any(MatchAny {
            any: AnyVariants::Strings(_),
        })
        | Match::Except(MatchExcept {
            except: AnyVariants::Strings(_),
        })
        | Match::Substring(MatchSubstring { substring: _ }) => true,
        Match::Prefix(MatchPrefix { prefix: _ }) => has_prefix_index,
        Match::Any(MatchAny {
            any: AnyVariants::Integers(integers),
        }) => integers.is_empty(),
        Match::Value(MatchValue {
            value: ValueVariants::Integer(_) | ValueVariants::Bool(_),
        })
        | Match::Except(MatchExcept {
            except: AnyVariants::Integers(_),
        })
        | Match::Text(MatchText { text: _ })
        | Match::TextAny(MatchTextAny { text_any: _ })
        | Match::Phrase(MatchPhrase { phrase: _ }) => false,
    }
}

pub(super) fn str_map_supports_condition_checker(condition: &FieldCondition) -> bool {
    let FieldCondition {
        key: _,
        r#match,
        range: _,
        geo_radius: _,
        geo_bounding_box: _,
        geo_polygon: _,
        values_count: _,
        is_empty: _,
        is_null: _,
    } = condition;

    let Some(cond_match) = r#match.as_ref() else {
        return false;
    };
    match cond_match {
        Match::Value(MatchValue {
            value: ValueVariants::String(_),
        })
        | Match::Any(MatchAny {
            any: AnyVariants::Strings(_),
        })
        | Match::Except(MatchExcept {
            except: AnyVariants::Strings(_),
        })
        | Match::Prefix(MatchPrefix { prefix: _ })
        | Match::Substring(MatchSubstring { substring: _ }) => true,
        Match::Value(MatchValue {
            value: ValueVariants::Integer(_) | ValueVariants::Bool(_),
        })
        | Match::Any(MatchAny {
            any: AnyVariants::Integers(_),
        })
        | Match::Except(MatchExcept {
            except: AnyVariants::Integers(_),
        })
        | Match::Text(MatchText { text: _ })
        | Match::TextAny(MatchTextAny { text_any: _ })
        | Match::Phrase(MatchPhrase { phrase: _ }) => false,
    }
}

pub(super) fn uuid_map_supports_posting_filter(condition: &FieldCondition) -> bool {
    let FieldCondition {
        key: _,
        r#match,
        range: _,
        geo_radius: _,
        geo_bounding_box: _,
        geo_polygon: _,
        values_count: _,
        is_empty: _,
        is_null: _,
    } = condition;

    let Some(cond_match) = r#match.as_ref() else {
        return false;
    };
    match cond_match {
        Match::Value(MatchValue {
            value: ValueVariants::String(keyword),
        }) => Uuid::from_str(keyword).is_ok(),
        Match::Any(MatchAny {
            any: AnyVariants::Strings(list),
        })
        | Match::Except(MatchExcept {
            except: AnyVariants::Strings(list),
        }) => list.iter().all(|s| Uuid::from_str(s).is_ok()),
        Match::Any(MatchAny {
            any: AnyVariants::Integers(integers),
        }) => integers.is_empty(),
        Match::Value(MatchValue {
            value: ValueVariants::Integer(_) | ValueVariants::Bool(_),
        })
        | Match::Except(MatchExcept {
            except: AnyVariants::Integers(_),
        })
        | Match::Text(MatchText { text: _ })
        | Match::TextAny(MatchTextAny { text_any: _ })
        | Match::Phrase(MatchPhrase { phrase: _ })
        | Match::Prefix(MatchPrefix { prefix: _ })
        | Match::Substring(MatchSubstring { substring: _ }) => false,
    }
}

pub(super) fn uuid_map_supports_condition_checker(condition: &FieldCondition) -> bool {
    let FieldCondition {
        key: _,
        r#match,
        range: _,
        geo_radius: _,
        geo_bounding_box: _,
        geo_polygon: _,
        values_count: _,
        is_empty: _,
        is_null: _,
    } = condition;

    let Some(cond_match) = r#match.as_ref() else {
        return false;
    };
    match cond_match {
        Match::Value(MatchValue {
            value: ValueVariants::String(keyword),
        }) => Uuid::parse_str(keyword).is_ok(),
        Match::Any(MatchAny {
            any: AnyVariants::Strings(list),
        })
        | Match::Except(MatchExcept {
            except: AnyVariants::Strings(list),
        }) => list.iter().all(|s| Uuid::parse_str(s).is_ok()),
        Match::Value(MatchValue {
            value: ValueVariants::Integer(_) | ValueVariants::Bool(_),
        })
        | Match::Any(MatchAny {
            any: AnyVariants::Integers(_),
        })
        | Match::Except(MatchExcept {
            except: AnyVariants::Integers(_),
        })
        | Match::Text(MatchText { text: _ })
        | Match::TextAny(MatchTextAny { text_any: _ })
        | Match::Phrase(MatchPhrase { phrase: _ })
        | Match::Prefix(MatchPrefix { prefix: _ })
        | Match::Substring(MatchSubstring { substring: _ }) => false,
    }
}

pub(super) fn geo_supports_condition(condition: &FieldCondition) -> bool {
    let FieldCondition {
        key: _,
        r#match: _,
        range: _,
        geo_radius,
        geo_bounding_box,
        geo_polygon,
        values_count: _,
        is_empty: _,
        is_null: _,
    } = condition;

    geo_radius.is_some() || geo_bounding_box.is_some() || geo_polygon.is_some()
}

pub(super) fn bool_supports_condition(condition: &FieldCondition) -> bool {
    let FieldCondition {
        key: _,
        r#match,
        range: _,
        geo_radius: _,
        geo_bounding_box: _,
        geo_polygon: _,
        values_count: _,
        is_empty: _,
        is_null: _,
    } = condition;

    let Some(cond_match) = r#match.as_ref() else {
        return false;
    };
    match cond_match {
        Match::Value(MatchValue {
            value: ValueVariants::Bool(_),
        }) => true,
        Match::Value(MatchValue {
            value: ValueVariants::String(_) | ValueVariants::Integer(_),
        })
        | Match::Any(MatchAny { any: _ })
        | Match::Except(MatchExcept { except: _ })
        | Match::Text(MatchText { text: _ })
        | Match::TextAny(MatchTextAny { text_any: _ })
        | Match::Phrase(MatchPhrase { phrase: _ })
        | Match::Prefix(MatchPrefix { prefix: _ })
        | Match::Substring(MatchSubstring { substring: _ }) => false,
    }
}

pub(super) fn full_text_supports_condition(condition: &FieldCondition) -> bool {
    let FieldCondition {
        key: _,
        r#match,
        range: _,
        geo_radius: _,
        geo_bounding_box: _,
        geo_polygon: _,
        values_count: _,
        is_empty: _,
        is_null: _,
    } = condition;

    let Some(cond_match) = r#match.as_ref() else {
        return false;
    };
    match cond_match {
        Match::Text(MatchText { text: _ })
        | Match::TextAny(MatchTextAny { text_any: _ })
        | Match::Phrase(MatchPhrase { phrase: _ }) => true,
        Match::Value(MatchValue { value: _ })
        | Match::Any(MatchAny { any: _ })
        | Match::Except(MatchExcept { except: _ })
        | Match::Prefix(MatchPrefix { prefix: _ })
        | Match::Substring(MatchSubstring { substring: _ }) => false,
    }
}

pub(super) fn null_supports_condition(condition: &FieldCondition) -> bool {
    let FieldCondition {
        key: _,
        r#match: _,
        range: _,
        geo_radius: _,
        geo_bounding_box: _,
        geo_polygon: _,
        values_count: _,
        is_empty,
        is_null,
    } = condition;

    is_empty.is_some() || is_null.is_some()
}
