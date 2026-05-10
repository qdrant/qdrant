use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::types::PointOffsetType;

use crate::index::field_index::{FieldIndex, FieldIndexRead};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::types::{
    AnyVariants, Match, MatchAny, MatchExcept, MatchPhrase, MatchText, MatchTextAny, MatchValue,
    ValueVariants,
};

pub fn get_match_checkers(
    index: &FieldIndex,
    cond_match: Match,
    hw_acc: HwMeasurementAcc,
) -> Option<ConditionCheckerFn<'_>> {
    match cond_match {
        Match::Value(MatchValue { value }) => get_match_value_checker(value, index, hw_acc),
        Match::Text(MatchText { text }) => {
            get_match_text_checker(text, TextQueryType::Text, index, hw_acc)
        }
        Match::TextAny(MatchTextAny { text_any }) => {
            get_match_text_checker(text_any, TextQueryType::TextAny, index, hw_acc)
        }
        Match::Phrase(MatchPhrase { phrase }) => {
            get_match_text_checker(phrase, TextQueryType::Phrase, index, hw_acc)
        }
        Match::Any(MatchAny { any }) => get_match_any_checker(any, index, hw_acc),
        Match::Except(MatchExcept { except }) => {
            Some(get_match_except_checker(except, index, hw_acc))
        }
    }
}

fn get_match_value_checker(
    value_variant: ValueVariants,
    index: &FieldIndex,
    hw_acc: HwMeasurementAcc,
) -> Option<ConditionCheckerFn<'_>> {
    // Map cases (String/KeywordIndex, String/UuidMapIndex,
    // Integer/IntMapIndex) are served via `condition_checker` on
    // `MapIndex<K>`. Only `(Bool, BoolIndex)` remains here until
    // `BoolIndex` migrates in a follow-up PR.
    match (value_variant, index) {
        (ValueVariants::Bool(is_true), FieldIndex::BoolIndex(index)) => {
            let hw_counter = hw_acc.get_counter_cell();
            Some(Box::new(move |point_id: PointOffsetType| {
                index.check_values_any(point_id, is_true, &hw_counter)
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
        | (ValueVariants::Bool(_), FieldIndex::NullIndex(_))
        | (ValueVariants::Integer(_), FieldIndex::BoolIndex(_))
        | (ValueVariants::Integer(_), FieldIndex::DatetimeIndex(_))
        | (ValueVariants::Integer(_), FieldIndex::FloatIndex(_))
        | (ValueVariants::Integer(_), FieldIndex::FullTextIndex(_))
        | (ValueVariants::Integer(_), FieldIndex::GeoIndex(_))
        | (ValueVariants::Integer(_), FieldIndex::IntIndex(_))
        | (ValueVariants::Integer(_), FieldIndex::IntMapIndex(_))
        | (ValueVariants::Integer(_), FieldIndex::KeywordIndex(_))
        | (ValueVariants::Integer(_), FieldIndex::UuidIndex(_))
        | (ValueVariants::Integer(_), FieldIndex::UuidMapIndex(_))
        | (ValueVariants::Integer(_), FieldIndex::NullIndex(_))
        | (ValueVariants::String(_), FieldIndex::BoolIndex(_))
        | (ValueVariants::String(_), FieldIndex::DatetimeIndex(_))
        | (ValueVariants::String(_), FieldIndex::FloatIndex(_))
        | (ValueVariants::String(_), FieldIndex::FullTextIndex(_))
        | (ValueVariants::String(_), FieldIndex::GeoIndex(_))
        | (ValueVariants::String(_), FieldIndex::IntIndex(_))
        | (ValueVariants::String(_), FieldIndex::IntMapIndex(_))
        | (ValueVariants::String(_), FieldIndex::KeywordIndex(_))
        | (ValueVariants::String(_), FieldIndex::UuidIndex(_))
        | (ValueVariants::String(_), FieldIndex::UuidMapIndex(_))
        | (ValueVariants::String(_), FieldIndex::NullIndex(_)) => None,
    }
}

fn get_match_any_checker(
    any_variant: AnyVariants,
    index: &FieldIndex,
    _hw_acc: HwMeasurementAcc,
) -> Option<ConditionCheckerFn<'_>> {
    // All `Match::Any` cases against `MapIndex<K>` are served via
    // `condition_checker` on the typed index. No other variant ever
    // served `Any` — explicit list below to keep this exhaustive so a
    // new `FieldIndex` variant or `AnyVariants` case forces a decision.
    match (any_variant, index) {
        (AnyVariants::Strings(_), FieldIndex::BoolIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::DatetimeIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::FloatIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::FullTextIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::GeoIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::IntIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::IntMapIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::KeywordIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::UuidIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::UuidMapIndex(_))
        | (AnyVariants::Strings(_), FieldIndex::NullIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::BoolIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::DatetimeIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::FloatIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::FullTextIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::GeoIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::IntIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::IntMapIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::KeywordIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::UuidIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::UuidMapIndex(_))
        | (AnyVariants::Integers(_), FieldIndex::NullIndex(_)) => None,
    }
}

fn get_match_except_checker(
    _except: AnyVariants,
    index: &FieldIndex,
    _hw_acc: HwMeasurementAcc,
) -> ConditionCheckerFn<'_> {
    // Typed `Match::Except` cases against `MapIndex<K>` are served via
    // `condition_checker` on the typed index. The legacy uniform
    // fallback survives: when nobody handled the Except (e.g. the
    // list's value type doesn't match any indexed field's type), match
    // any point that has at least one value in the index — the value
    // can't possibly be in the type-mismatched list.
    Box::new(|point_id: PointOffsetType| index.values_count(point_id) > 0)
}

enum TextQueryType {
    Phrase,
    Text,
    TextAny,
}

fn get_match_text_checker(
    text: String,
    query_type: TextQueryType,
    index: &FieldIndex,
    hw_acc: HwMeasurementAcc,
) -> Option<ConditionCheckerFn<'_>> {
    let hw_counter = hw_acc.get_counter_cell();
    match index {
        FieldIndex::FullTextIndex(full_text_index) => {
            let query_opt = match query_type {
                TextQueryType::Phrase => full_text_index.parse_phrase_query(&text, &hw_counter),
                TextQueryType::Text => full_text_index.parse_text_query(&text, &hw_counter),
                TextQueryType::TextAny => full_text_index.parse_text_any_query(&text, &hw_counter),
            };

            let parsed_query = match query_opt {
                Ok(Some(query)) => query,
                Ok(None) => return Some(Box::new(|_| false)),
                Err(_) => {
                    // FIXME(uio): don't silently ignore errors. Log error? Update ConditionCheckerFn?
                    return Some(Box::new(|_| false));
                }
            };

            Some(Box::new(move |point_id: PointOffsetType| {
                // FIXME(uio): don't silently ignore errors. Log error? Update ConditionCheckerFn?
                full_text_index
                    .check_match(&parsed_query, point_id)
                    .unwrap_or(false)
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
        | FieldIndex::UuidMapIndex(_)
        | FieldIndex::NullIndex(_) => None,
    }
}
