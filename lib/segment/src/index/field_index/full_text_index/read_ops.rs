use common::condition_checker::{ConditionChecker, ConstantConditionChecker};
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UserData;
use serde_json::Value;

use super::FullTextIndex;
use super::full_text_index_read::{FullTextIndexRead, PayloadMatchQueryType};
use super::inverted_index::{ParsedQuery, TokenId};
use super::tokenizers::Tokenizer;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndexRead,
};
use crate::index::payload_config::StorageType;
use crate::types::{
    FieldCondition, Match, MatchAny, MatchExcept, MatchPhrase, MatchPrefix, MatchText,
    MatchTextAny, MatchValue, PayloadKeyType,
};

impl FullTextIndexRead for FullTextIndex {
    fn tokenizer(&self) -> &Tokenizer {
        match self {
            Self::Mutable(index) => index.tokenizer(),
            Self::Immutable(index) => index.tokenizer(),
            Self::OnDisk(index) => index.tokenizer(),
        }
    }

    fn telemetry_index_type(&self) -> &'static str {
        match self {
            Self::Mutable(index) => index.telemetry_index_type(),
            Self::Immutable(index) => index.telemetry_index_type(),
            Self::OnDisk(index) => index.telemetry_index_type(),
        }
    }

    fn points_count(&self) -> usize {
        match self {
            Self::Mutable(index) => index.points_count(),
            Self::Immutable(index) => index.points_count(),
            Self::OnDisk(index) => index.points_count(),
        }
    }

    fn values_count(&self, point_id: PointOffsetType) -> usize {
        match self {
            Self::Mutable(index) => index.values_count(point_id),
            Self::Immutable(index) => index.values_count(point_id),
            Self::OnDisk(index) => index.values_count(point_id),
        }
    }

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        match self {
            Self::Mutable(index) => index.values_is_empty(point_id),
            Self::Immutable(index) => index.values_is_empty(point_id),
            Self::OnDisk(index) => index.values_is_empty(point_id),
        }
    }

    fn for_each_token_id<'a, U: UserData>(
        &self,
        iter: impl Iterator<Item = (U, &'a str)>,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(U, Option<TokenId>),
    ) -> OperationResult<()> {
        match self {
            Self::Mutable(index) => index.for_each_token_id(iter, hw_counter, f),
            Self::Immutable(index) => index.for_each_token_id(iter, hw_counter, f),
            Self::OnDisk(index) => index.for_each_token_id(iter, hw_counter, f),
        }
    }

    fn filter_query<'a>(
        &'a self,
        query: ParsedQuery,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        match self {
            Self::Mutable(index) => index.filter_query(query, hw_counter),
            Self::Immutable(index) => index.filter_query(query, hw_counter),
            Self::OnDisk(index) => index.filter_query(query, hw_counter),
        }
    }

    fn estimate_query_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation> {
        match self {
            Self::Mutable(index) => index.estimate_query_cardinality(query, condition, hw_counter),
            Self::Immutable(index) => {
                index.estimate_query_cardinality(query, condition, hw_counter)
            }
            Self::OnDisk(index) => index.estimate_query_cardinality(query, condition, hw_counter),
        }
    }

    fn check_match(&self, query: &ParsedQuery, point_id: PointOffsetType) -> OperationResult<bool> {
        match self {
            Self::Mutable(index) => index.check_match(query, point_id),
            Self::Immutable(index) => index.check_match(query, point_id),
            Self::OnDisk(index) => index.check_match(query, point_id),
        }
    }

    fn for_each_payload_block_inner(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            Self::Mutable(index) => index.for_each_payload_block_inner(threshold, key, f),
            Self::Immutable(index) => index.for_each_payload_block_inner(threshold, key, f),
            Self::OnDisk(index) => index.for_each_payload_block_inner(threshold, key, f),
        }
    }

    fn get_storage_type(&self) -> StorageType {
        match self {
            Self::Mutable(index) => FullTextIndexRead::get_storage_type(index),
            Self::Immutable(index) => FullTextIndexRead::get_storage_type(index),
            Self::OnDisk(index) => FullTextIndexRead::get_storage_type(index),
        }
    }

    fn ram_usage_bytes(&self) -> usize {
        match self {
            Self::Mutable(index) => FullTextIndexRead::ram_usage_bytes(index),
            Self::Immutable(index) => FullTextIndexRead::ram_usage_bytes(index),
            Self::OnDisk(index) => FullTextIndexRead::ram_usage_bytes(index),
        }
    }

    fn is_on_disk(&self) -> bool {
        match self {
            Self::Mutable(index) => FullTextIndexRead::is_on_disk(index),
            Self::Immutable(index) => FullTextIndexRead::is_on_disk(index),
            Self::OnDisk(index) => FullTextIndexRead::is_on_disk(index),
        }
    }
}

impl PayloadFieldIndexRead for FullTextIndex {
    fn count_indexed_points(&self) -> OperationResult<usize> {
        Ok(FullTextIndexRead::points_count(self))
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        filter(self, condition, hw_counter)
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        estimate_cardinality(self, condition, hw_counter)
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        for_each_payload_block(self, threshold, key, f)
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> OperationResult<Option<ConditionCheckerEnum<'a>>> {
        condition_checker(
            self,
            condition,
            hw_acc,
            ConditionCheckerEnum::FullTextWritable,
        )
    }

    fn special_check_condition(
        &self,
        condition: &FieldCondition,
        payload_value: &Value,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<bool>> {
        special_check_condition(self, condition, payload_value, hw_counter)
    }
}

impl FullTextIndex {
    #[cfg(test)]
    pub fn query<'a>(
        &'a self,
        query: &'a str,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        let Some(parsed_query) = self.parse_text_query(query, hw_counter)? else {
            return Ok(Box::new(std::iter::empty()));
        };
        self.filter_query(parsed_query, hw_counter)
    }
}

/// Body for [`PayloadFieldIndexRead::filter`]. Shared between [`FullTextIndex`]
/// and `ReadOnlyFullTextIndex<S>`.
pub fn filter<'a, T: FullTextIndexRead>(
    index: &'a T,
    condition: &FieldCondition,
    hw_counter: &'a HardwareCounterCell,
) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
    let Some(r#match) = &condition.r#match else {
        return Ok(None);
    };

    let parsed_query_opt = match r#match {
        Match::Text(MatchText { text }) => index.parse_text_query(text, hw_counter),
        Match::Phrase(MatchPhrase { phrase }) => index.parse_phrase_query(phrase, hw_counter),
        Match::TextAny(MatchTextAny { text_any }) => {
            index.parse_text_any_query(text_any, hw_counter)
        }
        Match::Value(_) | Match::Any(_) | Match::Except(_) | Match::Prefix(_) => {
            return Ok(None);
        }
    }?;

    let Some(parsed_query) = parsed_query_opt else {
        return Ok(Some(Box::new(std::iter::empty())));
    };

    Ok(Some(index.filter_query(parsed_query, hw_counter)?))
}

/// Body for [`PayloadFieldIndexRead::estimate_cardinality`]. Shared.
pub fn estimate_cardinality<T: FullTextIndexRead>(
    index: &T,
    condition: &FieldCondition,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<Option<CardinalityEstimation>> {
    let Some(r#match) = &condition.r#match else {
        return Ok(None);
    };

    let parsed_query_opt = match r#match {
        Match::Text(MatchText { text }) => index.parse_text_query(text, hw_counter),
        Match::Phrase(MatchPhrase { phrase }) => index.parse_phrase_query(phrase, hw_counter),
        Match::TextAny(MatchTextAny { text_any }) => {
            index.parse_text_any_query(text_any, hw_counter)
        }
        Match::Value(_) | Match::Any(_) | Match::Except(_) | Match::Prefix(_) => {
            return Ok(None);
        }
    }?;

    let Some(parsed_query) = parsed_query_opt else {
        return Ok(Some(CardinalityEstimation::exact(0)));
    };

    Ok(Some(index.estimate_query_cardinality(
        &parsed_query,
        condition,
        hw_counter,
    )?))
}

/// Body for [`PayloadFieldIndexRead::for_each_payload_block`]. Shared.
pub fn for_each_payload_block<T: FullTextIndexRead>(
    index: &T,
    threshold: usize,
    key: PayloadKeyType,
    f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
) -> OperationResult<()> {
    index.for_each_payload_block_inner(threshold, key, f)
}

/// Body for [`PayloadFieldIndexRead::condition_checker`]. Shared.
pub fn condition_checker<'a, T: FullTextIndexRead>(
    index: &'a T,
    condition: &FieldCondition,
    hw_acc: HwMeasurementAcc,
    to_enum: impl FnOnce(FullTextConditionChecker<'a, T>) -> ConditionCheckerEnum<'a>,
) -> OperationResult<Option<ConditionCheckerEnum<'a>>> {
    // Destructure explicitly (no `..`) so a new field added to
    // `FieldCondition` forces this method to be revisited.
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
        return Ok(None);
    };
    let hw_counter = hw_acc.get_counter_cell();

    // FullTextIndex serves Text / TextAny / Phrase only. Other
    // Match variants are explicitly listed so a new `Match`
    // variant forces a decision.
    let (text, query_type): (&str, _) = match cond_match {
        Match::Text(MatchText { text }) => (text, PayloadMatchQueryType::Text),
        Match::TextAny(MatchTextAny { text_any }) => (text_any, PayloadMatchQueryType::TextAny),
        Match::Phrase(MatchPhrase { phrase }) => (phrase, PayloadMatchQueryType::Phrase),
        Match::Value(MatchValue { value: _ })
        | Match::Any(MatchAny { any: _ })
        | Match::Except(MatchExcept { except: _ })
        | Match::Prefix(MatchPrefix { prefix: _ }) => return Ok(None),
    };

    let query_opt = match query_type {
        PayloadMatchQueryType::Phrase => index.parse_phrase_query(text, &hw_counter),
        PayloadMatchQueryType::Text => index.parse_text_query(text, &hw_counter),
        PayloadMatchQueryType::TextAny => index.parse_text_any_query(text, &hw_counter),
    }?;

    let Some(parsed_query) = query_opt else {
        return Ok(Some(ConditionCheckerEnum::Constant(
            ConstantConditionChecker::MATCH_NONE,
        )));
    };

    Ok(Some(to_enum(FullTextConditionChecker {
        index,
        parsed_query,
    })))
}

pub struct FullTextConditionChecker<'a, T> {
    index: &'a T,
    parsed_query: ParsedQuery,
}

impl<T: FullTextIndexRead> ConditionChecker for FullTextConditionChecker<'_, T> {
    type Error = OperationError;

    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        self.index.check_match(&self.parsed_query, point_id)
    }
}

/// Body for [`PayloadFieldIndexRead::special_check_condition`]. Shared.
pub fn special_check_condition<T: FullTextIndexRead>(
    index: &T,
    condition: &FieldCondition,
    payload_value: &serde_json::Value,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<Option<bool>> {
    Ok(match &condition.r#match {
        Some(Match::Text(MatchText { text })) => Some(index.check_payload_match(
            payload_value,
            text,
            PayloadMatchQueryType::Text,
            hw_counter,
        )?),
        Some(Match::Phrase(MatchPhrase { phrase })) => Some(index.check_payload_match(
            payload_value,
            phrase,
            PayloadMatchQueryType::Phrase,
            hw_counter,
        )?),
        Some(Match::TextAny(MatchTextAny { text_any })) => Some(index.check_payload_match(
            payload_value,
            text_any,
            PayloadMatchQueryType::TextAny,
            hw_counter,
        )?),
        Some(Match::Value(_) | Match::Any(_) | Match::Except(_) | Match::Prefix(_)) | None => None,
    })
}
