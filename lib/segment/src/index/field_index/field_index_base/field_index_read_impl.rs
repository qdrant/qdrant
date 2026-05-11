use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::Value;

use super::field_index::FieldIndex;
use super::field_index_read::FieldIndexRead;
use super::payload_field_index::PayloadFieldIndexRead;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::facet_index::{FacetIndex, FacetIndexEnum};
use crate::index::field_index::full_text_index::text_index::PayloadMatchQueryType;
use crate::index::field_index::numeric_index::{NumericFieldIndex, NumericFieldIndexRead};
use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, Match, MatchPhrase, MatchText, MatchTextAny};

impl FieldIndexRead for FieldIndex {
    /// Try to check condition for a payload given a field index.
    /// Required because some index parameters may influence the condition checking logic.
    /// For example, full text index may have different tokenizers.
    ///
    /// Returns `None` if there is no special logic for the given index
    /// returns `Some(true)` if condition is satisfied
    /// returns `Some(false)` if condition is not satisfied
    fn special_check_condition(
        &self,
        condition: &FieldCondition,
        payload_value: &Value,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<bool>> {
        Ok(match self {
            FieldIndex::IntIndex(_) => None,
            FieldIndex::DatetimeIndex(_) => None,
            FieldIndex::IntMapIndex(_) => None,
            FieldIndex::KeywordIndex(_) => None,
            FieldIndex::FloatIndex(_) => None,
            FieldIndex::GeoIndex(_) => None,
            FieldIndex::BoolIndex(_) => None,
            FieldIndex::FullTextIndex(index) => match &condition.r#match {
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
                Some(Match::TextAny(MatchTextAny { text_any })) => {
                    Some(index.check_payload_match(
                        payload_value,
                        text_any,
                        PayloadMatchQueryType::TextAny,
                        hw_counter,
                    )?)
                }
                Some(Match::Value(_) | Match::Any(_) | Match::Except(_)) | None => None,
            },
            FieldIndex::UuidIndex(_) => None,
            FieldIndex::UuidMapIndex(_) => None,
            FieldIndex::NullIndex(_) => None,
        })
    }

    fn get_payload_field_index_read(&self) -> &dyn PayloadFieldIndexRead {
        match self {
            FieldIndex::IntIndex(payload_field_index) => payload_field_index.inner(),
            FieldIndex::DatetimeIndex(payload_field_index) => payload_field_index.inner(),
            FieldIndex::IntMapIndex(payload_field_index) => payload_field_index,
            FieldIndex::KeywordIndex(payload_field_index) => payload_field_index,
            FieldIndex::FloatIndex(payload_field_index) => payload_field_index.inner(),
            FieldIndex::GeoIndex(payload_field_index) => payload_field_index,
            FieldIndex::BoolIndex(payload_field_index) => payload_field_index,
            FieldIndex::FullTextIndex(payload_field_index) => payload_field_index,
            FieldIndex::UuidIndex(payload_field_index) => payload_field_index.inner(),
            FieldIndex::UuidMapIndex(payload_field_index) => payload_field_index,
            FieldIndex::NullIndex(payload_field_index) => payload_field_index,
        }
    }

    fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        match self {
            FieldIndex::IntIndex(index) => index.get_telemetry_data(),
            FieldIndex::DatetimeIndex(index) => index.get_telemetry_data(),
            FieldIndex::IntMapIndex(index) => index.get_telemetry_data(),
            FieldIndex::KeywordIndex(index) => index.get_telemetry_data(),
            FieldIndex::FloatIndex(index) => index.get_telemetry_data(),
            FieldIndex::GeoIndex(index) => index.get_telemetry_data(),
            FieldIndex::BoolIndex(index) => index.get_telemetry_data(),
            FieldIndex::FullTextIndex(index) => index.get_telemetry_data(),
            FieldIndex::UuidIndex(index) => index.get_telemetry_data(),
            FieldIndex::UuidMapIndex(index) => index.get_telemetry_data(),
            FieldIndex::NullIndex(index) => index.get_telemetry_data(),
        }
    }

    fn values_count(&self, point_id: PointOffsetType) -> usize {
        match self {
            FieldIndex::IntIndex(index) => index.values_count(point_id),
            FieldIndex::DatetimeIndex(index) => index.values_count(point_id),
            FieldIndex::IntMapIndex(index) => index.values_count(point_id),
            FieldIndex::KeywordIndex(index) => index.values_count(point_id),
            FieldIndex::FloatIndex(index) => index.values_count(point_id),
            FieldIndex::GeoIndex(index) => index.values_count(point_id),
            FieldIndex::BoolIndex(index) => index.values_count(point_id),
            FieldIndex::FullTextIndex(index) => index.values_count(point_id),
            FieldIndex::UuidIndex(index) => index.values_count(point_id),
            FieldIndex::UuidMapIndex(index) => index.values_count(point_id),
            FieldIndex::NullIndex(index) => index.values_count(point_id),
        }
    }

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        match self {
            FieldIndex::IntIndex(index) => index.values_is_empty(point_id),
            FieldIndex::DatetimeIndex(index) => index.values_is_empty(point_id),
            FieldIndex::IntMapIndex(index) => index.values_is_empty(point_id),
            FieldIndex::KeywordIndex(index) => index.values_is_empty(point_id),
            FieldIndex::FloatIndex(index) => index.values_is_empty(point_id),
            FieldIndex::GeoIndex(index) => index.values_is_empty(point_id),
            FieldIndex::BoolIndex(index) => index.values_is_empty(point_id),
            FieldIndex::FullTextIndex(index) => index.values_is_empty(point_id),
            FieldIndex::UuidIndex(index) => index.values_is_empty(point_id),
            FieldIndex::UuidMapIndex(index) => index.values_is_empty(point_id),
            FieldIndex::NullIndex(index) => index.values_is_empty(point_id),
        }
    }

    fn value_retriever<'a, 'q>(
        &'a self,
        hw_counter: &'q HardwareCounterCell,
    ) -> Option<VariableRetrieverFn<'q>>
    where
        'a: 'q,
    {
        // Per-variant value-to-`Value` conversion lives on each typed
        // index as an inherent `value_retriever` method (see e.g.
        // `NumericIndex<IntPayloadType, DateTimePayloadType>::value_retriever`).
        // This dispatch is mechanical — adding a `FieldIndex` variant
        // forces a compile error here.
        match self {
            FieldIndex::IntIndex(index) => Some(index.value_retriever(hw_counter)),
            FieldIndex::DatetimeIndex(index) => Some(index.value_retriever(hw_counter)),
            FieldIndex::IntMapIndex(index) => Some(index.value_retriever(hw_counter)),
            FieldIndex::KeywordIndex(index) => Some(index.value_retriever(hw_counter)),
            FieldIndex::FloatIndex(index) => Some(index.value_retriever(hw_counter)),
            FieldIndex::GeoIndex(index) => Some(index.value_retriever(hw_counter)),
            FieldIndex::BoolIndex(index) => Some(index.value_retriever(hw_counter)),
            FieldIndex::UuidIndex(index) => Some(index.value_retriever(hw_counter)),
            FieldIndex::UuidMapIndex(index) => Some(index.value_retriever(hw_counter)),
            // FullTextIndex: caller falls back to payload — text values
            // are easier to read directly than reconstruct from the
            // inverted index.
            FieldIndex::FullTextIndex(_) => None,
            // NullIndex: no underlying values to return; another index
            // on the same field is expected to provide them.
            FieldIndex::NullIndex(_) => None,
        }
    }

    fn as_numeric(&self) -> Option<impl NumericFieldIndexRead + '_> {
        match self {
            FieldIndex::IntIndex(index) => Some(NumericFieldIndex::IntIndex(index.inner())),
            FieldIndex::DatetimeIndex(index) => Some(NumericFieldIndex::IntIndex(index.inner())),
            FieldIndex::FloatIndex(index) => Some(NumericFieldIndex::FloatIndex(index.inner())),
            FieldIndex::IntMapIndex(_)
            | FieldIndex::KeywordIndex(_)
            | FieldIndex::GeoIndex(_)
            | FieldIndex::BoolIndex(_)
            | FieldIndex::UuidMapIndex(_)
            | FieldIndex::UuidIndex(_)
            | FieldIndex::FullTextIndex(_)
            | FieldIndex::NullIndex(_) => None,
        }
    }

    fn as_facet_index(&self) -> Option<impl FacetIndex + '_> {
        match self {
            FieldIndex::KeywordIndex(index) => Some(FacetIndexEnum::Keyword(index)),
            FieldIndex::IntMapIndex(index) => Some(FacetIndexEnum::Int(index)),
            FieldIndex::UuidMapIndex(index) => Some(FacetIndexEnum::Uuid(index)),
            FieldIndex::BoolIndex(index) => Some(FacetIndexEnum::Bool(index)),
            FieldIndex::UuidIndex(_)
            | FieldIndex::IntIndex(_)
            | FieldIndex::DatetimeIndex(_)
            | FieldIndex::FloatIndex(_)
            | FieldIndex::GeoIndex(_)
            | FieldIndex::FullTextIndex(_)
            | FieldIndex::NullIndex(_) => None,
        }
    }
}
