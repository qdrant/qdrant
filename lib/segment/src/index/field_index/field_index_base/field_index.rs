use std::fmt::Formatter;
use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::Value;

use super::FieldIndexRead;
use super::payload_field_index::{PayloadFieldIndex, PayloadFieldIndexRead};
use super::value_indexer::ValueIndexer;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::bool_index::BoolIndex;
use crate::index::field_index::facet_index::{FacetIndex, FacetIndexEnum};
use crate::index::field_index::full_text_index::text_index::{
    FullTextIndex, PayloadMatchQueryType,
};
use crate::index::field_index::geo_index::GeoMapIndex;
use crate::index::field_index::map_index::MapIndex;
use crate::index::field_index::null_index::NullIndex;
use crate::index::field_index::numeric_index::{
    NumericFieldIndex, NumericFieldIndexRead, NumericIndex,
};
use crate::index::payload_config::{
    FullPayloadIndexType, IndexMutability, PayloadIndexType, StorageType,
};
use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    DateTimePayloadType, FieldCondition, FloatPayloadType, IntPayloadType, Match, MatchPhrase,
    MatchText, MatchTextAny, UuidIntType, UuidPayloadType,
};

/// Common interface for all possible types of field indexes
/// Enables polymorphism on field indexes
pub enum FieldIndex {
    IntIndex(NumericIndex<IntPayloadType, IntPayloadType>),
    DatetimeIndex(NumericIndex<IntPayloadType, DateTimePayloadType>),
    IntMapIndex(MapIndex<IntPayloadType>),
    KeywordIndex(MapIndex<str>),
    FloatIndex(NumericIndex<FloatPayloadType, FloatPayloadType>),
    GeoIndex(GeoMapIndex),
    FullTextIndex(FullTextIndex),
    BoolIndex(BoolIndex),
    UuidIndex(NumericIndex<UuidIntType, UuidPayloadType>),
    UuidMapIndex(MapIndex<UuidIntType>),
    NullIndex(NullIndex),
}

impl std::fmt::Debug for FieldIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldIndex::IntIndex(_index) => write!(f, "IntIndex"),
            FieldIndex::DatetimeIndex(_index) => write!(f, "DatetimeIndex"),
            FieldIndex::IntMapIndex(_index) => write!(f, "IntMapIndex"),
            FieldIndex::KeywordIndex(_index) => write!(f, "KeywordIndex"),
            FieldIndex::FloatIndex(_index) => write!(f, "FloatIndex"),
            FieldIndex::GeoIndex(_index) => write!(f, "GeoIndex"),
            FieldIndex::BoolIndex(_index) => write!(f, "BoolIndex"),
            FieldIndex::FullTextIndex(_index) => write!(f, "FullTextIndex"),
            FieldIndex::UuidIndex(_index) => write!(f, "UuidIndex"),
            FieldIndex::UuidMapIndex(_index) => write!(f, "UuidMapIndex"),
            FieldIndex::NullIndex(_index) => write!(f, "NullIndex"),
        }
    }
}

impl FieldIndex {
    /// Borrow the underlying typed index as the full
    /// [`PayloadFieldIndex`] trait object — used for write/lifecycle
    /// methods (`flusher`, `files`, `immutable_files`).
    fn get_payload_field_index(&self) -> &dyn PayloadFieldIndex {
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

    pub fn wipe(self) -> OperationResult<()> {
        match self {
            FieldIndex::IntIndex(index) => index.wipe(),
            FieldIndex::DatetimeIndex(index) => index.wipe(),
            FieldIndex::IntMapIndex(index) => index.wipe(),
            FieldIndex::KeywordIndex(index) => index.wipe(),
            FieldIndex::FloatIndex(index) => index.wipe(),
            FieldIndex::GeoIndex(index) => index.wipe(),
            FieldIndex::BoolIndex(index) => index.wipe(),
            FieldIndex::FullTextIndex(index) => index.wipe(),
            FieldIndex::UuidIndex(index) => index.wipe(),
            FieldIndex::UuidMapIndex(index) => index.wipe(),
            FieldIndex::NullIndex(index) => index.wipe(),
        }
    }

    pub fn flusher(&self) -> Flusher {
        self.get_payload_field_index().flusher()
    }

    pub fn files(&self) -> Vec<PathBuf> {
        self.get_payload_field_index().files()
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        self.get_payload_field_index().immutable_files()
    }

    pub fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            FieldIndex::IntIndex(payload_field_index) => {
                payload_field_index.add_point(id, payload, hw_counter)
            }
            FieldIndex::DatetimeIndex(payload_field_index) => {
                payload_field_index.add_point(id, payload, hw_counter)
            }
            FieldIndex::IntMapIndex(payload_field_index) => {
                payload_field_index.add_point(id, payload, hw_counter)
            }
            FieldIndex::KeywordIndex(payload_field_index) => {
                payload_field_index.add_point(id, payload, hw_counter)
            }
            FieldIndex::FloatIndex(payload_field_index) => {
                payload_field_index.add_point(id, payload, hw_counter)
            }
            FieldIndex::GeoIndex(payload_field_index) => {
                payload_field_index.add_point(id, payload, hw_counter)
            }
            FieldIndex::BoolIndex(payload_field_index) => {
                payload_field_index.add_point(id, payload, hw_counter)
            }
            FieldIndex::FullTextIndex(payload_field_index) => {
                payload_field_index.add_point(id, payload, hw_counter)
            }
            FieldIndex::UuidIndex(payload_field_index) => {
                payload_field_index.add_point(id, payload, hw_counter)
            }
            FieldIndex::UuidMapIndex(payload_field_index) => {
                payload_field_index.add_point(id, payload, hw_counter)
            }
            FieldIndex::NullIndex(payload_field_index) => {
                payload_field_index.add_point(id, payload, hw_counter)
            }
        }
    }

    pub fn remove_point(&mut self, point_id: PointOffsetType) -> OperationResult<()> {
        match self {
            FieldIndex::IntIndex(index) => index.mut_inner().remove_point(point_id),
            FieldIndex::DatetimeIndex(index) => index.mut_inner().remove_point(point_id),
            FieldIndex::IntMapIndex(index) => index.remove_point(point_id),
            FieldIndex::KeywordIndex(index) => index.remove_point(point_id),
            FieldIndex::FloatIndex(index) => index.mut_inner().remove_point(point_id),
            FieldIndex::GeoIndex(index) => index.remove_point(point_id),
            FieldIndex::BoolIndex(index) => index.remove_point(point_id),
            FieldIndex::FullTextIndex(index) => index.remove_point(point_id),
            FieldIndex::UuidIndex(index) => index.remove_point(point_id),
            FieldIndex::UuidMapIndex(index) => index.remove_point(point_id),
            FieldIndex::NullIndex(index) => index.remove_point(point_id),
        }
    }

    /// Approximate RAM usage in bytes for in-memory index structures.
    pub fn ram_usage_bytes(&self) -> usize {
        match self {
            FieldIndex::IntIndex(index) => index.inner().ram_usage_bytes(),
            FieldIndex::DatetimeIndex(index) => index.inner().ram_usage_bytes(),
            FieldIndex::IntMapIndex(index) => index.ram_usage_bytes(),
            FieldIndex::KeywordIndex(index) => index.ram_usage_bytes(),
            FieldIndex::FloatIndex(index) => index.inner().ram_usage_bytes(),
            FieldIndex::GeoIndex(index) => index.ram_usage_bytes(),
            FieldIndex::BoolIndex(index) => index.ram_usage_bytes(),
            FieldIndex::FullTextIndex(index) => index.ram_usage_bytes(),
            FieldIndex::UuidIndex(index) => index.inner().ram_usage_bytes(),
            FieldIndex::UuidMapIndex(index) => index.ram_usage_bytes(),
            FieldIndex::NullIndex(index) => index.ram_usage_bytes(),
        }
    }

    pub fn is_on_disk(&self) -> bool {
        match self {
            FieldIndex::IntIndex(index) => index.is_on_disk(),
            FieldIndex::DatetimeIndex(index) => index.is_on_disk(),
            FieldIndex::IntMapIndex(index) => index.is_on_disk(),
            FieldIndex::KeywordIndex(index) => index.is_on_disk(),
            FieldIndex::FloatIndex(index) => index.is_on_disk(),
            FieldIndex::GeoIndex(index) => index.is_on_disk(),
            FieldIndex::BoolIndex(index) => index.is_on_disk(),
            FieldIndex::FullTextIndex(index) => index.is_on_disk(),
            FieldIndex::UuidIndex(index) => index.is_on_disk(),
            FieldIndex::UuidMapIndex(index) => index.is_on_disk(),
            FieldIndex::NullIndex(index) => index.is_on_disk(),
        }
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        match self {
            FieldIndex::IntIndex(index) => index.populate(),
            FieldIndex::DatetimeIndex(index) => index.populate(),
            FieldIndex::IntMapIndex(index) => index.populate(),
            FieldIndex::KeywordIndex(index) => index.populate(),
            FieldIndex::FloatIndex(index) => index.populate(),
            FieldIndex::GeoIndex(index) => index.populate(),
            FieldIndex::BoolIndex(index) => index.populate(),
            FieldIndex::FullTextIndex(index) => index.populate(),
            FieldIndex::UuidIndex(index) => index.populate(),
            FieldIndex::UuidMapIndex(index) => index.populate(),
            FieldIndex::NullIndex(index) => index.populate(),
        }
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            FieldIndex::IntIndex(index) => index.clear_cache(),
            FieldIndex::DatetimeIndex(index) => index.clear_cache(),
            FieldIndex::IntMapIndex(index) => index.clear_cache(),
            FieldIndex::KeywordIndex(index) => index.clear_cache(),
            FieldIndex::FloatIndex(index) => index.clear_cache(),
            FieldIndex::GeoIndex(index) => index.clear_cache(),
            FieldIndex::BoolIndex(index) => index.clear_cache(),
            FieldIndex::FullTextIndex(index) => index.clear_cache(),
            FieldIndex::UuidIndex(index) => index.clear_cache(),
            FieldIndex::UuidMapIndex(index) => index.clear_cache(),
            FieldIndex::NullIndex(index) => index.clear_cache(),
        }
    }

    pub fn get_full_index_type(&self) -> FullPayloadIndexType {
        let index_type = match self {
            FieldIndex::IntIndex(_) => PayloadIndexType::IntIndex,
            FieldIndex::DatetimeIndex(_) => PayloadIndexType::DatetimeIndex,
            FieldIndex::IntMapIndex(_) => PayloadIndexType::IntMapIndex,
            FieldIndex::KeywordIndex(_) => PayloadIndexType::KeywordIndex,
            FieldIndex::FloatIndex(_) => PayloadIndexType::FloatIndex,
            FieldIndex::GeoIndex(_) => PayloadIndexType::GeoIndex,
            FieldIndex::FullTextIndex(_) => PayloadIndexType::FullTextIndex,
            FieldIndex::BoolIndex(_) => PayloadIndexType::BoolIndex,
            FieldIndex::UuidIndex(_) => PayloadIndexType::UuidIndex,
            FieldIndex::UuidMapIndex(_) => PayloadIndexType::UuidMapIndex,
            FieldIndex::NullIndex(_) => PayloadIndexType::NullIndex,
        };

        FullPayloadIndexType {
            index_type,
            mutability: self.get_mutability_type(),
            storage_type: self.get_storage_type(),
        }
    }

    fn get_mutability_type(&self) -> IndexMutability {
        match self {
            FieldIndex::IntIndex(index) => index.get_mutability_type(),
            FieldIndex::DatetimeIndex(index) => index.get_mutability_type(),
            FieldIndex::IntMapIndex(index) => index.get_mutability_type(),
            FieldIndex::KeywordIndex(index) => index.get_mutability_type(),
            FieldIndex::FloatIndex(index) => index.get_mutability_type(),
            FieldIndex::GeoIndex(index) => index.get_mutability_type(),
            FieldIndex::FullTextIndex(index) => index.get_mutability_type(),
            FieldIndex::BoolIndex(index) => index.get_mutability_type(),
            FieldIndex::UuidIndex(index) => index.get_mutability_type(),
            FieldIndex::UuidMapIndex(index) => index.get_mutability_type(),
            FieldIndex::NullIndex(index) => index.get_mutability_type(),
        }
    }

    fn get_storage_type(&self) -> StorageType {
        match self {
            FieldIndex::IntIndex(index) => index.get_storage_type(),
            FieldIndex::DatetimeIndex(index) => index.get_storage_type(),
            FieldIndex::IntMapIndex(index) => index.get_storage_type(),
            FieldIndex::KeywordIndex(index) => index.get_storage_type(),
            FieldIndex::FloatIndex(index) => index.get_storage_type(),
            FieldIndex::GeoIndex(index) => index.get_storage_type(),
            FieldIndex::FullTextIndex(index) => index.get_storage_type(),
            FieldIndex::BoolIndex(index) => index.get_storage_type(),
            FieldIndex::UuidIndex(index) => index.get_storage_type(),
            FieldIndex::UuidMapIndex(index) => index.get_storage_type(),
            FieldIndex::NullIndex(index) => index.get_storage_type(),
        }
    }
}

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
