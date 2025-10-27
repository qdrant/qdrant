use std::fmt::Formatter;
use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::Value;

use super::bool_index::BoolIndex;
use super::bool_index::mutable_bool_index::MutableBoolIndexBuilder;
use super::facet_index::FacetIndexEnum;
use super::full_text_index::mmap_text_index::FullTextMmapIndexBuilder;
use super::full_text_index::text_index::{FullTextGridstoreIndexBuilder, FullTextIndex};
use super::geo_index::{GeoMapIndexGridstoreBuilder, GeoMapIndexMmapBuilder};
#[cfg(feature = "rocksdb")]
use super::map_index::MapIndexBuilder;
use super::map_index::{MapIndex, MapIndexGridstoreBuilder, MapIndexMmapBuilder};
#[cfg(feature = "rocksdb")]
use super::numeric_index::NumericIndexBuilder;
use super::numeric_index::{
    NumericIndex, NumericIndexGridstoreBuilder, NumericIndexMmapBuilder, StreamRange,
};
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::data_types::order_by::OrderValue;
use crate::index::field_index::geo_index::GeoMapIndex;
use crate::index::field_index::null_index::MutableNullIndex;
use crate::index::field_index::null_index::mutable_null_index::MutableNullIndexBuilder;
use crate::index::field_index::numeric_index::NumericIndexInner;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::index::payload_config::{
    FullPayloadIndexType, IndexMutability, PayloadIndexType, StorageType,
};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    DateTimePayloadType, FieldCondition, FloatPayloadType, IntPayloadType, Match, MatchPhrase,
    MatchText, PayloadKeyType, RangeInterface, UuidIntType, UuidPayloadType,
};

pub trait PayloadFieldIndex {
    /// Return number of points with at least one value indexed in here
    fn count_indexed_points(&self) -> usize;

    /// Remove db content or files of the current payload index
    fn cleanup(self) -> OperationResult<()>;

    /// Return function that flushes all pending updates to disk.
    fn flusher(&self) -> (Flusher, Flusher);

    /// Immediately flush all pending updates and deletes to disk.
    fn flush_all(&self) -> OperationResult<()> {
        let (stage_1_flusher, stage_2_flusher) = self.flusher();
        stage_1_flusher()?;
        stage_2_flusher()?;
        Ok(())
    }

    fn files(&self) -> Vec<PathBuf>;

    fn immutable_files(&self) -> Vec<PathBuf>;

    /// Get iterator over points fitting given `condition`
    /// Return `None` if condition does not match the index type
    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>;

    /// Return estimation of amount of points which satisfy given condition.
    /// Returns `None` if the condition does not match the index type
    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> Option<CardinalityEstimation>;

    /// Iterate conditions for payload blocks with minimum size of `threshold`
    /// Required for building HNSW index
    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_>;
}

pub trait ValueIndexer {
    type ValueType;

    /// Add multiple values associated with a single point
    /// This function should be called only once for each point
    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<Self::ValueType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    /// Extract index-able value from payload `Value`
    fn get_value(value: &Value) -> Option<Self::ValueType>;

    /// Try to extract index-able values from payload `Value`, even if it is an array
    fn get_values(value: &Value) -> Vec<Self::ValueType> {
        match value {
            Value::Array(values) => values.iter().filter_map(|x| Self::get_value(x)).collect(),
            _ => Self::get_value(value).map(|x| vec![x]).unwrap_or_default(),
        }
    }

    /// Add point with payload to index
    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.remove_point(id)?;
        let mut flatten_values: Vec<_> = vec![];
        for value in payload.iter() {
            match value {
                Value::Array(values) => {
                    flatten_values.extend(values.iter().filter_map(|x| Self::get_value(x)));
                }
                _ => {
                    if let Some(x) = Self::get_value(value) {
                        flatten_values.push(x);
                    }
                }
            }
        }
        self.add_many(id, flatten_values, hw_counter)
    }

    /// remove a point from the index
    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()>;
}

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
    NullIndex(MutableNullIndex),
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
    /// Try to check condition for a payload given a field index.
    /// Required because some index parameters may influence the condition checking logic.
    /// For example, full text index may have different tokenizers.
    ///
    /// Returns `None` if there is no special logic for the given index
    /// returns `Some(true)` if condition is satisfied
    /// returns `Some(false)` if condition is not satisfied
    pub fn special_check_condition(
        &self,
        condition: &FieldCondition,
        payload_value: &Value,
        hw_counter: &HardwareCounterCell,
    ) -> Option<bool> {
        match self {
            FieldIndex::IntIndex(_) => None,
            FieldIndex::DatetimeIndex(_) => None,
            FieldIndex::IntMapIndex(_) => None,
            FieldIndex::KeywordIndex(_) => None,
            FieldIndex::FloatIndex(_) => None,
            FieldIndex::GeoIndex(_) => None,
            FieldIndex::BoolIndex(_) => None,
            FieldIndex::FullTextIndex(full_text_index) => match &condition.r#match {
                Some(Match::Text(MatchText { text })) => Some(
                    full_text_index.check_payload_match::<false>(payload_value, text, hw_counter),
                ),
                Some(Match::Phrase(MatchPhrase { phrase })) => Some(
                    full_text_index.check_payload_match::<true>(payload_value, phrase, hw_counter),
                ),
                _ => None,
            },
            FieldIndex::UuidIndex(_) => None,
            FieldIndex::UuidMapIndex(_) => None,
            FieldIndex::NullIndex(_) => None,
        }
    }

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

    pub fn cleanup(self) -> OperationResult<()> {
        match self {
            FieldIndex::IntIndex(index) => index.cleanup(),
            FieldIndex::DatetimeIndex(index) => index.cleanup(),
            FieldIndex::IntMapIndex(index) => index.cleanup(),
            FieldIndex::KeywordIndex(index) => index.cleanup(),
            FieldIndex::FloatIndex(index) => index.cleanup(),
            FieldIndex::GeoIndex(index) => index.cleanup(),
            FieldIndex::BoolIndex(index) => index.cleanup(),
            FieldIndex::FullTextIndex(index) => index.cleanup(),
            FieldIndex::UuidIndex(index) => index.cleanup(),
            FieldIndex::UuidMapIndex(index) => index.cleanup(),
            FieldIndex::NullIndex(index) => index.cleanup(),
        }
    }

    pub fn count_indexed_points(&self) -> usize {
        self.get_payload_field_index().count_indexed_points()
    }

    pub fn flusher(&self) -> (Flusher, Flusher) {
        self.get_payload_field_index().flusher()
    }

    pub fn files(&self) -> Vec<PathBuf> {
        self.get_payload_field_index().files()
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        self.get_payload_field_index().immutable_files()
    }

    pub fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        self.get_payload_field_index().filter(condition, hw_counter)
    }

    pub fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> Option<CardinalityEstimation> {
        self.get_payload_field_index()
            .estimate_cardinality(condition, hw_counter)
    }

    pub fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        self.get_payload_field_index()
            .payload_blocks(threshold, key)
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

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
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

    pub fn values_count(&self, point_id: PointOffsetType) -> usize {
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

    pub fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
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

    pub fn as_numeric(&self) -> Option<NumericFieldIndex<'_>> {
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

    pub fn as_facet_index(&self) -> Option<FacetIndexEnum<'_>> {
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

    #[cfg(feature = "rocksdb")]
    pub fn is_rocksdb(&self) -> bool {
        match self {
            FieldIndex::IntIndex(index) => index.is_rocksdb(),
            FieldIndex::DatetimeIndex(index) => index.is_rocksdb(),
            FieldIndex::IntMapIndex(index) => index.is_rocksdb(),
            FieldIndex::KeywordIndex(index) => index.is_rocksdb(),
            FieldIndex::FloatIndex(index) => index.is_rocksdb(),
            FieldIndex::GeoIndex(index) => index.is_rocksdb(),
            FieldIndex::BoolIndex(index) => index.is_rocksdb(),
            FieldIndex::FullTextIndex(index) => index.is_rocksdb(),
            FieldIndex::UuidIndex(index) => index.is_rocksdb(),
            FieldIndex::UuidMapIndex(index) => index.is_rocksdb(),
            FieldIndex::NullIndex(_) => false,
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

/// Common interface for all index builders.
pub trait FieldIndexBuilderTrait {
    /// The resulting type of the index.
    type FieldIndexType;

    /// Start building the index, e.g. create a database column or a directory.
    /// Expected to be called exactly once before any other method.
    fn init(&mut self) -> OperationResult<()>;

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    fn finalize(self) -> OperationResult<Self::FieldIndexType>;

    /// Create an empty index for testing purposes.
    #[cfg(test)]
    fn make_empty(mut self) -> OperationResult<Self::FieldIndexType>
    where
        Self: Sized,
    {
        self.init()?;
        self.finalize()
    }
}

/// Builders for all index types
pub enum FieldIndexBuilder {
    #[cfg(feature = "rocksdb")]
    IntIndex(NumericIndexBuilder<IntPayloadType, IntPayloadType>),
    IntMmapIndex(NumericIndexMmapBuilder<IntPayloadType, IntPayloadType>),
    IntGridstoreIndex(NumericIndexGridstoreBuilder<IntPayloadType, IntPayloadType>),
    #[cfg(feature = "rocksdb")]
    DatetimeIndex(NumericIndexBuilder<IntPayloadType, DateTimePayloadType>),
    DatetimeMmapIndex(NumericIndexMmapBuilder<IntPayloadType, DateTimePayloadType>),
    DatetimeGridstoreIndex(NumericIndexGridstoreBuilder<IntPayloadType, DateTimePayloadType>),
    #[cfg(feature = "rocksdb")]
    IntMapIndex(MapIndexBuilder<IntPayloadType>),
    IntMapMmapIndex(MapIndexMmapBuilder<IntPayloadType>),
    IntMapGridstoreIndex(MapIndexGridstoreBuilder<IntPayloadType>),
    #[cfg(feature = "rocksdb")]
    KeywordIndex(MapIndexBuilder<str>),
    KeywordMmapIndex(MapIndexMmapBuilder<str>),
    KeywordGridstoreIndex(MapIndexGridstoreBuilder<str>),
    #[cfg(feature = "rocksdb")]
    FloatIndex(NumericIndexBuilder<FloatPayloadType, FloatPayloadType>),
    FloatMmapIndex(NumericIndexMmapBuilder<FloatPayloadType, FloatPayloadType>),
    FloatGridstoreIndex(NumericIndexGridstoreBuilder<FloatPayloadType, FloatPayloadType>),
    #[cfg(feature = "rocksdb")]
    GeoIndex(super::geo_index::GeoMapIndexBuilder),
    GeoMmapIndex(GeoMapIndexMmapBuilder),
    GeoGridstoreIndex(GeoMapIndexGridstoreBuilder),
    #[cfg(feature = "rocksdb")]
    FullTextIndex(super::full_text_index::text_index::FullTextIndexRocksDbBuilder),
    FullTextMmapIndex(FullTextMmapIndexBuilder),
    FullTextGridstoreIndex(FullTextGridstoreIndexBuilder),
    #[cfg(feature = "rocksdb")]
    BoolIndex(super::bool_index::simple_bool_index::BoolIndexBuilder),
    BoolMmapIndex(MutableBoolIndexBuilder),
    #[cfg(feature = "rocksdb")]
    UuidIndex(MapIndexBuilder<UuidIntType>),
    UuidMmapIndex(MapIndexMmapBuilder<UuidIntType>),
    UuidGridstoreIndex(MapIndexGridstoreBuilder<UuidIntType>),
    NullIndex(MutableNullIndexBuilder),
}

impl FieldIndexBuilderTrait for FieldIndexBuilder {
    type FieldIndexType = FieldIndex;

    fn init(&mut self) -> OperationResult<()> {
        match self {
            #[cfg(feature = "rocksdb")]
            Self::IntIndex(index) => index.init(),
            Self::IntMmapIndex(index) => index.init(),
            Self::IntGridstoreIndex(index) => index.init(),
            #[cfg(feature = "rocksdb")]
            Self::DatetimeIndex(index) => index.init(),
            Self::DatetimeMmapIndex(index) => index.init(),
            Self::DatetimeGridstoreIndex(index) => index.init(),
            #[cfg(feature = "rocksdb")]
            Self::IntMapIndex(index) => index.init(),
            Self::IntMapMmapIndex(index) => index.init(),
            Self::IntMapGridstoreIndex(index) => index.init(),
            #[cfg(feature = "rocksdb")]
            Self::KeywordIndex(index) => index.init(),
            Self::KeywordMmapIndex(index) => index.init(),
            Self::KeywordGridstoreIndex(index) => index.init(),
            #[cfg(feature = "rocksdb")]
            Self::FloatIndex(index) => index.init(),
            Self::FloatMmapIndex(index) => index.init(),
            Self::FloatGridstoreIndex(index) => index.init(),
            #[cfg(feature = "rocksdb")]
            Self::GeoIndex(index) => index.init(),
            Self::GeoMmapIndex(index) => index.init(),
            Self::GeoGridstoreIndex(index) => index.init(),
            #[cfg(feature = "rocksdb")]
            Self::BoolIndex(index) => index.init(),
            Self::BoolMmapIndex(index) => index.init(),
            #[cfg(feature = "rocksdb")]
            Self::FullTextIndex(index) => index.init(),
            Self::FullTextMmapIndex(builder) => builder.init(),
            Self::FullTextGridstoreIndex(builder) => builder.init(),
            #[cfg(feature = "rocksdb")]
            Self::UuidIndex(index) => index.init(),
            Self::UuidMmapIndex(index) => index.init(),
            Self::UuidGridstoreIndex(index) => index.init(),
            Self::NullIndex(index) => index.init(),
        }
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            #[cfg(feature = "rocksdb")]
            Self::IntIndex(index) => index.add_point(id, payload, hw_counter),
            Self::IntMmapIndex(index) => index.add_point(id, payload, hw_counter),
            Self::IntGridstoreIndex(index) => index.add_point(id, payload, hw_counter),
            #[cfg(feature = "rocksdb")]
            Self::DatetimeIndex(index) => index.add_point(id, payload, hw_counter),
            Self::DatetimeMmapIndex(index) => index.add_point(id, payload, hw_counter),
            Self::DatetimeGridstoreIndex(index) => index.add_point(id, payload, hw_counter),
            #[cfg(feature = "rocksdb")]
            Self::IntMapIndex(index) => index.add_point(id, payload, hw_counter),
            Self::IntMapMmapIndex(index) => index.add_point(id, payload, hw_counter),
            Self::IntMapGridstoreIndex(index) => index.add_point(id, payload, hw_counter),
            #[cfg(feature = "rocksdb")]
            Self::KeywordIndex(index) => index.add_point(id, payload, hw_counter),
            Self::KeywordMmapIndex(index) => index.add_point(id, payload, hw_counter),
            Self::KeywordGridstoreIndex(index) => index.add_point(id, payload, hw_counter),
            #[cfg(feature = "rocksdb")]
            Self::FloatIndex(index) => index.add_point(id, payload, hw_counter),
            Self::FloatMmapIndex(index) => index.add_point(id, payload, hw_counter),
            Self::FloatGridstoreIndex(index) => index.add_point(id, payload, hw_counter),
            #[cfg(feature = "rocksdb")]
            Self::GeoIndex(index) => index.add_point(id, payload, hw_counter),
            Self::GeoMmapIndex(index) => index.add_point(id, payload, hw_counter),
            Self::GeoGridstoreIndex(index) => index.add_point(id, payload, hw_counter),
            #[cfg(feature = "rocksdb")]
            Self::BoolIndex(index) => index.add_point(id, payload, hw_counter),
            Self::BoolMmapIndex(index) => index.add_point(id, payload, hw_counter),
            #[cfg(feature = "rocksdb")]
            Self::FullTextIndex(index) => index.add_point(id, payload, hw_counter),
            Self::FullTextMmapIndex(builder) => {
                FieldIndexBuilderTrait::add_point(builder, id, payload, hw_counter)
            }
            Self::FullTextGridstoreIndex(builder) => {
                FieldIndexBuilderTrait::add_point(builder, id, payload, hw_counter)
            }
            #[cfg(feature = "rocksdb")]
            Self::UuidIndex(index) => index.add_point(id, payload, hw_counter),
            Self::UuidMmapIndex(index) => index.add_point(id, payload, hw_counter),
            Self::UuidGridstoreIndex(index) => index.add_point(id, payload, hw_counter),
            Self::NullIndex(index) => index.add_point(id, payload, hw_counter),
        }
    }

    fn finalize(self) -> OperationResult<FieldIndex> {
        Ok(match self {
            #[cfg(feature = "rocksdb")]
            Self::IntIndex(index) => FieldIndex::IntIndex(index.finalize()?),
            Self::IntMmapIndex(index) => FieldIndex::IntIndex(index.finalize()?),
            Self::IntGridstoreIndex(index) => FieldIndex::IntIndex(index.finalize()?),
            #[cfg(feature = "rocksdb")]
            Self::DatetimeIndex(index) => FieldIndex::DatetimeIndex(index.finalize()?),
            Self::DatetimeMmapIndex(index) => FieldIndex::DatetimeIndex(index.finalize()?),
            Self::DatetimeGridstoreIndex(index) => FieldIndex::DatetimeIndex(index.finalize()?),
            #[cfg(feature = "rocksdb")]
            Self::IntMapIndex(index) => FieldIndex::IntMapIndex(index.finalize()?),
            Self::IntMapMmapIndex(index) => FieldIndex::IntMapIndex(index.finalize()?),
            Self::IntMapGridstoreIndex(index) => FieldIndex::IntMapIndex(index.finalize()?),
            #[cfg(feature = "rocksdb")]
            Self::KeywordIndex(index) => FieldIndex::KeywordIndex(index.finalize()?),
            Self::KeywordMmapIndex(index) => FieldIndex::KeywordIndex(index.finalize()?),
            Self::KeywordGridstoreIndex(index) => FieldIndex::KeywordIndex(index.finalize()?),
            #[cfg(feature = "rocksdb")]
            Self::FloatIndex(index) => FieldIndex::FloatIndex(index.finalize()?),
            Self::FloatMmapIndex(index) => FieldIndex::FloatIndex(index.finalize()?),
            Self::FloatGridstoreIndex(index) => FieldIndex::FloatIndex(index.finalize()?),
            #[cfg(feature = "rocksdb")]
            Self::GeoIndex(index) => FieldIndex::GeoIndex(index.finalize()?),
            Self::GeoMmapIndex(index) => FieldIndex::GeoIndex(index.finalize()?),
            Self::GeoGridstoreIndex(index) => FieldIndex::GeoIndex(index.finalize()?),
            #[cfg(feature = "rocksdb")]
            Self::BoolIndex(index) => FieldIndex::BoolIndex(index.finalize()?),
            Self::BoolMmapIndex(index) => FieldIndex::BoolIndex(index.finalize()?),
            #[cfg(feature = "rocksdb")]
            Self::FullTextIndex(index) => FieldIndex::FullTextIndex(index.finalize()?),
            Self::FullTextMmapIndex(builder) => FieldIndex::FullTextIndex(builder.finalize()?),
            Self::FullTextGridstoreIndex(builder) => FieldIndex::FullTextIndex(builder.finalize()?),
            #[cfg(feature = "rocksdb")]
            Self::UuidIndex(index) => FieldIndex::UuidMapIndex(index.finalize()?),
            Self::UuidMmapIndex(index) => FieldIndex::UuidMapIndex(index.finalize()?),
            Self::UuidGridstoreIndex(index) => FieldIndex::UuidMapIndex(index.finalize()?),
            Self::NullIndex(index) => FieldIndex::NullIndex(index.finalize()?),
        })
    }
}

pub enum NumericFieldIndex<'a> {
    IntIndex(&'a NumericIndexInner<IntPayloadType>),
    FloatIndex(&'a NumericIndexInner<FloatPayloadType>),
}

impl<'a> StreamRange<OrderValue> for NumericFieldIndex<'a> {
    fn stream_range(
        &self,
        range: &RangeInterface,
    ) -> Box<dyn DoubleEndedIterator<Item = (OrderValue, PointOffsetType)> + 'a> {
        match self {
            NumericFieldIndex::IntIndex(index) => Box::new(
                index
                    .stream_range(range)
                    .map(|(v, p)| (OrderValue::from(v), p)),
            ),
            NumericFieldIndex::FloatIndex(index) => Box::new(
                index
                    .stream_range(range)
                    .map(|(v, p)| (OrderValue::from(v), p)),
            ),
        }
    }
}

impl<'a> NumericFieldIndex<'a> {
    pub fn get_ordering_values(
        &self,
        idx: PointOffsetType,
    ) -> Box<dyn Iterator<Item = OrderValue> + 'a> {
        match self {
            NumericFieldIndex::IntIndex(index) => Box::new(
                index
                    .get_values(idx)
                    .into_iter()
                    .flatten()
                    .map(OrderValue::Int),
            ),
            NumericFieldIndex::FloatIndex(index) => Box::new(
                index
                    .get_values(idx)
                    .into_iter()
                    .flatten()
                    .map(OrderValue::Float),
            ),
        }
    }
}
