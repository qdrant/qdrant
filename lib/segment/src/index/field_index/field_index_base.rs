use std::fmt::Formatter;
use std::path::PathBuf;

use common::types::PointOffsetType;
use serde_json::Value;

use super::bool_index::simple_bool_index::BoolIndexBuilder;
use super::facet_index::FacetIndex;
use super::full_text_index::mmap_text_index::FullTextMmapIndexBuilder;
use super::full_text_index::text_index::{FullTextIndex, FullTextIndexBuilder};
use super::geo_index::{GeoMapIndexBuilder, GeoMapIndexMmapBuilder};
use super::map_index::{MapIndex, MapIndexBuilder, MapIndexMmapBuilder};
use super::numeric_index::{
    NumericIndex, NumericIndexBuilder, NumericIndexMmapBuilder, StreamRange,
};
use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
use crate::data_types::order_by::OrderValue;
use crate::index::field_index::bool_index::simple_bool_index::BoolIndex;
use crate::index::field_index::geo_index::GeoMapIndex;
use crate::index::field_index::numeric_index::NumericIndexInner;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    DateTimePayloadType, FieldCondition, FloatPayloadType, IntPayloadType, Match, MatchText,
    PayloadKeyType, RangeInterface, UuidIntType, UuidPayloadType,
};

pub trait PayloadFieldIndex {
    /// Return number of points with at least one value indexed in here
    fn count_indexed_points(&self) -> usize;

    /// Load index from disk.
    fn load(&mut self) -> OperationResult<bool>;

    /// Remove db content or files of the current payload index
    fn cleanup(self) -> OperationResult<()>;

    /// Return function that flushes all pending updates to disk.
    fn flusher(&self) -> Flusher;

    fn files(&self) -> Vec<PathBuf>;

    /// Get iterator over points fitting given `condition`
    /// Return `None` if condition does not match the index type
    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>;

    /// Return estimation of amount of points which satisfy given condition.
    /// Returns `None` if the condition does not match the index type
    fn estimate_cardinality(&self, condition: &FieldCondition) -> Option<CardinalityEstimation>;

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
    fn add_point(&mut self, id: PointOffsetType, payload: &[&Value]) -> OperationResult<()> {
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
        self.add_many(id, flatten_values)
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
    pub fn check_condition(
        &self,
        condition: &FieldCondition,
        payload_value: &Value,
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
                Some(Match::Text(MatchText { text })) => {
                    let query = full_text_index.parse_query(text);
                    for value in FullTextIndex::get_values(payload_value) {
                        let document = full_text_index.parse_document(&value);
                        if query.check_match(&document) {
                            return Some(true);
                        }
                    }
                    Some(false)
                }
                _ => None,
            },
            FieldIndex::UuidIndex(_) => None,
            FieldIndex::UuidMapIndex(_) => None,
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
        }
    }

    pub fn load(&mut self) -> OperationResult<bool> {
        match self {
            FieldIndex::IntIndex(ref mut payload_field_index) => payload_field_index.load(),
            FieldIndex::DatetimeIndex(ref mut payload_field_index) => payload_field_index.load(),
            FieldIndex::IntMapIndex(ref mut payload_field_index) => payload_field_index.load(),
            FieldIndex::KeywordIndex(ref mut payload_field_index) => payload_field_index.load(),
            FieldIndex::FloatIndex(ref mut payload_field_index) => payload_field_index.load(),
            FieldIndex::GeoIndex(ref mut payload_field_index) => payload_field_index.load(),
            FieldIndex::BoolIndex(ref mut payload_field_index) => payload_field_index.load(),
            FieldIndex::FullTextIndex(ref mut payload_field_index) => payload_field_index.load(),
            FieldIndex::UuidIndex(ref mut payload_field_index) => payload_field_index.load(),
            FieldIndex::UuidMapIndex(ref mut payload_field_index) => payload_field_index.load(),
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
        }
    }

    pub fn count_indexed_points(&self) -> usize {
        self.get_payload_field_index().count_indexed_points()
    }

    pub fn flusher(&self) -> Flusher {
        self.get_payload_field_index().flusher()
    }

    pub fn files(&self) -> Vec<PathBuf> {
        self.get_payload_field_index().files()
    }

    pub fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        self.get_payload_field_index().filter(condition)
    }

    pub fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
    ) -> Option<CardinalityEstimation> {
        self.get_payload_field_index()
            .estimate_cardinality(condition)
    }

    pub fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        self.get_payload_field_index()
            .payload_blocks(threshold, key)
    }

    pub fn add_point(&mut self, id: PointOffsetType, payload: &[&Value]) -> OperationResult<()> {
        match self {
            FieldIndex::IntIndex(ref mut payload_field_index) => {
                payload_field_index.add_point(id, payload)
            }
            FieldIndex::DatetimeIndex(ref mut payload_field_index) => {
                payload_field_index.add_point(id, payload)
            }
            FieldIndex::IntMapIndex(ref mut payload_field_index) => {
                payload_field_index.add_point(id, payload)
            }
            FieldIndex::KeywordIndex(ref mut payload_field_index) => {
                payload_field_index.add_point(id, payload)
            }
            FieldIndex::FloatIndex(ref mut payload_field_index) => {
                payload_field_index.add_point(id, payload)
            }
            FieldIndex::GeoIndex(ref mut payload_field_index) => {
                payload_field_index.add_point(id, payload)
            }
            FieldIndex::BoolIndex(ref mut payload_field_index) => {
                payload_field_index.add_point(id, payload)
            }
            FieldIndex::FullTextIndex(ref mut payload_field_index) => {
                payload_field_index.add_point(id, payload)
            }
            FieldIndex::UuidIndex(ref mut payload_field_index) => {
                payload_field_index.add_point(id, payload)
            }
            FieldIndex::UuidMapIndex(ref mut payload_field_index) => {
                payload_field_index.add_point(id, payload)
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
        }
    }

    pub fn as_numeric(&self) -> Option<NumericFieldIndex> {
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
            | FieldIndex::FullTextIndex(_) => None,
        }
    }

    pub fn as_facet_index(&self) -> Option<FacetIndex> {
        match self {
            FieldIndex::KeywordIndex(index) => Some(FacetIndex::Keyword(index)),
            FieldIndex::IntMapIndex(index) => Some(FacetIndex::Int(index)),
            FieldIndex::UuidMapIndex(index) => Some(FacetIndex::Uuid(index)),
            FieldIndex::BoolIndex(index) => Some(FacetIndex::Bool(index)),
            FieldIndex::UuidIndex(_)
            | FieldIndex::IntIndex(_)
            | FieldIndex::DatetimeIndex(_)
            | FieldIndex::FloatIndex(_)
            | FieldIndex::GeoIndex(_)
            | FieldIndex::FullTextIndex(_) => None,
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

    fn add_point(&mut self, id: PointOffsetType, payload: &[&Value]) -> OperationResult<()>;

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
    IntIndex(NumericIndexBuilder<IntPayloadType, IntPayloadType>),
    IntMmapIndex(NumericIndexMmapBuilder<IntPayloadType, IntPayloadType>),
    DatetimeIndex(NumericIndexBuilder<IntPayloadType, DateTimePayloadType>),
    DatetimeMmapIndex(NumericIndexMmapBuilder<IntPayloadType, DateTimePayloadType>),
    IntMapIndex(MapIndexBuilder<IntPayloadType>),
    IntMapMmapIndex(MapIndexMmapBuilder<IntPayloadType>),
    KeywordIndex(MapIndexBuilder<str>),
    KeywordMmapIndex(MapIndexMmapBuilder<str>),
    FloatIndex(NumericIndexBuilder<FloatPayloadType, FloatPayloadType>),
    FloatMmapIndex(NumericIndexMmapBuilder<FloatPayloadType, FloatPayloadType>),
    GeoIndex(GeoMapIndexBuilder),
    GeoMmapIndex(GeoMapIndexMmapBuilder),
    FullTextIndex(FullTextIndexBuilder),
    FullTextMmapIndex(FullTextMmapIndexBuilder),
    BoolIndex(BoolIndexBuilder),
    UuidIndex(MapIndexBuilder<UuidIntType>),
    UuidMmapIndex(MapIndexMmapBuilder<UuidIntType>),
}

impl FieldIndexBuilderTrait for FieldIndexBuilder {
    type FieldIndexType = FieldIndex;

    fn init(&mut self) -> OperationResult<()> {
        match self {
            Self::IntIndex(index) => index.init(),
            Self::IntMmapIndex(index) => index.init(),
            Self::DatetimeIndex(index) => index.init(),
            Self::DatetimeMmapIndex(index) => index.init(),
            Self::IntMapIndex(index) => index.init(),
            Self::IntMapMmapIndex(index) => index.init(),
            Self::KeywordIndex(index) => index.init(),
            Self::KeywordMmapIndex(index) => index.init(),
            Self::FloatIndex(index) => index.init(),
            Self::FloatMmapIndex(index) => index.init(),
            Self::GeoIndex(index) => index.init(),
            Self::GeoMmapIndex(index) => index.init(),
            Self::BoolIndex(index) => index.init(),
            Self::FullTextIndex(index) => index.init(),
            Self::FullTextMmapIndex(builder) => builder.init(),
            Self::UuidIndex(index) => index.init(),
            Self::UuidMmapIndex(index) => index.init(),
        }
    }

    fn add_point(&mut self, id: PointOffsetType, payload: &[&Value]) -> OperationResult<()> {
        match self {
            Self::IntIndex(index) => index.add_point(id, payload),
            Self::IntMmapIndex(index) => index.add_point(id, payload),
            Self::DatetimeIndex(index) => index.add_point(id, payload),
            Self::DatetimeMmapIndex(index) => index.add_point(id, payload),
            Self::IntMapIndex(index) => index.add_point(id, payload),
            Self::IntMapMmapIndex(index) => index.add_point(id, payload),
            Self::KeywordIndex(index) => index.add_point(id, payload),
            Self::KeywordMmapIndex(index) => index.add_point(id, payload),
            Self::FloatIndex(index) => index.add_point(id, payload),
            Self::FloatMmapIndex(index) => index.add_point(id, payload),
            Self::GeoIndex(index) => index.add_point(id, payload),
            Self::GeoMmapIndex(index) => index.add_point(id, payload),
            Self::BoolIndex(index) => index.add_point(id, payload),
            Self::FullTextIndex(index) => index.add_point(id, payload),
            Self::FullTextMmapIndex(builder) => {
                FieldIndexBuilderTrait::add_point(builder, id, payload)
            }
            Self::UuidIndex(index) => index.add_point(id, payload),
            Self::UuidMmapIndex(index) => index.add_point(id, payload),
        }
    }

    fn finalize(self) -> OperationResult<FieldIndex> {
        Ok(match self {
            Self::IntIndex(index) => FieldIndex::IntIndex(index.finalize()?),
            Self::IntMmapIndex(index) => FieldIndex::IntIndex(index.finalize()?),
            Self::DatetimeIndex(index) => FieldIndex::DatetimeIndex(index.finalize()?),
            Self::DatetimeMmapIndex(index) => FieldIndex::DatetimeIndex(index.finalize()?),
            Self::IntMapIndex(index) => FieldIndex::IntMapIndex(index.finalize()?),
            Self::IntMapMmapIndex(index) => FieldIndex::IntMapIndex(index.finalize()?),
            Self::KeywordIndex(index) => FieldIndex::KeywordIndex(index.finalize()?),
            Self::KeywordMmapIndex(index) => FieldIndex::KeywordIndex(index.finalize()?),
            Self::FloatIndex(index) => FieldIndex::FloatIndex(index.finalize()?),
            Self::FloatMmapIndex(index) => FieldIndex::FloatIndex(index.finalize()?),
            Self::GeoIndex(index) => FieldIndex::GeoIndex(index.finalize()?),
            Self::GeoMmapIndex(index) => FieldIndex::GeoIndex(index.finalize()?),
            Self::BoolIndex(index) => FieldIndex::BoolIndex(index.finalize()?),
            Self::FullTextIndex(index) => FieldIndex::FullTextIndex(index.finalize()?),
            Self::FullTextMmapIndex(builder) => FieldIndex::FullTextIndex(builder.finalize()?),
            Self::UuidIndex(index) => FieldIndex::UuidMapIndex(index.finalize()?),
            Self::UuidMmapIndex(index) => FieldIndex::UuidMapIndex(index.finalize()?),
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
