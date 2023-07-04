use enum_dispatch::enum_dispatch;
use serde_json::Value;

use self::private::DbWrapper;
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::common::utils::MultiValue;
use crate::common::Flusher;
use crate::entry::entry_point::OperationResult;
use crate::index::field_index::binary_index::BinaryIndex;
use crate::index::field_index::full_text_index::text_index::FullTextIndex;
use crate::index::field_index::geo_index::GeoMapIndex;
use crate::index::field_index::map_index::MapIndex;
use crate::index::field_index::numeric_index::NumericIndex;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    FieldCondition, FloatPayloadType, IntPayloadType, Match, MatchText, PayloadKeyType,
    PointOffsetType,
};

pub(super) mod private {
    use enum_dispatch::enum_dispatch;

    use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;

    #[enum_dispatch]
    pub trait DbWrapper {
        fn db_wrapper(&self) -> &DatabaseColumnWrapper;
    }
}

/// Base trait for all field indexes, meant to be implemented only once per indexing approach
#[enum_dispatch]
pub trait PayloadFieldIndex: private::DbWrapper {
    /// Return number of points with at least one value indexed in here
    fn count_indexed_points(&self) -> usize;

    /// Load index from disk.
    fn load(&mut self) -> OperationResult<bool>;

    /// Remove db content of the current payload index
    fn clear(&self) -> OperationResult<()> {
        Self::db_wrapper(self).remove_column_family()
    }

    /// Return function that flushes all pending updates to disk.
    fn flusher(&self) -> Flusher {
        self.db_wrapper().flusher()
    }

    fn recreate(&self) -> OperationResult<()> {
        self.db_wrapper().recreate_column_family()
    }

    fn get_telemetry_data(&self) -> PayloadIndexTelemetry;

    fn values_count(&self, point_id: PointOffsetType) -> usize;

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool;
}

/// Main trait for all concrete field indexes, allowing polymorphic implementations for them.
/// Depending on the specific field type they act on.
#[enum_dispatch]
pub trait PayloadFieldIndexExt: PayloadFieldIndex {
    /// Get iterator over points fitting given `condition`
    /// Return `None` if condition does not match the index type
    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>;

    /// Return estimation of points amount which satisfy given condition
    fn estimate_cardinality(&self, condition: &FieldCondition) -> Option<CardinalityEstimation>;

    /// Iterate conditions for payload blocks with minimum size of `threshold`
    /// Required for building HNSW index
    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_>;
}

// enum_dispatch won't work with associated types (nor generics), because it would be
// impossible to implement it as a trait with a specific associated type on the dispatcher enum.
// Instead, we add these methods as part of the dispatcher enum implementation.
pub trait ValueIndexer {
    type Value;

    /// Add multiple values associated with a single point
    /// This function should be called only once for each point
    fn add_many(&mut self, id: PointOffsetType, values: Vec<Self::Value>) -> OperationResult<()>;

    /// Extract index-able value from payload `Value`
    fn get_value(&self, value: &Value) -> Option<Self::Value>;

    /// Try to extract index-able values from payload `Value`, even if it is an array
    fn get_values(&self, value: &Value) -> Vec<Self::Value> {
        match value {
            Value::Array(values) => values.iter().flat_map(|x| self.get_value(x)).collect(),
            _ => self.get_value(value).map(|x| vec![x]).unwrap_or_default(),
        }
    }

    /// Add point with payload to index
    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &MultiValue<&Value>,
    ) -> OperationResult<()> {
        match payload {
            MultiValue::Multiple(values) => {
                self.remove_point(id)?;
                let mut flatten_values: Vec<_> = vec![];

                for value in values {
                    match value {
                        Value::Array(values) => {
                            flatten_values.extend(values.iter().flat_map(|x| self.get_value(x)));
                        }
                        _ => {
                            if let Some(x) = self.get_value(value) {
                                flatten_values.push(x);
                            }
                        }
                    }
                }
                self.add_many(id, flatten_values)
            }
            MultiValue::Single(Some(Value::Array(values))) => {
                self.remove_point(id)?;
                self.add_many(id, values.iter().flat_map(|x| self.get_value(x)).collect())
            }
            MultiValue::Single(Some(value)) => {
                if let Some(x) = self.get_value(value) {
                    self.remove_point(id)?;
                    self.add_many(id, vec![x])
                } else {
                    Ok(())
                }
            }
            MultiValue::Single(None) => Ok(()),
        }
    }

    /// remove a point from the index
    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()>;
}

/// Common interface for all possible types of field indexes
/// Enables polymorphism on field indexes
/// TODO: Rename with major release
#[enum_dispatch(PayloadFieldIndex, PayloadFieldIndexExt, DbWrapper)]
#[allow(clippy::enum_variant_names)]
pub enum FieldIndex {
    IntIndex(NumericIndex<IntPayloadType>),
    IntMapIndex(MapIndex<IntPayloadType>),
    KeywordIndex(MapIndex<String>),
    FloatIndex(NumericIndex<FloatPayloadType>),
    GeoIndex(GeoMapIndex),
    FullTextIndex(FullTextIndex),
    BinaryIndex(BinaryIndex),
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
            FieldIndex::IntMapIndex(_) => None,
            FieldIndex::KeywordIndex(_) => None,
            FieldIndex::FloatIndex(_) => None,
            FieldIndex::GeoIndex(_) => None,
            FieldIndex::BinaryIndex(_) => None,
            FieldIndex::FullTextIndex(full_text_index) => match &condition.r#match {
                Some(Match::Text(MatchText { text })) => {
                    let query = full_text_index.parse_query(text);
                    for value in full_text_index.get_values(payload_value) {
                        let document = full_text_index.parse_document(&value);
                        if query.check_match(&document) {
                            return Some(true);
                        }
                    }
                    Some(false)
                }
                _ => None,
            },
        }
    }

    pub fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &MultiValue<&Value>,
    ) -> OperationResult<()> {
        match self {
            FieldIndex::IntIndex(ref mut payload_field_index) => {
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
            FieldIndex::BinaryIndex(ref mut payload_field_index) => {
                payload_field_index.add_point(id, payload)
            }
            FieldIndex::FullTextIndex(ref mut payload_field_index) => {
                payload_field_index.add_point(id, payload)
            }
        }
    }

    pub fn remove_point(&mut self, point_id: PointOffsetType) -> OperationResult<()> {
        match self {
            FieldIndex::IntIndex(index) => index.remove_point(point_id),
            FieldIndex::IntMapIndex(index) => index.remove_point(point_id),
            FieldIndex::KeywordIndex(index) => index.remove_point(point_id),
            FieldIndex::FloatIndex(index) => index.remove_point(point_id),
            FieldIndex::GeoIndex(index) => index.remove_point(point_id),
            FieldIndex::BinaryIndex(index) => index.remove_point(point_id),
            FieldIndex::FullTextIndex(index) => index.remove_point(point_id),
        }
    }
}
