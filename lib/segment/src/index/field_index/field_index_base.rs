use crate::index::field_index::map_index::PersistedMapIndex;
use crate::index::field_index::numeric_index::PersistedNumericIndex;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::types::{
    FieldCondition, FloatPayloadType, IntPayloadType, PayloadKeyType, PayloadType, PointOffsetType,
};
use serde::{Deserialize, Serialize};

pub trait PayloadFieldIndex {
    /// Get iterator over points fitting given `condition`
    fn filter(
        &self,
        condition: &FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + '_>>;

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

pub trait PayloadFieldIndexBuilder {
    fn add(&mut self, id: PointOffsetType, value: &PayloadType);

    fn build(&mut self) -> FieldIndex;
}

/// Common interface for all possible types of field indexes
/// Enables polymorphism on field indexes
/// ToDo: Rename with major release
#[allow(clippy::enum_variant_names)]
#[derive(Serialize, Deserialize)]
pub enum FieldIndex {
    IntIndex(PersistedNumericIndex<IntPayloadType>),
    IntMapIndex(PersistedMapIndex<IntPayloadType>),
    KeywordIndex(PersistedMapIndex<String>),
    FloatIndex(PersistedNumericIndex<FloatPayloadType>),
}

impl FieldIndex {
    pub fn get_payload_field_index(&self) -> &dyn PayloadFieldIndex {
        match self {
            FieldIndex::IntIndex(payload_field_index) => payload_field_index,
            FieldIndex::IntMapIndex(payload_field_index) => payload_field_index,
            FieldIndex::KeywordIndex(payload_field_index) => payload_field_index,
            FieldIndex::FloatIndex(payload_field_index) => payload_field_index,
        }
    }
}

impl PayloadFieldIndex for FieldIndex {
    fn filter(
        &self,
        condition: &FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        self.get_payload_field_index().filter(condition)
    }

    fn estimate_cardinality(&self, condition: &FieldCondition) -> Option<CardinalityEstimation> {
        self.get_payload_field_index()
            .estimate_cardinality(condition)
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        self.get_payload_field_index()
            .payload_blocks(threshold, key)
    }
}
