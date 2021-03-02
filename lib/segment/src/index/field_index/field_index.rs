use serde::{Deserialize, Serialize};
use crate::index::field_index::CardinalityEstimation;
use crate::index::field_index::map_index::PersistedMapIndex;
use crate::index::field_index::numeric_index::PersistedNumericIndex;
use crate::types::{Condition, FloatPayloadType, IntPayloadType, PayloadType, PointOffsetType};

pub trait PayloadFieldIndex {
    /// Get iterator over points fitting given `condition`
    fn filter(&self, condition: &Condition) -> Box<dyn Iterator<Item=PointOffsetType> + '_>;

    fn estimate_cardinality(&self, condition: &Condition) -> Option<CardinalityEstimation>;

}

pub trait PayloadFieldIndexBuilder {
    fn add(&mut self, id: PointOffsetType, value: &PayloadType);

    fn build(&mut self) -> FieldIndex;
}


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

    fn filter(&self, condition: &Condition) -> Box<dyn Iterator<Item=usize>> {
        self.get_payload_field_index().filter(condition)
    }

    fn estimate_cardinality(&self, condition: &Condition) -> Option<CardinalityEstimation> {
        self.get_payload_field_index().estimate_cardinality(condition)
    }
}