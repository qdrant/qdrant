use serde::{Deserialize, Serialize};
use crate::index::field_index::CardinalityEstimation;
use crate::index::field_index::map_index::PersistedMapIndex;
use crate::index::field_index::numeric_index::PersistedNumericIndex;
use crate::types::{Condition, FloatPayloadType, IntPayloadType, PayloadType, PointOffsetType};

pub trait PayloadFieldIndex {
    /// Get iterator over points fitting given `condition`
    fn filter(&self, condition: &Condition) -> Box<dyn Iterator<Item=PointOffsetType> + '_>;

    fn estimate_condition_cardinality(&self, condition: &Condition) -> Option<CardinalityEstimation>;

    /// Estimate number of points fitting given `condition`
    fn estimate_cardinality(&self, condition: &Condition) -> Option<CardinalityEstimation> {
        match condition {
            Condition::Filter(_) => None,  // Can't estimate nested here
            Condition::HasId(ids) => Some(CardinalityEstimation {
                primary_clauses: vec![Condition::HasId(ids.clone())],
                min: 0,
                exp: ids.len(),
                max: ids.len()
            }),
            _ => self.estimate_condition_cardinality(condition)
        }
    }
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

impl PayloadFieldIndex for FieldIndex {

    fn filter(&self, condition: &Condition) -> Box<dyn Iterator<Item=usize>> {
        unimplemented!()
    }

    fn estimate_condition_cardinality(&self, condition: &Condition) -> Option<CardinalityEstimation> {
        unimplemented!()
    }
}