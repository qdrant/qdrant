use crate::types::{PointOffsetType, Condition, PayloadType};
use crate::index::field_index::{CardinalityEstimation, FieldIndex};

pub trait PayloadFieldIndex {
    /// Get iterator over points fitting given `condition`
    fn filter(&self, condition: &Condition) -> Box<dyn Iterator<Item=PointOffsetType> + '_>;

    fn estimate_condition_cardinality(&self, condition: &Condition) -> Option<CardinalityEstimation>;

    /// Estimate number of points fitting given `condition`
    fn estimate_cardinality(&self, condition: &Condition) -> Option<CardinalityEstimation> {
        match condition {
            Condition::Filter(_) => None,  // Can't estimate nested here
            Condition::HasId(ids) => Some(CardinalityEstimation {
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
