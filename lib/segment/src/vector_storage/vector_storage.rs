use crate::types::{PointOffsetType, ScoreType, VectorElementType, Distance};
use std::cmp::{Ordering, Reverse};
use ordered_float::OrderedFloat;


#[derive(Copy, Clone, PartialEq, Debug)]
pub struct ScoredPoint {
    pub idx: PointOffsetType,
    pub score: ScoreType
}

impl ScoredPoint {
    pub fn to_tuple(&self) -> (PointOffsetType, ScoreType) {
        (self.idx, self.score)
    }
}

impl Eq for ScoredPoint {}

impl Ord for ScoredPoint {
    fn cmp(&self, other: &Self) -> Ordering {
        Reverse(OrderedFloat(other.score)).cmp(&Reverse(OrderedFloat(self.score)))
    }
}

impl PartialOrd for ScoredPoint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}


/// Trait for vector storage
/// El - type of vector element, expected numerical type
/// Storage operates with internal IDs (PointOffsetType), which always starts with zero and have no skips
pub trait VectorStorage {
    fn vector_dim(&self) -> usize;
    fn vector_count(&self) -> usize;
    fn get_vector(&self, key: PointOffsetType) -> Option<Vec<VectorElementType>>;
    fn put_vector(&mut self, vector: &Vec<VectorElementType>) -> PointOffsetType;
    fn delete(&mut self, key: PointOffsetType);
}

pub trait VectorCounter {
    fn vector_count(&self) -> PointOffsetType;
}

pub trait VectorMatcher {
    fn score_points(
        &self,
        vector: &Vec<VectorElementType>,
        points: &[PointOffsetType],
        top: usize,
        distance: &Distance
    ) -> Vec<ScoredPoint>;
    fn score_all(
        &self,
        vector: &Vec<VectorElementType>,
        top: usize,
        distance: &Distance
    ) -> Vec<ScoredPoint>;
    fn score_internal(
        &self,
        point: PointOffsetType,
        points: &[PointOffsetType],
        top: usize,
        distance: &Distance
    ) -> Vec<ScoredPoint>;
}
