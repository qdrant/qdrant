use crate::spaces::metric::Metric;
use crate::types::{PointOffsetType, ScoreType};
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
pub trait VectorStorage<El: Clone> {
    fn vector_count(&self) -> PointOffsetType;
    fn get_vector(&self, key: PointOffsetType) -> Option<Vec<El>>;
    fn put_vector(&mut self, vector: &Vec<El>) -> PointOffsetType;
}

pub trait VectorMatcher<El> {
    fn score_points(
        &self,
        vector: &Vec<El>,
        points: &[PointOffsetType],
        top: usize
    ) -> Vec<ScoredPoint>;
    fn score_all(&self, vector: &Vec<El>, top: usize) -> Vec<ScoredPoint>;
    fn score_internal(
        &self,
        point: PointOffsetType,
        points: &[PointOffsetType],
        top: usize
    ) -> Vec<ScoredPoint>;
}
