use crate::types::{PointOffsetType, ScoreType, VectorElementType, Distance};
use std::cmp::{Ordering};
use ordered_float::OrderedFloat;
use crate::entry::entry_point::OperationResult;
use std::ops::Range;


#[derive(Copy, Clone, PartialEq, Debug)]
pub struct ScoredPointOffset {
    pub idx: PointOffsetType,
    pub score: ScoreType
}

impl Eq for ScoredPointOffset {}

impl Ord for ScoredPointOffset {
    fn cmp(&self, other: &Self) -> Ordering {
        OrderedFloat(self.score).cmp(&OrderedFloat(other.score))
    }
}

impl PartialOrd for ScoredPointOffset {
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
    fn deleted_count(&self) -> usize; /// Number of vectors, marked as deleted but still stored
    fn get_vector(&self, key: PointOffsetType) -> Option<Vec<VectorElementType>>;
    fn put_vector(&mut self, vector: &Vec<VectorElementType>) -> OperationResult<PointOffsetType>;
    fn update_vector(&mut self, key: PointOffsetType, vector: &Vec<VectorElementType>) -> OperationResult<PointOffsetType>;
    fn update_from(&mut self, other: &dyn VectorStorage) -> OperationResult<Range<PointOffsetType>>;
    fn delete(&mut self, key: PointOffsetType) -> OperationResult<()>;
    fn iter_ids(&self) -> Box<dyn Iterator<Item=PointOffsetType> + '_>;
    fn flush(&self) -> OperationResult<usize>;

    fn score_points(
        &self,
        vector: &Vec<VectorElementType>,
        points: &[PointOffsetType],
        top: usize,
        distance: &Distance,
    ) -> Vec<ScoredPointOffset>;
    fn score_all(
        &self,
        vector: &Vec<VectorElementType>,
        top: usize,
        distance: &Distance
    ) -> Vec<ScoredPointOffset>;
    fn score_internal(
        &self,
        point: PointOffsetType,
        points: &[PointOffsetType],
        top: usize,
        distance: &Distance
    ) -> Vec<ScoredPointOffset>;
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ordering() {
        assert!(ScoredPointOffset { idx: 10, score: 0.9} > ScoredPointOffset { idx: 20, score: 0.6})
    }
}