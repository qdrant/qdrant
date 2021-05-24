use crate::types::{PointOffsetType, ScoreType, VectorElementType};
use std::cmp::{Ordering};
use ordered_float::OrderedFloat;
use crate::entry::entry_point::OperationResult;
use std::ops::Range;
use rand::Rng;


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


/// Optimized scorer for multiple scoring requests comparing with a single query
/// Holds current query and params, receives only subset of points to score
pub trait RawScorer {
    // ToDo: Replace boxed iterator with callback and make a benchmark (-4% on benchmarks, but ugly)
    fn score_points<'a>(&'a self, points: &'a mut dyn Iterator<Item=PointOffsetType>) -> Box<dyn Iterator<Item=ScoredPointOffset> + 'a>;
    /// Return true if point satisfies current search context (exists and not deleted)
    fn check_point(&self, point: PointOffsetType) -> bool;
    /// Score stored vector with vector under the given index
    fn score_point(&self, point: PointOffsetType) -> ScoreType;

    /// Return distance between stored points selected by ids
    /// Panics if any id is out of range
    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType;
}

/// Trait for vector storage
/// El - type of vector element, expected numerical type
/// Storage operates with internal IDs (PointOffsetType), which always starts with zero and have no skips
pub trait VectorStorage {
    fn vector_dim(&self) -> usize;
    fn vector_count(&self) -> usize; /// Number of searchable vectors (not deleted)
    fn deleted_count(&self) -> usize; /// Number of vectors, marked as deleted but still stored
    fn total_vector_count(&self) -> usize; /// Number of all stored vectors including deleted
    fn get_vector(&self, key: PointOffsetType) -> Option<Vec<VectorElementType>>;
    fn put_vector(&mut self, vector: Vec<VectorElementType>) -> OperationResult<PointOffsetType>;
    fn update_vector(&mut self, key: PointOffsetType, vector: Vec<VectorElementType>) -> OperationResult<PointOffsetType>;
    fn update_from(&mut self, other: &dyn VectorStorage) -> OperationResult<Range<PointOffsetType>>;
    fn delete(&mut self, key: PointOffsetType) -> OperationResult<()>;
    fn is_deleted(&self, key: PointOffsetType) -> bool;
    fn iter_ids(&self) -> Box<dyn Iterator<Item=PointOffsetType> + '_>; /// Iterator over not-deleted ids
    fn flush(&self) -> OperationResult<()>;

    /// Generate a RawScorer object which contains all required context for searching similar vector
    fn raw_scorer(&self, vector: Vec<VectorElementType>) -> Box<dyn RawScorer + '_>;
    /// Same as `raw_scorer` but uses internal vector for search, avoids double pre-processing
    fn raw_scorer_internal(&self, point_id: PointOffsetType) -> Box<dyn RawScorer + '_>;


    fn score_points(
        &self,
        vector: &Vec<VectorElementType>,
        points: &mut dyn Iterator<Item=PointOffsetType>,
        top: usize
    ) -> Vec<ScoredPointOffset>;
    fn score_all(
        &self,
        vector: &Vec<VectorElementType>,
        top: usize
    ) -> Vec<ScoredPointOffset>;
    fn score_internal(
        &self,
        point: PointOffsetType,
        points: &mut dyn Iterator<Item=PointOffsetType>,
        top: usize
    ) -> Vec<ScoredPointOffset>;

    /// Iterator over `n` random ids which are not deleted
    fn sample_ids(&self) -> Box<dyn Iterator<Item=PointOffsetType> + '_> {
        let total = self.total_vector_count() as PointOffsetType;
        let mut rng = rand::thread_rng();
        Box::new((0..total)
            .map(move |_| rng.gen_range(0..total))
            .filter(move |x| !self.is_deleted(*x))
        )
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ordering() {
        assert!(ScoredPointOffset { idx: 10, score: 0.9} > ScoredPointOffset { idx: 20, score: 0.6})
    }
}