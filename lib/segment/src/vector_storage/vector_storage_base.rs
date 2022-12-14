use std::cmp::Ordering;
use std::ops::Range;
use std::sync::atomic::AtomicBool;

use ordered_float::OrderedFloat;
use rand::Rng;

use crate::common::Flusher;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::OperationResult;
use crate::types::{PointOffsetType, ScoreType};

#[derive(Copy, Clone, PartialEq, Debug, Default)]
pub struct ScoredPointOffset {
    pub idx: PointOffsetType,
    pub score: ScoreType,
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
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoredPointOffset]) -> usize;

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
/// Storage operates with internal IDs (`PointOffsetType`), which always starts with zero and have no skips
pub trait VectorStorage {
    fn vector_dim(&self) -> usize;
    fn vector_count(&self) -> usize;
    /// Number of searchable vectors (not deleted)
    fn deleted_count(&self) -> usize;
    /// Number of vectors, marked as deleted but still stored
    fn total_vector_count(&self) -> usize;
    /// Number of all stored vectors including deleted
    fn get_vector(&self, key: PointOffsetType) -> Option<Vec<VectorElementType>>;
    fn put_vector(&mut self, vector: Vec<VectorElementType>) -> OperationResult<PointOffsetType>;
    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: Vec<VectorElementType>,
    ) -> OperationResult<()>;
    /// Returns next available id
    fn next_id(&self) -> PointOffsetType;
    fn update_from(
        &mut self,
        other: &VectorStorageSS,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>>;
    fn delete(&mut self, key: PointOffsetType) -> OperationResult<()>;
    fn is_deleted(&self, key: PointOffsetType) -> bool;
    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_>;
    fn flusher(&self) -> Flusher;

    /// Generate a `RawScorer` object which contains all required context for searching similar vector
    fn raw_scorer(&self, vector: Vec<VectorElementType>) -> Box<dyn RawScorer + '_>;

    fn score_points(
        &self,
        vector: &[VectorElementType],
        points: &mut dyn Iterator<Item = PointOffsetType>,
        top: usize,
    ) -> Vec<ScoredPointOffset>;
    fn score_all(&self, vector: &[VectorElementType], top: usize) -> Vec<ScoredPointOffset>;
    fn score_internal(
        &self,
        point: PointOffsetType,
        points: &mut dyn Iterator<Item = PointOffsetType>,
        top: usize,
    ) -> Vec<ScoredPointOffset>;

    /// Iterator over `n` random ids which are not deleted
    fn sample_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        let total = self.total_vector_count() as PointOffsetType;
        let mut rng = rand::thread_rng();
        Box::new(
            (0..total)
                .map(move |_| rng.gen_range(0..total))
                .filter(move |x| !self.is_deleted(*x)),
        )
    }
}

trait SuperVectorStorage {}

pub type VectorStorageSS = dyn VectorStorage + Sync + Send;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ordering() {
        assert!(
            ScoredPointOffset {
                idx: 10,
                score: 0.9
            } > ScoredPointOffset {
                idx: 20,
                score: 0.6
            }
        )
    }
}
