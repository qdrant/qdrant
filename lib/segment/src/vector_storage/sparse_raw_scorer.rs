use std::sync::atomic::{AtomicBool, Ordering};

use bitvec::slice::BitSlice;
use common::types::{PointOffsetType, ScoreType, ScoredPointOffset};
use sparse::common::sparse_vector::SparseVector;

use super::{RawScorer, SparseVectorStorage};
use crate::spaces::tools::peek_top_largest_iterable;

pub struct SparseRawScorer<'a, TVectorStorage: SparseVectorStorage> {
    query: SparseVector,
    vector_storage: &'a TVectorStorage,
    point_deleted: &'a BitSlice,
    vec_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
}

impl<'a, TVectorStorage: SparseVectorStorage> SparseRawScorer<'a, TVectorStorage> {
    pub fn new(
        query: SparseVector,
        vector_storage: &'a TVectorStorage,
        point_deleted: &'a BitSlice,
        vec_deleted: &'a BitSlice,
        is_stopped: &'a AtomicBool,
    ) -> Self {
        Self {
            query,
            vector_storage,
            point_deleted,
            vec_deleted,
            is_stopped,
        }
    }

    /// Scores a single point if it overlaps with the query.
    ///
    /// This keeps the same semantics as the sparse inverted index which does not return a score for points that do not overlap with the query
    ///
    /// Returns None if the point does not overlap with the query
    pub fn score_overlapping_point(&self, point_id: PointOffsetType) -> Option<ScoredPointOffset> {
        let point_id = point_id as PointOffsetType;
        let vector = self.vector_storage.get_sparse(point_id);
        if self.query.overlaps(vector) {
            Some(ScoredPointOffset {
                idx: point_id,
                score: vector.score(&self.query),
            })
        } else {
            None
        }
    }
}

impl<'a, TVectorStorage: SparseVectorStorage> RawScorer for SparseRawScorer<'a, TVectorStorage> {
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoredPointOffset]) -> usize {
        if self.is_stopped.load(Ordering::Relaxed) {
            return 0;
        }
        let mut size: usize = 0;
        for point_id in points.iter().copied() {
            if !self.check_vector(point_id) {
                continue;
            }
            let vector = self.vector_storage.get_sparse(point_id);
            // do not check overlap here as we want to score all points
            scores[size] = ScoredPointOffset {
                idx: point_id,
                score: vector.score(&self.query),
            };

            size += 1;
            if size == scores.len() {
                return size;
            }
        }
        size
    }

    fn score_points_unfiltered(
        &self,
        points: &mut dyn Iterator<Item = PointOffsetType>,
    ) -> Vec<ScoredPointOffset> {
        points
            .take_while(|_| !self.is_stopped.load(Ordering::Relaxed))
            .filter_map(|point_id| self.score_overlapping_point(point_id))
            .collect()
    }

    fn check_vector(&self, point: PointOffsetType) -> bool {
        sparse_check_vector(point, self.point_deleted, self.vec_deleted)
    }

    fn score_point(&self, point: PointOffsetType) -> ScoreType {
        let vector = self.vector_storage.get_sparse(point);
        vector.score(&self.query)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        let vector_a = self.vector_storage.get_sparse(point_a);
        let vector_b = self.vector_storage.get_sparse(point_b);
        vector_a.score(vector_b)
    }

    fn peek_top_iter(
        &self,
        points: &mut dyn Iterator<Item = PointOffsetType>,
        top: usize,
    ) -> Vec<ScoredPointOffset> {
        let scores = points
            .take_while(|_| !self.is_stopped.load(Ordering::Relaxed))
            .filter(|point_id| self.check_vector(*point_id))
            .filter_map(|point_id| self.score_overlapping_point(point_id));
        peek_top_largest_iterable(scores, top)
    }

    fn peek_top_all(&self, top: usize) -> Vec<ScoredPointOffset> {
        let scores = (0..self.point_deleted.len() as PointOffsetType)
            .take_while(|_| !self.is_stopped.load(Ordering::Relaxed))
            .filter(|point_id| self.check_vector(*point_id))
            .filter_map(|point_id| self.score_overlapping_point(point_id));

        peek_top_largest_iterable(scores, top)
    }
}

// TODO(sparse) extracted for reuse but should be unified RawScorerImpl
pub fn sparse_check_vector<'a>(
    point: PointOffsetType,
    point_deleted: &'a BitSlice,
    vec_deleted: &'a BitSlice,
) -> bool {
    // Deleted points propagate to vectors; check vector deletion for possible early return
    !vec_deleted
        .get(point as usize)
        .map(|x| *x)
        // Default to not deleted if our deleted flags failed grow
        .unwrap_or(false)
        // Additionally check point deletion for integrity if delete propagation to vector failed
        && !point_deleted
        .get(point as usize)
        .map(|x| *x)
        // Default to deleted if the point mapping was removed from the ID tracker
        .unwrap_or(true)
}
