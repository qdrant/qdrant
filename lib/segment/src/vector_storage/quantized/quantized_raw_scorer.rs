use std::sync::atomic::{AtomicBool, Ordering};

use bitvec::prelude::BitSlice;

use crate::spaces::tools::peek_top_largest_iterable;
use crate::types::{PointOffsetType, ScoreType};
use crate::vector_storage::{RawScorer, ScoredPointOffset};

pub struct QuantizedRawScorer<'a, TEncodedQuery, TEncodedVectors>
where
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    pub(super) query: TEncodedQuery,
    /// [`BitSlice`] defining flags for deleted points (and thus these vectors).
    pub(super) point_deleted: &'a BitSlice,
    /// [`BitSlice`] defining flags for deleted vectors in this segment.
    pub(super) vec_deleted: &'a BitSlice,
    pub quantized_data: &'a TEncodedVectors,
    /// This flag indicates that the search process is stopped externally,
    /// the search result is no longer needed and the search process should be stopped as soon as possible.
    pub is_stopped: &'a AtomicBool,
}

impl<TEncodedQuery, TEncodedVectors> RawScorer
    for QuantizedRawScorer<'_, TEncodedQuery, TEncodedVectors>
where
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoredPointOffset]) -> usize {
        if self.is_stopped.load(Ordering::Relaxed) {
            return 0;
        }
        let mut size: usize = 0;
        for point_id in points.iter().copied() {
            if !self.check_vector(point_id) {
                continue;
            }
            scores[size] = ScoredPointOffset {
                idx: point_id,
                score: self.quantized_data.score_point(&self.query, point_id),
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
        if self.is_stopped.load(Ordering::Relaxed) {
            return vec![];
        }
        let mut scores = vec![];
        for point in points {
            scores.push(ScoredPointOffset {
                idx: point,
                score: self.quantized_data.score_point(&self.query, point),
            });
        }
        scores
    }

    fn check_vector(&self, point: PointOffsetType) -> bool {
        // Deleted points propagate to vectors; check vector deletion for possible early return
        !self
            .vec_deleted
            .get(point as usize)
            .as_deref()
            .copied()
            // Default to not deleted if our deleted flags failed grow
            .unwrap_or(false)
        // Additionally check point deletion for integrity if delete propagation to vector failed
        && !self
            .point_deleted
            .get(point as usize)
            .as_deref()
            .copied()
            // Default to deleted if the point mapping was removed from the ID tracker
            .unwrap_or(true)
    }

    fn score_point(&self, point: PointOffsetType) -> ScoreType {
        self.quantized_data.score_point(&self.query, point)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.quantized_data.score_internal(point_a, point_b)
    }

    fn peek_top_iter(
        &self,
        points: &mut dyn Iterator<Item = PointOffsetType>,
        top: usize,
    ) -> Vec<ScoredPointOffset> {
        let scores = points
            .take_while(|_| !self.is_stopped.load(Ordering::Relaxed))
            .filter(|idx| self.check_vector(*idx))
            .map(|idx| {
                let score = self.score_point(idx);
                ScoredPointOffset { idx, score }
            });
        peek_top_largest_iterable(scores, top)
    }

    fn peek_top_all(&self, top: usize) -> Vec<ScoredPointOffset> {
        let scores = (0..self.point_deleted.len() as PointOffsetType)
            .take_while(|_| !self.is_stopped.load(Ordering::Relaxed))
            .filter(|idx| self.check_vector(*idx))
            .map(|idx| {
                let score = self.score_point(idx);
                ScoredPointOffset { idx, score }
            });
        peek_top_largest_iterable(scores, top)
    }
}
