use bitvec::prelude::BitVec;

use crate::spaces::tools::peek_top_largest_iterable;
use crate::types::{PointOffsetType, ScoreType};
use crate::vector_storage::{RawScorer, ScoredPointOffset};

pub struct QuantizedRawScorer<'a, TEncodedQuery, TEncodedVectors>
where
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    pub query: TEncodedQuery,
    pub deleted: &'a BitVec,
    // Total number of vectors including deleted ones
    pub quantized_data: &'a TEncodedVectors,
}

impl<TEncodedQuery, TEncodedVectors> RawScorer
    for QuantizedRawScorer<'_, TEncodedQuery, TEncodedVectors>
where
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoredPointOffset]) -> usize {
        let mut size: usize = 0;
        for point_id in points.iter().copied() {
            if self.deleted[point_id as usize] {
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

    fn check_point(&self, point: PointOffsetType) -> bool {
        (point as usize) < self.deleted.len() && !self.deleted[point as usize]
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
            .filter(|idx| !self.deleted[*idx as usize])
            .map(|idx| {
                let score = self.score_point(idx);
                ScoredPointOffset { idx, score }
            });
        peek_top_largest_iterable(scores, top)
    }

    fn peek_top_all(&self, top: usize) -> Vec<ScoredPointOffset> {
        let scores = (0..self.deleted.len() as PointOffsetType)
            .filter(|idx| !self.deleted[*idx as usize])
            .map(|idx| {
                let score = self.score_point(idx);
                ScoredPointOffset { idx, score }
            });
        peek_top_largest_iterable(scores, top)
    }
}
