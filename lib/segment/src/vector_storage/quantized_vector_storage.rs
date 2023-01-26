use bitvec::vec::BitVec;

use super::{RawScorer, ScoredPointOffset};
use crate::types::{PointOffsetType, ScoreType};

pub type EncodedVectors = quantization::encoder::EncodedVectors<Vec<u8>>;

pub struct QuantizedRawScorer<'a> {
    pub query: quantization::encoder::EncodedQuery,
    pub deleted: &'a BitVec,
    pub quantized_data: &'a EncodedVectors,
}

impl RawScorer for QuantizedRawScorer<'_> {
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
}
