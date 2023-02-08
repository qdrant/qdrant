use bitvec::vec::BitVec;
use quantization::encoder::{EncodedQuery, EncodedVectors, Storage};

use super::{RawScorer, ScoredPointOffset};
use crate::data_types::vectors::VectorElementType;
use crate::types::{PointOffsetType, ScoreType};

pub trait QuantizedVectorStorage: Send + Sync {
    fn raw_scorer<'a>(
        &'a self,
        query: &[VectorElementType],
        deleted: &'a BitVec,
    ) -> Box<dyn RawScorer + 'a>;
}

impl<TStorage> QuantizedVectorStorage for EncodedVectors<TStorage>
where
    TStorage: Storage + Send + Sync,
{
    fn raw_scorer<'a>(
        &'a self,
        query: &[VectorElementType],
        deleted: &'a BitVec,
    ) -> Box<dyn RawScorer + 'a> {
        let query = self.encode_query(query);
        Box::new(QuantizedRawScorer {
            query,
            deleted,
            quantized_data: self,
        })
    }
}

pub struct QuantizedRawScorer<'a, TStorage>
where
    TStorage: Storage,
{
    pub query: EncodedQuery,
    pub deleted: &'a BitVec,
    pub quantized_data: &'a EncodedVectors<TStorage>,
}

impl<TStorage> RawScorer for QuantizedRawScorer<'_, TStorage>
where
    TStorage: Storage,
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
}
