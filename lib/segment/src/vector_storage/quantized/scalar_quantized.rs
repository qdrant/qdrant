use std::path::{Path, PathBuf};

use bitvec::prelude::BitSlice;
use quantization::EncodedVectors;

use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::OperationResult;
use crate::spaces::tools::peek_top_largest_iterable;
use crate::types::{Distance, PointOffsetType, ScoreType};
use crate::vector_storage::quantized::quantized_vectors_base::QuantizedVectors;
use crate::vector_storage::{RawScorer, ScoredPointOffset};

pub const QUANTIZED_DATA_PATH: &str = "quantized.data";
pub const QUANTIZED_META_PATH: &str = "quantized.meta.json";

pub struct ScalarQuantizedRawScorer<'a, TEncodedQuery, TEncodedVectors>
where
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    query: TEncodedQuery,
    deleted: &'a BitSlice,
    // Total number of vectors including deleted ones
    quantized_data: &'a TEncodedVectors,
}

impl<TEncodedQuery, TEncodedVectors> RawScorer
    for ScalarQuantizedRawScorer<'_, TEncodedQuery, TEncodedVectors>
where
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoredPointOffset]) -> usize {
        let mut size: usize = 0;
        for point_id in points.iter().copied() {
            if !self.check_point(point_id) {
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
        let scores = points.filter(|idx| self.check_point(*idx)).map(|idx| {
            let score = self.score_point(idx);
            ScoredPointOffset { idx, score }
        });
        peek_top_largest_iterable(scores, top)
    }

    fn peek_top_all(&self, top: usize) -> Vec<ScoredPointOffset> {
        let scores = (0..self.deleted.len() as PointOffsetType)
            .filter(|idx| self.check_point(*idx))
            .map(|idx| {
                let score = self.score_point(idx);
                ScoredPointOffset { idx, score }
            });
        peek_top_largest_iterable(scores, top)
    }
}

pub struct ScalarQuantizedVectors<TStorage: quantization::EncodedStorage + Send + Sync> {
    storage: quantization::EncodedVectorsU8<TStorage>,
    distance: Distance,
}

impl<TStorage: quantization::EncodedStorage + Send + Sync> ScalarQuantizedVectors<TStorage> {
    pub fn new(storage: quantization::EncodedVectorsU8<TStorage>, distance: Distance) -> Self {
        Self { storage, distance }
    }
}

impl<TStorage> QuantizedVectors for ScalarQuantizedVectors<TStorage>
where
    TStorage: quantization::EncodedStorage + Send + Sync,
{
    fn raw_scorer<'a>(
        &'a self,
        query: &[VectorElementType],
        deleted: &'a BitSlice,
    ) -> Box<dyn RawScorer + 'a> {
        let query = self
            .distance
            .preprocess_vector(query)
            .unwrap_or_else(|| query.to_vec());
        let query = self.storage.encode_query(&query);
        Box::new(ScalarQuantizedRawScorer {
            query,
            deleted,
            quantized_data: &self.storage,
        })
    }

    fn save_to(&self, path: &Path) -> OperationResult<()> {
        let data_path = path.join(QUANTIZED_DATA_PATH);
        let meta_path = path.join(QUANTIZED_META_PATH);
        self.storage.save(&data_path, &meta_path)?;
        Ok(())
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![QUANTIZED_DATA_PATH.into(), QUANTIZED_META_PATH.into()]
    }
}
