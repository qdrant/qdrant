use std::marker::PhantomData;

use bitvec::vec::BitVec;

use super::{ScoredPointOffset, VectorStorage, VectorStorageEnum};
use crate::data_types::vectors::VectorElementType;
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric};
use crate::spaces::tools::peek_top_largest_iterable;
use crate::types::{Distance, PointOffsetType, ScoreType};

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

    fn peek_top_iter(
        &self,
        points: &mut dyn Iterator<Item = PointOffsetType>,
        top: usize,
    ) -> Vec<ScoredPointOffset>;

    fn peek_top_all(&self, top: usize) -> Vec<ScoredPointOffset>;
}

pub struct RawScorerImpl<'a, TMetric: Metric, TVectorStorage: VectorStorage> {
    pub points_count: PointOffsetType,
    pub query: Vec<VectorElementType>,
    pub vector_storage: &'a TVectorStorage,
    pub deleted: &'a BitVec,
    pub metric: PhantomData<TMetric>,
}

pub fn new_raw_scorer<'a>(
    vector: Vec<VectorElementType>,
    vector_storage: &'a VectorStorageEnum,
    deleted: &'a BitVec,
) -> Box<dyn RawScorer + 'a> {
    match vector_storage {
        VectorStorageEnum::Simple(vector_storage) => {
            raw_scorer_impl(vector, vector_storage, deleted)
        }
        VectorStorageEnum::Memmap(vector_storage) => {
            raw_scorer_impl(vector, vector_storage.as_ref(), deleted)
        }
    }
}

fn raw_scorer_impl<'a, TVectorStorage: VectorStorage>(
    vector: Vec<VectorElementType>,
    vector_storage: &'a TVectorStorage,
    deleted: &'a BitVec,
) -> Box<dyn RawScorer + 'a> {
    let points_count = vector_storage.total_vector_count() as PointOffsetType;
    match vector_storage.distance() {
        Distance::Cosine => Box::new(RawScorerImpl::<'a, CosineMetric, TVectorStorage> {
            points_count,
            query: CosineMetric::preprocess(&vector).unwrap_or(vector),
            vector_storage,
            deleted,
            metric: PhantomData,
        }),
        Distance::Euclid => Box::new(RawScorerImpl::<'a, EuclidMetric, TVectorStorage> {
            points_count,
            query: EuclidMetric::preprocess(&vector).unwrap_or(vector),
            vector_storage,
            deleted,
            metric: PhantomData,
        }),
        Distance::Dot => Box::new(RawScorerImpl::<'a, DotProductMetric, TVectorStorage> {
            points_count,
            query: DotProductMetric::preprocess(&vector).unwrap_or(vector),
            vector_storage,
            deleted,
            metric: PhantomData,
        }),
    }
}

impl<'a, TMetric, TVectorStorage> RawScorer for RawScorerImpl<'a, TMetric, TVectorStorage>
where
    TMetric: Metric,
    TVectorStorage: VectorStorage,
{
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoredPointOffset]) -> usize {
        let mut size: usize = 0;
        for point_id in points.iter().copied() {
            if self.deleted[point_id as usize] {
                continue;
            }
            let other_vector = self.vector_storage.get_vector(point_id);
            scores[size] = ScoredPointOffset {
                idx: point_id,
                score: TMetric::similarity(&self.query, other_vector),
            };

            size += 1;
            if size == scores.len() {
                return size;
            }
        }
        size
    }

    fn check_point(&self, point: PointOffsetType) -> bool {
        point < self.points_count && !self.deleted[point as usize]
    }

    fn score_point(&self, point: PointOffsetType) -> ScoreType {
        let other_vector = self.vector_storage.get_vector(point);
        TMetric::similarity(&self.query, other_vector)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        let vector_a = self.vector_storage.get_vector(point_a);
        let vector_b = self.vector_storage.get_vector(point_b);
        TMetric::similarity(vector_a, vector_b)
    }

    fn peek_top_iter(
        &self,
        points: &mut dyn Iterator<Item = PointOffsetType>,
        top: usize,
    ) -> Vec<ScoredPointOffset> {
        let scores = points
            .filter(|point_id| !self.deleted[*point_id as usize])
            .map(|point_id| {
                let other_vector = self.vector_storage.get_vector(point_id);
                ScoredPointOffset {
                    idx: point_id,
                    score: TMetric::similarity(&self.query, other_vector),
                }
            });
        peek_top_largest_iterable(scores, top)
    }

    fn peek_top_all(&self, top: usize) -> Vec<ScoredPointOffset> {
        let scores = (0..self.points_count)
            .filter(|point_id| self.check_point(*point_id))
            .map(|point_id| {
                let point_id = point_id as PointOffsetType;
                let other_vector = &self.vector_storage.get_vector(point_id);
                ScoredPointOffset {
                    idx: point_id,
                    score: TMetric::similarity(&self.query, other_vector),
                }
            });
        peek_top_largest_iterable(scores, top)
    }
}
