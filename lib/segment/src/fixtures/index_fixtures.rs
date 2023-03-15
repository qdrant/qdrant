use std::marker::PhantomData;

use bitvec::prelude::BitVec;
use rand::Rng;

use crate::data_types::vectors::VectorElementType;
use crate::payload_storage::FilterContext;
use crate::spaces::metric::Metric;
use crate::spaces::tools::peek_top_largest_iterable;
use crate::types::{PointOffsetType, ScoreType};
use crate::vector_storage::chunked_vectors::ChunkedVectors;
use crate::vector_storage::{RawScorer, ScoredPointOffset};

pub fn random_vector<R: Rng + ?Sized>(rnd_gen: &mut R, size: usize) -> Vec<VectorElementType> {
    (0..size).map(|_| rnd_gen.gen_range(0.0..1.0)).collect()
}

pub struct FakeFilterContext {}

impl FilterContext for FakeFilterContext {
    fn check(&self, _point_id: PointOffsetType) -> bool {
        true
    }
}

pub struct TestRawScorerProducer<TMetric: Metric> {
    pub vectors: ChunkedVectors<VectorElementType>,
    pub deleted: BitVec,
    pub metric: PhantomData<TMetric>,
}

struct TestRawScorer<'a, TMetric: Metric> {
    points_count: PointOffsetType,
    query: Vec<VectorElementType>,
    data: &'a TestRawScorerProducer<TMetric>,
}

impl<'a, TMetric> RawScorer for TestRawScorer<'a, TMetric>
where
    TMetric: Metric,
{
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoredPointOffset]) -> usize {
        let mut size: usize = 0;
        for point_id in points.iter().copied() {
            if self.data.deleted[point_id as usize] {
                continue;
            }
            let other_vector = self.data.vectors.get(point_id);
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
        point < self.points_count && !self.data.deleted[point as usize]
    }

    fn score_point(&self, point: PointOffsetType) -> ScoreType {
        let other_vector = self.data.vectors.get(point);
        TMetric::similarity(&self.query, other_vector)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        let vector_a = self.data.vectors.get(point_a);
        let vector_b = self.data.vectors.get(point_b);
        TMetric::similarity(vector_a, vector_b)
    }

    fn peek_top_iter(
        &self,
        points: &mut dyn Iterator<Item = PointOffsetType>,
        top: usize,
    ) -> Vec<ScoredPointOffset> {
        let scores = points
            .filter(|point_id| !self.data.deleted[*point_id as usize])
            .map(|point_id| {
                let other_vector = self.data.vectors.get(point_id);
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
                let other_vector = &self.data.vectors.get(point_id);
                ScoredPointOffset {
                    idx: point_id,
                    score: TMetric::similarity(&self.query, other_vector),
                }
            });
        peek_top_largest_iterable(scores, top)
    }
}

impl<TMetric> TestRawScorerProducer<TMetric>
where
    TMetric: Metric,
{
    pub fn new<R>(dim: usize, num_vectors: usize, rng: &mut R) -> Self
    where
        R: Rng + ?Sized,
    {
        let mut vectors = ChunkedVectors::new(dim);
        for _ in 0..num_vectors {
            let rnd_vec = random_vector(rng, dim);
            let rnd_vec = TMetric::preprocess(&rnd_vec).unwrap_or(rnd_vec);
            vectors.push(&rnd_vec);
        }
        TestRawScorerProducer::<TMetric> {
            vectors,
            deleted: BitVec::repeat(false, num_vectors),
            metric: PhantomData,
        }
    }

    pub fn get_raw_scorer(&self, query: Vec<VectorElementType>) -> Box<dyn RawScorer + '_> {
        let points_count = self.vectors.len() as PointOffsetType;
        let query = TMetric::preprocess(&query).unwrap_or(query);
        Box::new(TestRawScorer {
            points_count,
            query,
            data: self,
        })
    }
}
