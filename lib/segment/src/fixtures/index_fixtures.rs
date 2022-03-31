use crate::payload_storage::{ConditionChecker, FilterContext};
use crate::spaces::metric::Metric;
use crate::types::{Filter, PointOffsetType, VectorElementType};
use crate::vector_storage::simple_vector_storage::SimpleRawScorer;
use bit_vec::BitVec;
use itertools::Itertools;
use rand::Rng;

pub fn random_vector<R: Rng + ?Sized>(rnd_gen: &mut R, size: usize) -> Vec<VectorElementType> {
    (0..size).map(|_| rnd_gen.gen_range(0.0..1.0)).collect()
}

pub struct FakeConditionChecker {}

impl ConditionChecker for FakeConditionChecker {
    fn check(&self, _point_id: PointOffsetType, _query: &Filter) -> bool {
        true
    }
}

pub struct FakeFilterContext {}

impl FilterContext for FakeFilterContext {
    fn check(&self, _point_id: PointOffsetType) -> bool {
        true
    }
}

pub struct TestRawScorerProducer<TMetric: Metric> {
    pub vectors: Vec<Vec<VectorElementType>>,
    pub deleted: BitVec,
    pub metric: TMetric,
}

impl<TMetric> TestRawScorerProducer<TMetric>
where
    TMetric: Metric,
{
    pub fn new<R>(dim: usize, num_vectors: usize, metric: TMetric, rng: &mut R) -> Self
    where
        R: Rng + ?Sized,
    {
        let vectors = (0..num_vectors)
            .map(|_x| {
                let rnd_vec = random_vector(rng, dim);
                metric.preprocess(&rnd_vec).unwrap_or(rnd_vec)
            })
            .collect_vec();

        TestRawScorerProducer {
            vectors,
            deleted: BitVec::from_elem(num_vectors, false),
            metric,
        }
    }

    pub fn get_raw_scorer(&self, query: Vec<VectorElementType>) -> SimpleRawScorer<TMetric> {
        SimpleRawScorer {
            query: self.metric.preprocess(&query).unwrap_or(query),
            metric: &self.metric,
            vectors: &self.vectors,
            deleted: &self.deleted,
        }
    }
}
