use crate::payload_storage::FilterContext;
use crate::spaces::metric::Metric;
use crate::types::{PointOffsetType, VectorElementType};
use crate::vector_storage::chunked_vectors::ChunkedVectors;
use crate::vector_storage::simple_vector_storage::SimpleRawScorer;
use bitvec::prelude::BitVec;
use rand::Rng;
use std::marker::PhantomData;

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
    pub vectors: ChunkedVectors,
    pub deleted: BitVec,
    pub metric: PhantomData<TMetric>,
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

    pub fn get_raw_scorer(&self, query: Vec<VectorElementType>) -> SimpleRawScorer<TMetric> {
        SimpleRawScorer::<TMetric> {
            query: TMetric::preprocess(&query).unwrap_or(query),
            metric: PhantomData,
            vectors: &self.vectors,
            deleted: &self.deleted,
        }
    }
}
