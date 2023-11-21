use std::marker::PhantomData;

use common::types::{PointOffsetType, ScoreType};

use crate::data_types::vectors::{VectorElementType, VectorType};
use crate::spaces::metric::Metric;
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::DenseVectorStorage;

pub struct MetricQueryScorer<'a, TMetric: Metric, TVectorStorage: DenseVectorStorage> {
    vector_storage: &'a TVectorStorage,
    query: Vec<VectorElementType>,
    metric: PhantomData<TMetric>,
}

impl<'a, TMetric: Metric, TVectorStorage: DenseVectorStorage>
    MetricQueryScorer<'a, TMetric, TVectorStorage>
{
    pub fn new(query: VectorType, vector_storage: &'a TVectorStorage) -> Self {
        Self {
            query: TMetric::preprocess(query),
            vector_storage,
            metric: PhantomData,
        }
    }
}

impl<'a, TMetric: Metric, TVectorStorage: DenseVectorStorage> QueryScorer
    for MetricQueryScorer<'a, TMetric, TVectorStorage>
{
    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        TMetric::similarity(&self.query, self.vector_storage.get_dense(idx))
    }

    #[inline]
    fn score(&self, v2: &[VectorElementType]) -> ScoreType {
        TMetric::similarity(&self.query, v2)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        let v1 = self.vector_storage.get_dense(point_a);
        let v2 = self.vector_storage.get_dense(point_b);
        TMetric::similarity(v1, v2)
    }
}
