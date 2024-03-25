use std::marker::PhantomData;

use common::types::{PointOffsetType, ScoreType};

use super::score_multi;
use crate::data_types::vectors::{MultiDenseVector, VectorElementType};
use crate::spaces::metric::Metric;
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::MultiVectorStorage;

pub struct MultiMetricQueryScorer<
    'a,
    TMetric: Metric<VectorElementType>,
    TVectorStorage: MultiVectorStorage,
> {
    vector_storage: &'a TVectorStorage,
    query: MultiDenseVector,
    metric: PhantomData<TMetric>,
}

impl<'a, TMetric: Metric<VectorElementType>, TVectorStorage: MultiVectorStorage>
    MultiMetricQueryScorer<'a, TMetric, TVectorStorage>
{
    pub fn new(query: MultiDenseVector, vector_storage: &'a TVectorStorage) -> Self {
        Self {
            query: query.into_iter().map(|v| TMetric::preprocess(v)).collect(),
            vector_storage,
            metric: PhantomData,
        }
    }
}

impl<'a, TMetric: Metric<VectorElementType>, TVectorStorage: MultiVectorStorage>
    QueryScorer<MultiDenseVector> for MultiMetricQueryScorer<'a, TMetric, TVectorStorage>
{
    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        score_multi::<TMetric>(&self.query, self.vector_storage.get_multi(idx))
    }

    #[inline]
    fn score(&self, v2: &MultiDenseVector) -> ScoreType {
        score_multi::<TMetric>(&self.query, v2)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        let v1 = self.vector_storage.get_multi(point_a);
        let v2 = self.vector_storage.get_multi(point_b);
        score_multi::<TMetric>(v1, v2)
    }
}
