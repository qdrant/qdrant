use std::marker::PhantomData;

use common::types::{PointOffsetType, ScoreType};

use crate::data_types::vectors::{VectorElementType, VectorType};
use crate::spaces::metric::Metric;
use crate::vector_storage::query::reco_query::RecoQuery;
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::DenseVectorStorage;

pub struct RecoQueryScorer<'a, TMetric: Metric, TVectorStorage: DenseVectorStorage> {
    vector_storage: &'a TVectorStorage,
    query: RecoQuery<VectorType>,
    metric: PhantomData<TMetric>,
}

impl<'a, TMetric: Metric, TVectorStorage: DenseVectorStorage>
    RecoQueryScorer<'a, TMetric, TVectorStorage>
{
    pub fn new(query: RecoQuery<VectorType>, vector_storage: &'a TVectorStorage) -> Self {
        let query = query.transform(|vector| TMetric::preprocess(vector));

        Self {
            query,
            vector_storage,
            metric: PhantomData,
        }
    }
}

impl<'a, TMetric: Metric, TVectorStorage: DenseVectorStorage> QueryScorer
    for RecoQueryScorer<'a, TMetric, TVectorStorage>
{
    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        let stored = self.vector_storage.get_dense(idx);
        self.score(stored)
    }

    #[inline]
    fn score(&self, against: &[VectorElementType]) -> ScoreType {
        self.query
            .score_by(|example| TMetric::similarity(example, against))
    }

    fn score_internal(&self, _point_a: PointOffsetType, _point_b: PointOffsetType) -> ScoreType {
        unimplemented!("Recommendation scorer compares against multiple vectors, not just one")
    }
}
