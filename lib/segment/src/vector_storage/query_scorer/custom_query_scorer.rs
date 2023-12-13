use std::marker::PhantomData;

use common::types::{PointOffsetType, ScoreType};

use crate::data_types::vectors::{DenseVector, VectorElementType};
use crate::spaces::metric::Metric;
use crate::vector_storage::query::{Query, TransformInto};
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::DenseVectorStorage;

pub struct CustomQueryScorer<
    'a,
    TMetric: Metric,
    TVectorStorage: DenseVectorStorage,
    TQuery: Query<DenseVector>,
> {
    vector_storage: &'a TVectorStorage,
    query: TQuery,
    metric: PhantomData<TMetric>,
}

impl<
        'a,
        TMetric: Metric,
        TVectorStorage: DenseVectorStorage,
        TQuery: Query<DenseVector> + TransformInto<TQuery>,
    > CustomQueryScorer<'a, TMetric, TVectorStorage, TQuery>
{
    pub fn new(query: TQuery, vector_storage: &'a TVectorStorage) -> Self {
        let query = query
            .transform(|vector| Ok(TMetric::preprocess(vector)))
            .unwrap();

        Self {
            query,
            vector_storage,
            metric: PhantomData,
        }
    }
}

impl<'a, TMetric: Metric, TVectorStorage: DenseVectorStorage, TQuery: Query<DenseVector>>
    QueryScorer<[VectorElementType]> for CustomQueryScorer<'a, TMetric, TVectorStorage, TQuery>
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
        unimplemented!("Custom scorer can compare against multiple vectors, not just one")
    }
}
