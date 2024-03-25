use std::marker::PhantomData;

use common::types::{PointOffsetType, ScoreType};
use itertools::Itertools;

use super::score_multi;
use crate::data_types::vectors::{MultiDenseVector, VectorElementType};
use crate::spaces::metric::Metric;
use crate::vector_storage::query::{Query, TransformInto};
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::MultiVectorStorage;

pub struct MultiCustomQueryScorer<
    'a,
    TMetric: Metric<VectorElementType>,
    TVectorStorage: MultiVectorStorage,
    TQuery: Query<MultiDenseVector>,
> {
    vector_storage: &'a TVectorStorage,
    query: TQuery,
    metric: PhantomData<TMetric>,
}

impl<
        'a,
        TMetric: Metric<VectorElementType>,
        TVectorStorage: MultiVectorStorage,
        TQuery: Query<MultiDenseVector> + TransformInto<TQuery, MultiDenseVector, MultiDenseVector>,
    > MultiCustomQueryScorer<'a, TMetric, TVectorStorage, TQuery>
{
    pub fn new(query: TQuery, vector_storage: &'a TVectorStorage) -> Self {
        let query = query
            .transform(|vector| Ok(vector.into_iter().map(TMetric::preprocess).collect_vec()))
            .unwrap();

        Self {
            query,
            vector_storage,
            metric: PhantomData,
        }
    }
}

impl<
        'a,
        TMetric: Metric<VectorElementType>,
        TVectorStorage: MultiVectorStorage,
        TQuery: Query<MultiDenseVector>,
    > QueryScorer<MultiDenseVector>
    for MultiCustomQueryScorer<'a, TMetric, TVectorStorage, TQuery>
{
    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        let stored = self.vector_storage.get_multi(idx);
        self.score(stored)
    }

    #[inline]
    fn score(&self, against: &MultiDenseVector) -> ScoreType {
        self.query
            .score_by(|example| score_multi::<TMetric>(example, against))
    }

    fn score_internal(&self, _point_a: PointOffsetType, _point_b: PointOffsetType) -> ScoreType {
        unimplemented!("Custom scorer can compare against multiple vectors, not just one")
    }
}
