use std::marker::PhantomData;

use common::types::{PointOffsetType, ScoreType};

use crate::data_types::vectors::{VectorElementType, VectorType};
use crate::spaces::metric::Metric;
use crate::vector_storage::query::discovery_query::DiscoveryQuery;
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::VectorStorage;

// TODO(luis): maybe find a way to generalize DiscoveryQueryScorer and RecoQueryScorer by
//     using a single Query trait. The crux is that `transform` function is hard to convert
//     nicely into a trait function
pub struct DiscoveryQueryScorer<'a, TMetric: Metric, TVectorStorage: VectorStorage> {
    vector_storage: &'a TVectorStorage,
    query: DiscoveryQuery<VectorType>,
    metric: PhantomData<TMetric>,
}

impl<'a, TMetric: Metric, TVectorStorage: VectorStorage>
    DiscoveryQueryScorer<'a, TMetric, TVectorStorage>
{
    #[allow(dead_code)] // TODO(luis): remove once integrated
    pub fn new(query: DiscoveryQuery<VectorType>, vector_storage: &'a TVectorStorage) -> Self {
        let query = query.transform(|vector| TMetric::preprocess(vector));

        Self {
            query,
            vector_storage,
            metric: PhantomData,
        }
    }
}

impl<'a, TMetric: Metric, TVectorStorage: VectorStorage> QueryScorer
    for DiscoveryQueryScorer<'a, TMetric, TVectorStorage>
{
    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        let stored = self.vector_storage.get_vector(idx);
        self.score(stored)
    }

    #[inline]
    fn score(&self, against: &[VectorElementType]) -> ScoreType {
        self.query
            .score_by(|example| TMetric::similarity(example, against))
    }

    fn score_internal(&self, _point_a: PointOffsetType, _point_b: PointOffsetType) -> ScoreType {
        unimplemented!("Custom scorer compares against multiple vectors, not just one")
    }
}
