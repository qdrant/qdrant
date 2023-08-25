use std::marker::PhantomData;

use crate::data_types::vectors::VectorElementType;
use crate::spaces::metric::Metric;
use crate::types::{PointOffsetType, ScoreType};
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::VectorStorage;

pub struct RecommendQueryScorer<'a, TMetric: Metric, TVectorStorage: VectorStorage> {
    vector_storage: &'a TVectorStorage,
    query_positives: Vec<Vec<VectorElementType>>,
    query_negatives: Vec<Vec<VectorElementType>>,
    metric: PhantomData<TMetric>,
}

impl<'a, TMetric: Metric, TVectorStorage: VectorStorage>
    RecommendQueryScorer<'a, TMetric, TVectorStorage>
{
    pub fn new(
        query_positives: Vec<Vec<VectorElementType>>,
        query_negatives: Vec<Vec<VectorElementType>>,
        vector_storage: &'a TVectorStorage,
    ) -> Self {
        Self {
            query_positives: query_positives
                .into_iter()
                .map(|vector| TMetric::preprocess(&vector).unwrap_or(vector))
                .collect(),
            query_negatives: query_negatives
                .into_iter()
                .map(|vector| TMetric::preprocess(&vector).unwrap_or(vector))
                .collect(),
            vector_storage,
            metric: PhantomData,
        }
    }
}

impl<'a, TMetric: Metric, TVectorStorage: VectorStorage> QueryScorer
    for RecommendQueryScorer<'a, TMetric, TVectorStorage>
{
    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        let stored = self.vector_storage.get_vector(idx);
        self.score(stored)
    }

    #[inline]
    fn score(&self, v2: &[VectorElementType]) -> ScoreType {
        // get similarities to all positives
        let positive_similarities = self
            .query_positives
            .iter()
            .map(|positive| TMetric::similarity(positive, v2));

        // and all negatives
        let negative_similarities = self
            .query_negatives
            .iter()
            .map(|negative| TMetric::similarity(negative, v2));

        // get max similarity to positives and max to negatives
        let max_positive = positive_similarities
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .unwrap_or(0.0);

        let max_negative = negative_similarities
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .unwrap_or(0.0);

        if max_positive > max_negative {
            max_positive
        } else {
            -(max_negative * max_negative)
        }
    }

    fn score_internal(&self, _point_a: PointOffsetType, _point_b: PointOffsetType) -> ScoreType {
        unimplemented!("Recommendation scorer compares against multiple vectors, not just one")
    }
}
