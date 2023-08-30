use std::marker::PhantomData;

use crate::data_types::vectors::{VectorElementType, VectorType};
use crate::spaces::metric::Metric;
use crate::types::{PointOffsetType, ScoreType};
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::VectorStorage;

pub struct RecoQuery<T> {
    pub positives: Vec<T>,
    pub negatives: Vec<T>,
}

impl<T> RecoQuery<T> {
    pub fn new(positives: Vec<T>, negatives: Vec<T>) -> Self {
        Self {
            positives,
            negatives,
        }
    }

    pub fn new_preprocessed<F, B>(positives: Vec<B>, negatives: Vec<B>, preprocess: &F) -> Self
    where
        F: Fn(B) -> T,
    {
        Self {
            positives: positives.into_iter().map(preprocess).collect(),
            negatives: negatives.into_iter().map(preprocess).collect(),
        }
    }

    pub fn reprocess<F, U>(&self, preprocess: &F) -> RecoQuery<U>
    where
        F: Fn(&T) -> U,
    {
        RecoQuery::new(
            self.positives.iter().map(preprocess).collect(),
            self.negatives.iter().map(preprocess).collect(),
        )
    }

    pub fn score(&self, similarity: impl Fn(&T) -> ScoreType) -> ScoreType {
        // get similarities to all positives
        let positive_similarities = self.positives.iter().map(|positive| similarity(positive));

        // and all negatives
        let negative_similarities = self.negatives.iter().map(|negative| similarity(negative));

        // get max similarity to positives and max to negatives
        let max_positive = positive_similarities
            .max_by(|a, b| a.total_cmp(b))
            .unwrap_or(0.0);

        let max_negative = negative_similarities
            .max_by(|a, b| a.total_cmp(b))
            .unwrap_or(0.0);

        if max_positive > max_negative {
            max_positive
        } else {
            -(max_negative * max_negative)
        }
    }
}

pub struct RecoQueryScorer<'a, TMetric: Metric, TVectorStorage: VectorStorage> {
    vector_storage: &'a TVectorStorage,
    query: RecoQuery<VectorType>,
    metric: PhantomData<TMetric>,
}

impl<'a, TMetric: Metric, TVectorStorage: VectorStorage>
    RecoQueryScorer<'a, TMetric, TVectorStorage>
{
    pub fn new(
        query_positives: Vec<VectorType>,
        query_negatives: Vec<VectorType>,
        vector_storage: &'a TVectorStorage,
    ) -> Self {
        let query = RecoQuery::new_preprocessed(query_positives, query_negatives, &|vector| {
            TMetric::preprocess(vector)
        });

        Self {
            query,
            vector_storage,
            metric: PhantomData,
        }
    }
}

impl<'a, TMetric: Metric, TVectorStorage: VectorStorage> QueryScorer
    for RecoQueryScorer<'a, TMetric, TVectorStorage>
{
    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        let stored = self.vector_storage.get_vector(idx);
        self.score(stored)
    }

    #[inline]
    fn score(&self, v2: &[VectorElementType]) -> ScoreType {
        self.query.score(|this| TMetric::similarity(this, v2))
    }

    fn score_internal(&self, _point_a: PointOffsetType, _point_b: PointOffsetType) -> ScoreType {
        unimplemented!("Recommendation scorer compares against multiple vectors, not just one")
    }
}
