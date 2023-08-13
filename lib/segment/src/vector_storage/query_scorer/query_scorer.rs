use std::marker::PhantomData;

use crate::data_types::vectors::VectorElementType;
use crate::spaces::metric::Metric;
use crate::types::ScoreType;

pub trait QueryScorer {
    fn score(&self, v2: &[VectorElementType]) -> ScoreType;

    fn score_raw(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType;
}

pub struct MetricQueryScorer<TMetric: Metric> {
    query: Vec<VectorElementType>,
    metric: PhantomData<TMetric>,
}

impl<TMetric: Metric> MetricQueryScorer<TMetric> {
    pub fn new(query: Vec<VectorElementType>) -> Self {
        Self {
            query: TMetric::preprocess(&query).unwrap_or(query),
            metric: PhantomData,
        }
    }
}

impl<TMetric: Metric> QueryScorer for MetricQueryScorer<TMetric> {
    #[inline]
    fn score(&self, v2: &[VectorElementType]) -> ScoreType {
        TMetric::similarity(&self.query, v2)
    }

    #[inline]
    fn score_raw(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        TMetric::similarity(v1, v2)
    }
}
