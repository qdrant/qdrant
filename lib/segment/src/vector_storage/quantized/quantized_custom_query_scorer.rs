use std::marker::PhantomData;

use common::types::{PointOffsetType, ScoreType};

use crate::data_types::vectors::{DenseVector, VectorElementType};
use crate::spaces::metric::Metric;
use crate::vector_storage::query::{Query, TransformInto};
use crate::vector_storage::query_scorer::QueryScorer;

pub struct QuantizedCustomQueryScorer<
    'a,
    TMetric,
    TEncodedQuery,
    TEncodedVectors,
    TQuery,
    TOriginalQuery,
> where
    TMetric: Metric<VectorElementType>,
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
    TQuery: Query<TEncodedQuery>,
    TOriginalQuery: Query<DenseVector>,
{
    original_query: TOriginalQuery,
    query: TQuery,
    quantized_storage: &'a TEncodedVectors,
    phantom: PhantomData<TEncodedQuery>,
    metric: PhantomData<TMetric>,
}

impl<'a, TMetric, TEncodedQuery, TEncodedVectors, TQuery, TOriginalQuery>
    QuantizedCustomQueryScorer<'a, TMetric, TEncodedQuery, TEncodedVectors, TQuery, TOriginalQuery>
where
    TMetric: Metric<VectorElementType>,
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
    TQuery: Query<TEncodedQuery>,
    TOriginalQuery: Query<DenseVector>
        + Clone
        + TransformInto<TOriginalQuery>
        + TransformInto<TQuery, DenseVector, TEncodedQuery>,
{
    pub fn new(raw_query: TOriginalQuery, quantized_storage: &'a TEncodedVectors) -> Self {
        let original_query = raw_query.transform(|v| Ok(TMetric::preprocess(v))).unwrap();
        let query = original_query
            .clone()
            .transform(|v: DenseVector| Ok(quantized_storage.encode_query(&v)))
            .unwrap();

        Self {
            original_query,
            query,
            quantized_storage,
            phantom: PhantomData,
            metric: PhantomData,
        }
    }
}

impl<
        TMetric,
        TEncodedQuery,
        TEncodedVectors,
        TOriginalQuery: Query<DenseVector>,
        TQuery: Query<TEncodedQuery>,
    > QueryScorer<[VectorElementType]>
    for QuantizedCustomQueryScorer<
        '_,
        TMetric,
        TEncodedQuery,
        TEncodedVectors,
        TQuery,
        TOriginalQuery,
    >
where
    TMetric: Metric<VectorElementType>,
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        self.query
            .score_by(|this| self.quantized_storage.score_point(this, idx))
    }

    fn score(&self, v2: &[VectorElementType]) -> ScoreType {
        debug_assert!(
            false,
            "This method is not expected to be called for quantized scorer"
        );
        self.original_query
            .score_by(|this| TMetric::similarity(this, v2))
    }

    fn score_internal(&self, _point_a: PointOffsetType, _point_b: PointOffsetType) -> ScoreType {
        unimplemented!("Custom scorer compares against multiple vectors, not just one")
    }
}
