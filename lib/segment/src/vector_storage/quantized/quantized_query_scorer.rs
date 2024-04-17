use std::marker::PhantomData;

use common::types::{PointOffsetType, ScoreType};

use crate::data_types::vectors::{DenseVector, VectorElementType};
use crate::spaces::metric::Metric;
use crate::vector_storage::query_scorer::QueryScorer;

pub struct QuantizedQueryScorer<
    'a,
    TMetric: Metric<VectorElementType>,
    TEncodedQuery,
    TEncodedVectors,
> where
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    original_query: DenseVector,
    query: TEncodedQuery,
    quantized_data: &'a TEncodedVectors,
    metric: PhantomData<TMetric>,
}

impl<'a, TMetric, TEncodedQuery, TEncodedVectors>
    QuantizedQueryScorer<'a, TMetric, TEncodedQuery, TEncodedVectors>
where
    TMetric: Metric<VectorElementType>,
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    pub fn new(raw_query: DenseVector, quantized_data: &'a TEncodedVectors) -> Self {
        let original_query = TMetric::preprocess(raw_query);
        let query = quantized_data.encode_query(&original_query);

        Self {
            original_query,
            query,
            quantized_data,
            metric: PhantomData,
        }
    }
}

impl<TMetric, TEncodedQuery, TEncodedVectors> QueryScorer<[VectorElementType]>
    for QuantizedQueryScorer<'_, TMetric, TEncodedQuery, TEncodedVectors>
where
    TMetric: Metric<VectorElementType>,
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        self.quantized_data.score_point(&self.query, idx)
    }

    fn score(&self, v2: &[VectorElementType]) -> ScoreType {
        debug_assert!(
            false,
            "This method is not expected to be called for quantized scorer"
        );
        TMetric::similarity(&self.original_query, v2)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.quantized_data.score_internal(point_a, point_b)
    }
}
