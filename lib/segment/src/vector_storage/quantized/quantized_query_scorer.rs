use std::borrow::Cow;
use std::marker::PhantomData;

use common::types::{PointOffsetType, ScoreType};

use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{DenseVector, TypedDenseVector};
use crate::spaces::metric::Metric;
use crate::types::QuantizationConfig;
use crate::vector_storage::query_scorer::QueryScorer;

pub struct QuantizedQueryScorer<'a, TElement, TMetric, TEncodedQuery, TEncodedVectors>
where
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    original_query: TypedDenseVector<TElement>,
    query: TEncodedQuery,
    quantized_data: &'a TEncodedVectors,
    metric: PhantomData<TMetric>,
}

impl<'a, TElement, TMetric, TEncodedQuery, TEncodedVectors>
    QuantizedQueryScorer<'a, TElement, TMetric, TEncodedQuery, TEncodedVectors>
where
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    pub fn new(
        raw_query: DenseVector,
        quantized_data: &'a TEncodedVectors,
        quantization_config: &QuantizationConfig,
    ) -> Self {
        let raw_preprocessed_query = TMetric::preprocess(raw_query);
        let original_query = TElement::slice_from_float_cow(Cow::Owned(raw_preprocessed_query));
        let original_query_prequantized = TElement::quantization_preprocess(
            quantization_config,
            TMetric::distance(),
            original_query.as_ref(),
        );
        let query = quantized_data.encode_query(&original_query_prequantized);

        Self {
            original_query: original_query.into_owned(),
            query,
            quantized_data,
            metric: PhantomData,
        }
    }
}

impl<TElement, TMetric, TEncodedQuery, TEncodedVectors> QueryScorer<[TElement]>
    for QuantizedQueryScorer<'_, TElement, TMetric, TEncodedQuery, TEncodedVectors>
where
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        self.quantized_data.score_point(&self.query, idx)
    }

    fn score(&self, v2: &[TElement]) -> ScoreType {
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
