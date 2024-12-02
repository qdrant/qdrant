use std::borrow::Cow;
use std::marker::PhantomData;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoreType};

use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{DenseVector, MultiDenseVectorInternal};
use crate::spaces::metric::Metric;
use crate::types::QuantizationConfig;
use crate::vector_storage::common::VECTOR_READ_BATCH_SIZE;
use crate::vector_storage::query_scorer::QueryScorer;

pub struct QuantizedQueryScorer<'a, TElement, TMetric, TEncodedQuery, TEncodedVectors>
where
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    query: TEncodedQuery,
    quantized_data: &'a TEncodedVectors,
    metric: PhantomData<TMetric>,
    element: PhantomData<TElement>,
    hardware_counter: HardwareCounterCell,
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
            query,
            quantized_data,
            metric: PhantomData,
            element: PhantomData,
            hardware_counter: HardwareCounterCell::new(),
        }
    }

    pub fn new_multi(
        raw_query: &MultiDenseVectorInternal,
        quantized_data: &'a TEncodedVectors,
        quantization_config: &QuantizationConfig,
    ) -> Self {
        let mut query = Vec::new();
        for inner_vector in raw_query.multi_vectors() {
            let inner_preprocessed = TMetric::preprocess(inner_vector.to_vec());
            let inner_converted = TElement::slice_from_float_cow(Cow::Owned(inner_preprocessed));
            let inner_prequantized = TElement::quantization_preprocess(
                quantization_config,
                TMetric::distance(),
                inner_converted.as_ref(),
            );
            query.extend_from_slice(&inner_prequantized);
        }

        let query = quantized_data.encode_query(&query);

        Self {
            query,
            quantized_data,
            metric: PhantomData,
            element: PhantomData,
            hardware_counter: HardwareCounterCell::new(),
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
        self.quantized_data
            .score_point(&self.query, idx, &self.hardware_counter)
    }

    fn score_stored_batch(&self, ids: &[PointOffsetType], scores: &mut [ScoreType]) {
        debug_assert!(ids.len() <= VECTOR_READ_BATCH_SIZE);
        debug_assert_eq!(ids.len(), scores.len());
        // no specific implementation for batch scoring
        for (idx, id) in ids.iter().enumerate() {
            scores[idx] = self.score_stored(*id);
        }
    }

    fn score(&self, _v2: &[TElement]) -> ScoreType {
        unimplemented!("This method is not expected to be called for quantized scorer");
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.quantized_data
            .score_internal(point_a, point_b, &self.hardware_counter)
    }

    fn take_hardware_counter(&self) -> HardwareCounterCell {
        let mut counter = self.hardware_counter.take();

        counter
            .cpu_counter_mut()
            .multiplied_mut(size_of::<TElement>());

        counter
    }
}
