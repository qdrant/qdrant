use std::borrow::Cow;
use std::marker::PhantomData;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoreType};

use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::MultiDenseVectorInternal;
use crate::spaces::metric::Metric;
use crate::types::QuantizationConfig;
use crate::vector_storage::query_scorer::QueryScorer;

pub struct QuantizedMultiQueryScorer<'a, TElement, TMetric, TEncodedQuery, TEncodedVectors>
where
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    query: TEncodedQuery,
    quantized_multivector_storage: &'a TEncodedVectors,
    metric: PhantomData<TMetric>,
    element: PhantomData<TElement>,
    hardware_counter: HardwareCounterCell,
}

impl<'a, TElement, TMetric, TEncodedQuery, TEncodedVectors>
    QuantizedMultiQueryScorer<'a, TElement, TMetric, TEncodedQuery, TEncodedVectors>
where
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    pub fn new_multi(
        raw_query: &MultiDenseVectorInternal,
        quantized_multivector_storage: &'a TEncodedVectors,
        quantization_config: &QuantizationConfig,
        mut hardware_counter: HardwareCounterCell,
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

        let query = quantized_multivector_storage.encode_query(&query);

        hardware_counter
            .set_vector_io_read_multiplier(usize::from(quantized_multivector_storage.is_on_disk()));

        Self {
            query,
            quantized_multivector_storage,
            metric: PhantomData,
            element: PhantomData,
            hardware_counter,
        }
    }
}

impl<TElement, TMetric, TEncodedQuery, TEncodedVectors> QueryScorer<[TElement]>
    for QuantizedMultiQueryScorer<'_, TElement, TMetric, TEncodedQuery, TEncodedVectors>
where
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        // quantized multivector storage handles hardware counter to batch vector IO
        self.quantized_multivector_storage
            .score_point(&self.query, idx, &self.hardware_counter)
    }

    fn score(&self, _v2: &[TElement]) -> ScoreType {
        unimplemented!("This method is not expected to be called for quantized scorer");
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.quantized_multivector_storage
            .score_internal(point_a, point_b, &self.hardware_counter)
    }
}
