use std::borrow::Cow;
use std::marker::PhantomData;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoreType};

use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::DenseVector;
use crate::spaces::metric::Metric;
use crate::types::QuantizationConfig;
use crate::vector_storage::query_scorer::QueryScorer;

pub struct QuantizedQueryScorer<'a, TElement, TMetric, TEncodedVectors>
where
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TEncodedVectors: quantization::EncodedVectors,
{
    query: TEncodedVectors::EncodedQuery,
    quantized_data: &'a TEncodedVectors,
    metric: PhantomData<TMetric>,
    element: PhantomData<TElement>,
    hardware_counter: HardwareCounterCell,
}

impl<'a, TElement, TMetric, TEncodedVectors>
    QuantizedQueryScorer<'a, TElement, TMetric, TEncodedVectors>
where
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TEncodedVectors: quantization::EncodedVectors,
{
    pub fn new(
        raw_query: DenseVector,
        quantized_data: &'a TEncodedVectors,
        quantization_config: &QuantizationConfig,
        mut hardware_counter: HardwareCounterCell,
    ) -> Self {
        let raw_preprocessed_query = TMetric::preprocess(raw_query);
        let original_query = TElement::slice_from_float_cow(Cow::Owned(raw_preprocessed_query));
        let original_query_prequantized = TElement::quantization_preprocess(
            quantization_config,
            TMetric::distance(),
            original_query.as_ref(),
        );
        let query = quantized_data.encode_query(&original_query_prequantized);

        hardware_counter.set_vector_io_read_multiplier(usize::from(quantized_data.is_on_disk()));

        Self {
            query,
            quantized_data,
            metric: PhantomData,
            element: PhantomData,
            hardware_counter,
        }
    }
}
impl<TElement, TMetric, TEncodedVectors> QueryScorer<[TElement]>
    for QuantizedQueryScorer<'_, TElement, TMetric, TEncodedVectors>
where
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TEncodedVectors: quantization::EncodedVectors,
{
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        self.hardware_counter
            .vector_io_read()
            .incr_delta(self.quantized_data.quantized_vector_size());
        self.quantized_data
            .score_point(&self.query, idx, &self.hardware_counter)
    }

    fn score(&self, _v2: &[TElement]) -> ScoreType {
        unimplemented!("This method is not expected to be called for quantized scorer");
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.quantized_data
            .score_internal(point_a, point_b, &self.hardware_counter)
    }
}
