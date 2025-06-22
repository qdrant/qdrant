use std::borrow::Cow;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoreType};

use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{DenseVector, VectorElementType};
use crate::spaces::metric::Metric;
use crate::types::QuantizationConfig;
use crate::vector_storage::query_scorer::QueryScorer;

pub struct QuantizedQueryScorer<'a, TEncodedVectors>
where
    TEncodedVectors: quantization::EncodedVectors,
{
    query: TEncodedVectors::EncodedQuery,
    quantized_data: &'a TEncodedVectors,
    hardware_counter: HardwareCounterCell,
}

/// Result of internal scorer creation.
/// It can be constructed scorer or nothing but we want to get hardware counter ownership back
pub enum QuantizedInternalScorerResult<'a, TEncodedVectors>
where
    TEncodedVectors: quantization::EncodedVectors,
{
    Scorer(QuantizedQueryScorer<'a, TEncodedVectors>),
    NotSupported(HardwareCounterCell),
}

impl<'a, TEncodedVectors> QuantizedQueryScorer<'a, TEncodedVectors>
where
    TEncodedVectors: quantization::EncodedVectors,
{
    pub fn new<TElement, TMetric>(
        raw_query: DenseVector,
        quantized_data: &'a TEncodedVectors,
        quantization_config: &QuantizationConfig,
        mut hardware_counter: HardwareCounterCell,
    ) -> Self
    where
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement>,
    {
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
            hardware_counter,
        }
    }

    pub fn new_internal(
        point_id: PointOffsetType,
        quantized_data: &'a TEncodedVectors,
        mut hardware_counter: HardwareCounterCell,
    ) -> QuantizedInternalScorerResult<'a, TEncodedVectors> {
        let Some(query) = quantized_data.encode_internal_vector(point_id) else {
            return QuantizedInternalScorerResult::NotSupported(hardware_counter);
        };

        hardware_counter.set_vector_io_read_multiplier(usize::from(quantized_data.is_on_disk()));
        QuantizedInternalScorerResult::Scorer(Self {
            query,
            quantized_data,
            hardware_counter,
        })
    }
}

impl<TEncodedVectors> QueryScorer for QuantizedQueryScorer<'_, TEncodedVectors>
where
    TEncodedVectors: quantization::EncodedVectors,
{
    type TVector = [VectorElementType];

    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        self.hardware_counter
            .vector_io_read()
            .incr_delta(self.quantized_data.quantized_vector_size());
        self.quantized_data
            .score_point(&self.query, idx, &self.hardware_counter)
    }

    fn score(&self, _v2: &[VectorElementType]) -> ScoreType {
        unimplemented!("This method is not expected to be called for quantized scorer");
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.quantized_data
            .score_internal(point_a, point_b, &self.hardware_counter)
    }
}
