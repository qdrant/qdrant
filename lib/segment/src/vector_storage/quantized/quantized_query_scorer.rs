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

/// Error type returned when [`QuantizedQueryScorer::new_internal`] fails.
/// Contains the original [`HardwareCounterCell`] passed to [`QuantizedQueryScorer::new_internal`].
pub struct InternalScorerUnsupported(pub HardwareCounterCell);

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

    /// Build a raw scorer for the specified `point_id`.
    /// If not supported, return [`InternalScorerUnsupported`] with the original `hardware_counter`.
    pub fn new_internal(
        point_id: PointOffsetType,
        quantized_data: &'a TEncodedVectors,
        mut hardware_counter: HardwareCounterCell,
    ) -> Result<Self, InternalScorerUnsupported> {
        let Some(query) = quantized_data.encode_internal_vector(point_id) else {
            return Err(InternalScorerUnsupported(hardware_counter));
        };

        hardware_counter.set_vector_io_read_multiplier(usize::from(quantized_data.is_on_disk()));
        Ok(Self {
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

    type SupportsBytes = TEncodedVectors::SupportsBytes;
    fn score_bytes(&self, enabled: Self::SupportsBytes, bytes: &[u8]) -> ScoreType {
        self.quantized_data
            .score_bytes(enabled, &self.query, bytes, &self.hardware_counter)
    }
}
