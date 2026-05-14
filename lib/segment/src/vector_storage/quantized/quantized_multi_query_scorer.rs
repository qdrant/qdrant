use std::borrow::Cow;

use common::counter::hardware_counter::HardwareCounterCell;
use common::typelevel::False;
use common::types::{PointOffsetType, ScoreType};
use quantization::EncodedVectors;

use super::quantized_query_scorer::InternalScorerUnsupported;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::MultiDenseVectorInternal;
use crate::spaces::metric::Metric;
use crate::types::QuantizationConfig;
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffset, MultivectorOffsets, MultivectorOffsetsStorage, QuantizedMultivectorStorage,
};
use crate::vector_storage::query_scorer::QueryScorer;

pub struct QuantizedMultiQueryScorer<'a, QuantizedStorage, OffsetStorage>
where
    QuantizedStorage: quantization::EncodedVectors,
    OffsetStorage: MultivectorOffsetsStorage,
{
    query: Vec<QuantizedStorage::EncodedQuery>,
    quantized_multivector_storage: &'a QuantizedMultivectorStorage<QuantizedStorage, OffsetStorage>,
    hardware_counter: HardwareCounterCell,
}

impl<'a, QuantizedStorage, OffsetStorage>
    QuantizedMultiQueryScorer<'a, QuantizedStorage, OffsetStorage>
where
    QuantizedStorage: quantization::EncodedVectors,
    OffsetStorage: MultivectorOffsetsStorage,
{
    pub fn new_multi<TElement, TMetric>(
        raw_query: &MultiDenseVectorInternal,
        quantized_multivector_storage: &'a QuantizedMultivectorStorage<
            QuantizedStorage,
            OffsetStorage,
        >,
        quantization_config: &QuantizationConfig,
        mut hardware_counter: HardwareCounterCell,
    ) -> Self
    where
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement>,
    {
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
            hardware_counter,
        }
    }

    pub fn new_internal(
        point_id: PointOffsetType,
        quantized_multivector_storage: &'a QuantizedMultivectorStorage<
            QuantizedStorage,
            OffsetStorage,
        >,
        mut hardware_counter: HardwareCounterCell,
    ) -> Result<Self, InternalScorerUnsupported> {
        let Some(query) = quantized_multivector_storage.encode_internal_vector(point_id) else {
            return Err(InternalScorerUnsupported(hardware_counter));
        };

        hardware_counter
            .set_vector_io_read_multiplier(usize::from(quantized_multivector_storage.is_on_disk()));

        Ok(Self {
            query,
            quantized_multivector_storage,
            hardware_counter,
        })
    }
}

impl<QuantizedStorage, OffsetStorage> QueryScorer
    for QuantizedMultiQueryScorer<'_, QuantizedStorage, OffsetStorage>
where
    QuantizedStorage: quantization::EncodedVectors,
    OffsetStorage: MultivectorOffsetsStorage,
{
    type TVector = ();

    fn score_stored_batch(&self, ids: &[PointOffsetType], scores: &mut [ScoreType]) {
        debug_assert_eq!(ids.len(), scores.len());

        self.hardware_counter
            .vector_io_read()
            .incr_delta(size_of::<MultivectorOffset>() * ids.len());

        self.quantized_multivector_storage.score_points_batch(
            ids,
            |score_fn| score_fn(&self.query),
            scores,
            &self.hardware_counter,
        )
    }

    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        let multi_vector_offset = self.quantized_multivector_storage.get_offset(idx);
        let sub_vectors_count = multi_vector_offset.count as usize;
        self.hardware_counter.vector_io_read().incr_delta(
            size_of::<MultivectorOffset>()
                + self.quantized_multivector_storage.quantized_vector_size() * sub_vectors_count,
        );
        // quantized multivector storage handles hardware counter to batch vector IO
        self.quantized_multivector_storage
            .score_point(&self.query, idx, &self.hardware_counter)
    }

    fn score(&self, _v2: &()) -> ScoreType {
        unimplemented!("This method is not expected to be called for quantized scorer");
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.quantized_multivector_storage
            .score_internal(point_a, point_b, &self.hardware_counter)
    }

    type SupportsBytes = False;
    fn score_bytes(&self, enabled: Self::SupportsBytes, _: &[u8]) -> ScoreType {
        match enabled {}
    }
}
