use std::borrow::Cow;

use common::counter::hw::HwScale;
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
    hw: HwScale,
    query: Vec<QuantizedStorage::EncodedQuery>,
    quantized_multivector_storage: &'a QuantizedMultivectorStorage<QuantizedStorage, OffsetStorage>,
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
                inner_converted,
            );
            query.extend_from_slice(&inner_prequantized);
        }

        let query = quantized_multivector_storage.encode_query(&query);

        Self {
            hw: HwScale {
                cpu: 1,
                vector_io_read: usize::from(quantized_multivector_storage.is_cold()),
            },
            query,
            quantized_multivector_storage,
        }
    }

    pub fn new_internal(
        point_id: PointOffsetType,
        quantized_multivector_storage: &'a QuantizedMultivectorStorage<
            QuantizedStorage,
            OffsetStorage,
        >,
    ) -> Result<Self, InternalScorerUnsupported> {
        let Some(query) = quantized_multivector_storage.encode_internal_vector(point_id) else {
            return Err(InternalScorerUnsupported);
        };

        Ok(Self {
            hw: HwScale {
                cpu: 1,
                vector_io_read: usize::from(quantized_multivector_storage.is_cold()),
            },
            query,
            quantized_multivector_storage,
        })
    }
}

impl<QuantizedStorage, OffsetStorage> QueryScorer
    for QuantizedMultiQueryScorer<'_, QuantizedStorage, OffsetStorage>
where
    QuantizedStorage: quantization::EncodedVectors,
    OffsetStorage: MultivectorOffsetsStorage,
{
    fn score_stored_batch(&self, ids: &[PointOffsetType], scores: &mut [ScoreType]) {
        debug_assert_eq!(ids.len(), scores.len());

        self.hw
            .vector_io_read(size_of::<MultivectorOffset>() * ids.len());

        self.quantized_multivector_storage.score_points_batch(
            ids,
            |score_fn| score_fn(&self.query),
            scores,
        )
    }

    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        let multi_vector_offset = self.quantized_multivector_storage.get_offset(idx);
        let sub_vectors_count = multi_vector_offset.count as usize;
        self.hw.vector_io_read(
            size_of::<MultivectorOffset>()
                + self.quantized_multivector_storage.quantized_vector_size() * sub_vectors_count,
        );
        // quantized multivector storage handles hardware counter to batch vector IO
        self.quantized_multivector_storage
            .score_point(&self.query, idx)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.quantized_multivector_storage
            .score_internal(point_a, point_b)
    }

    type SupportsBytes = False;
    fn score_bytes(&self, enabled: Self::SupportsBytes, _: &[u8]) -> ScoreType {
        match enabled {}
    }
}
