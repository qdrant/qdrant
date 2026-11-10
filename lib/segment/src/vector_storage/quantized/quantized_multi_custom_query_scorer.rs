use std::borrow::Cow;
use std::cell::Cell;

use common::ambient::hw::{self, HwScale};
use common::typelevel::False;
use common::types::{PointOffsetType, ScoreType};
use quantization::{EncodedVectors, PackedMultiQuery};

use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::CowMultiVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{MultiDenseVectorInternal, TypedMultiDenseVector};
use crate::spaces::metric::Metric;
use crate::types::QuantizationConfig;
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffset, MultivectorOffsetsStorage, QuantizedMultivectorStorage,
};
use crate::vector_storage::query::{Query, TransformInto};
use crate::vector_storage::query_scorer::QueryScorer;

/// One multi-vector of a custom query: its encoded vectors, and the same packed
/// for the batched MaxSim kernel when the storage has one.
pub struct EncodedMultiQuery<TEncodedQuery> {
    vectors: Vec<TEncodedQuery>,
    packed: Option<PackedMultiQuery>,
}

pub struct QuantizedMultiCustomQueryScorer<'a, QuantizedStorage, OffsetStorage, TQuery>
where
    QuantizedStorage: quantization::EncodedVectors,
    OffsetStorage: MultivectorOffsetsStorage,
    TQuery: Query<EncodedMultiQuery<QuantizedStorage::EncodedQuery>>,
{
    hw: HwScale,
    query: TQuery,
    /// Every multi-vector of `query` is packed.
    packed: bool,
    quantized_multivector_storage: &'a QuantizedMultivectorStorage<QuantizedStorage, OffsetStorage>,
}

impl<'a, QuantizedStorage, OffsetStorage, TQuery>
    QuantizedMultiCustomQueryScorer<'a, QuantizedStorage, OffsetStorage, TQuery>
where
    QuantizedStorage: quantization::EncodedVectors,
    OffsetStorage: MultivectorOffsetsStorage,
    TQuery: Query<EncodedMultiQuery<QuantizedStorage::EncodedQuery>>,
{
    pub fn new_multi<TElement, TMetric, TOriginalQuery, TInputQuery>(
        raw_query: TInputQuery,
        quantized_multivector_storage: &'a QuantizedMultivectorStorage<
            QuantizedStorage,
            OffsetStorage,
        >,
        quantization_config: &QuantizationConfig,
    ) -> Self
    where
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement>,
        TOriginalQuery: Query<TypedMultiDenseVector<TElement>>
            + TransformInto<
                TQuery,
                TypedMultiDenseVector<TElement>,
                EncodedMultiQuery<QuantizedStorage::EncodedQuery>,
            > + Clone,
        TInputQuery: Query<MultiDenseVectorInternal>
            + TransformInto<TOriginalQuery, MultiDenseVectorInternal, TypedMultiDenseVector<TElement>>,
    {
        let original_query: TOriginalQuery = raw_query
            .transform(&|vector| {
                let mut preprocessed = Vec::new();
                for slice in vector.multi_vectors() {
                    preprocessed.extend_from_slice(&TMetric::preprocess(slice.to_vec()));
                }
                let preprocessed = MultiDenseVectorInternal::new(preprocessed, vector.dim);
                let converted =
                    TElement::from_float_multivector(CowMultiVector::Owned(preprocessed))
                        .to_owned();
                Ok(converted)
            })
            .unwrap();

        let packed = Cell::new(true);
        let query: TQuery = original_query
            .transform(&|original_vector| {
                let original_vector_prequantized = TElement::quantization_preprocess(
                    quantization_config,
                    TMetric::distance(),
                    Cow::Borrowed(&original_vector.flattened_vectors),
                );
                let vectors =
                    quantized_multivector_storage.encode_query(&original_vector_prequantized);
                let packed_vectors = quantized_multivector_storage
                    .inner_storage()
                    .pack_multi_query_if_faster(&vectors);
                packed.set(packed.get() && packed_vectors.is_some());
                Ok(EncodedMultiQuery {
                    vectors,
                    packed: packed_vectors,
                })
            })
            .unwrap();

        Self {
            hw: HwScale {
                cpu: size_of::<TElement>(),
                vector_io_read: usize::from(quantized_multivector_storage.is_cold()),
            },
            query,
            packed: packed.get(),
            quantized_multivector_storage,
        }
    }
}

impl<QuantizedStorage, OffsetStorage, TQuery> QueryScorer
    for QuantizedMultiCustomQueryScorer<'_, QuantizedStorage, OffsetStorage, TQuery>
where
    QuantizedStorage: quantization::EncodedVectors,
    OffsetStorage: MultivectorOffsetsStorage,
    TQuery: Query<EncodedMultiQuery<QuantizedStorage::EncodedQuery>>,
{
    fn score_stored_batch(
        &self,
        ids: &[PointOffsetType],
        scores: &mut [ScoreType],
    ) -> OperationResult<()> {
        debug_assert_eq!(ids.len(), scores.len());

        self.hw
            .vector_io_read(size_of::<MultivectorOffset>() * ids.len());

        hw::scale_cpu(self.hw.cpu, || {
            if self.packed {
                self.quantized_multivector_storage
                    .score_points_batch_packed_by(
                        ids,
                        |max_sim| {
                            self.query.score_by(|example| {
                                // `self.packed` holds only when every example is packed.
                                example
                                    .packed
                                    .as_ref()
                                    .map_or(ScoreType::NEG_INFINITY, max_sim)
                            })
                        },
                        scores,
                    )
            } else {
                self.quantized_multivector_storage.score_points_batch(
                    ids,
                    |score_fn| self.query.score_by(|example| score_fn(&example.vectors)),
                    scores,
                )
            }
        })?;
        Ok(())
    }

    fn score_internal(&self, _point_a: PointOffsetType, _point_b: PointOffsetType) -> ScoreType {
        unimplemented!("Custom scorer compares against multiple vectors, not just one")
    }

    type SupportsBytes = False;
    fn score_bytes(&self, enabled: Self::SupportsBytes, _: &[u8]) -> ScoreType {
        match enabled {}
    }
}
