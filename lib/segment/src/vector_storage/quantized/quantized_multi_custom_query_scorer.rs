use std::marker::PhantomData;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoreType};

use crate::data_types::named_vectors::CowMultiVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{MultiDenseVectorInternal, TypedMultiDenseVector};
use crate::spaces::metric::Metric;
use crate::types::QuantizationConfig;
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffset, MultivectorOffsets,
};
use crate::vector_storage::query::{Query, TransformInto};
use crate::vector_storage::query_scorer::QueryScorer;

pub struct QuantizedMultiCustomQueryScorer<'a, TElement, TMetric, TEncodedVectors, TQuery>
where
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TEncodedVectors: quantization::EncodedVectors + MultivectorOffsets,
    TQuery: Query<TEncodedVectors::EncodedQuery>,
{
    query: TQuery,
    quantized_multivector_storage: &'a TEncodedVectors,
    metric: PhantomData<TMetric>,
    element: PhantomData<TElement>,
    hardware_counter: HardwareCounterCell,
}

impl<'a, TElement, TMetric, TEncodedVectors, TQuery>
    QuantizedMultiCustomQueryScorer<'a, TElement, TMetric, TEncodedVectors, TQuery>
where
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TEncodedVectors: quantization::EncodedVectors + MultivectorOffsets,
    TQuery: Query<TEncodedVectors::EncodedQuery>,
{
    pub fn new_multi<TOriginalQuery, TInputQuery>(
        raw_query: TInputQuery,
        quantized_multivector_storage: &'a TEncodedVectors,
        quantization_config: &QuantizationConfig,
        mut hardware_counter: HardwareCounterCell,
    ) -> Self
    where
        TOriginalQuery: Query<TypedMultiDenseVector<TElement>>
            + TransformInto<TQuery, TypedMultiDenseVector<TElement>, TEncodedVectors::EncodedQuery>
            + Clone,
        TInputQuery: Query<MultiDenseVectorInternal>
            + TransformInto<TOriginalQuery, MultiDenseVectorInternal, TypedMultiDenseVector<TElement>>,
    {
        let original_query: TOriginalQuery = raw_query
            .transform(|vector| {
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

        let query: TQuery = original_query
            .transform(|original_vector| {
                let original_vector_prequantized = TElement::quantization_preprocess(
                    quantization_config,
                    TMetric::distance(),
                    &original_vector.flattened_vectors,
                );
                Ok(quantized_multivector_storage.encode_query(&original_vector_prequantized))
            })
            .unwrap();

        hardware_counter.set_cpu_multiplier(size_of::<TElement>());

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

impl<TElement, TMetric, TEncodedVectors, TQuery> QueryScorer
    for QuantizedMultiCustomQueryScorer<'_, TElement, TMetric, TEncodedVectors, TQuery>
where
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TEncodedVectors: quantization::EncodedVectors + MultivectorOffsets,
    TQuery: Query<TEncodedVectors::EncodedQuery>,
{
    type TVector = [TElement];

    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        let multi_vector_offset = self.quantized_multivector_storage.get_offset(idx);
        let sub_vectors_count = multi_vector_offset.count as usize;
        // compute vector IO read once for all examples
        self.hardware_counter.vector_io_read().incr_delta(
            size_of::<MultivectorOffset>()
                + self.quantized_multivector_storage.quantized_vector_size() * sub_vectors_count,
        );
        self.query.score_by(|this| {
            // quantized multivector storage handles hardware counter to batch vector IO
            self.quantized_multivector_storage
                .score_point(this, idx, &self.hardware_counter)
        })
    }

    fn score(&self, _v2: &[TElement]) -> ScoreType {
        unimplemented!("This method is not expected to be called for quantized scorer");
    }

    fn score_internal(&self, _point_a: PointOffsetType, _point_b: PointOffsetType) -> ScoreType {
        unimplemented!("Custom scorer compares against multiple vectors, not just one")
    }
}
