use std::borrow::Cow;
use std::marker::PhantomData;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoreType};

use crate::data_types::named_vectors::CowMultiVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{
    DenseVector, MultiDenseVectorInternal, TypedDenseVector, TypedMultiDenseVector,
};
use crate::spaces::metric::Metric;
use crate::types::QuantizationConfig;
use crate::vector_storage::common::VECTOR_READ_BATCH_SIZE;
use crate::vector_storage::query::{Query, TransformInto};
use crate::vector_storage::query_scorer::QueryScorer;

pub struct QuantizedCustomQueryScorer<'a, TElement, TMetric, TEncodedQuery, TEncodedVectors, TQuery>
where
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
    TQuery: Query<TEncodedQuery>,
{
    query: TQuery,
    quantized_storage: &'a TEncodedVectors,
    phantom: PhantomData<TEncodedQuery>,
    metric: PhantomData<TMetric>,
    element: PhantomData<TElement>,
    hardware_counter: HardwareCounterCell,
}

impl<'a, TElement, TMetric, TEncodedQuery, TEncodedVectors, TQuery>
    QuantizedCustomQueryScorer<'a, TElement, TMetric, TEncodedQuery, TEncodedVectors, TQuery>
where
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
    TQuery: Query<TEncodedQuery>,
{
    pub fn new<TOriginalQuery, TInputQuery>(
        raw_query: TInputQuery,
        quantized_storage: &'a TEncodedVectors,
        quantization_config: &QuantizationConfig,
    ) -> Self
    where
        TOriginalQuery: Query<TypedDenseVector<TElement>>
            + TransformInto<TQuery, TypedDenseVector<TElement>, TEncodedQuery>
            + Clone,
        TInputQuery: Query<DenseVector>
            + TransformInto<TOriginalQuery, DenseVector, TypedDenseVector<TElement>>,
    {
        let original_query: TOriginalQuery = raw_query
            .transform(|raw_vector| {
                let preprocessed_vector = TMetric::preprocess(raw_vector);
                let original_vector = TypedDenseVector::from(TElement::slice_from_float_cow(
                    Cow::Owned(preprocessed_vector),
                ));
                Ok(original_vector)
            })
            .unwrap();
        let query: TQuery = original_query
            .transform(|original_vector| {
                let original_vector_prequantized = TElement::quantization_preprocess(
                    quantization_config,
                    TMetric::distance(),
                    &original_vector,
                );
                Ok(quantized_storage.encode_query(&original_vector_prequantized))
            })
            .unwrap();

        Self {
            query,
            quantized_storage,
            phantom: PhantomData,
            metric: PhantomData,
            element: PhantomData,
            hardware_counter: HardwareCounterCell::new(),
        }
    }

    pub fn new_multi<TOriginalQuery, TInputQuery>(
        raw_query: TInputQuery,
        quantized_storage: &'a TEncodedVectors,
        quantization_config: &QuantizationConfig,
    ) -> Self
    where
        TOriginalQuery: Query<TypedMultiDenseVector<TElement>>
            + TransformInto<TQuery, TypedMultiDenseVector<TElement>, TEncodedQuery>
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
                Ok(quantized_storage.encode_query(&original_vector_prequantized))
            })
            .unwrap();

        Self {
            query,
            quantized_storage,
            phantom: PhantomData,
            metric: PhantomData,
            element: PhantomData,
            hardware_counter: HardwareCounterCell::new(),
        }
    }
}

impl<TElement, TMetric, TEncodedQuery, TEncodedVectors, TQuery: Query<TEncodedQuery>>
    QueryScorer<[TElement]>
    for QuantizedCustomQueryScorer<'_, TElement, TMetric, TEncodedQuery, TEncodedVectors, TQuery>
where
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        self.query.score_by(|this| {
            self.quantized_storage
                .score_point(this, idx, &self.hardware_counter)
        })
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

    fn score_internal(&self, _point_a: PointOffsetType, _point_b: PointOffsetType) -> ScoreType {
        unimplemented!("Custom scorer compares against multiple vectors, not just one")
    }

    fn take_hardware_counter(&self) -> HardwareCounterCell {
        let mut counter = self.hardware_counter.take();

        counter
            .cpu_counter_mut()
            .multiplied_mut(size_of::<TElement>());

        counter
    }
}
