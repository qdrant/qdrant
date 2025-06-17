use std::borrow::Cow;
use std::marker::PhantomData;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoreType};

use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{DenseVector, TypedDenseVector};
use crate::spaces::metric::Metric;
use crate::types::QuantizationConfig;
use crate::vector_storage::query::{Query, TransformInto};
use crate::vector_storage::query_scorer::QueryScorer;

pub struct QuantizedCustomQueryScorer<'a, TElement, TMetric, TEncodedVectors, TQuery>
where
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TEncodedVectors: quantization::EncodedVectors,
    TQuery: Query<TEncodedVectors::EncodedQuery>,
{
    query: TQuery,
    quantized_storage: &'a TEncodedVectors,
    metric: PhantomData<TMetric>,
    element: PhantomData<TElement>,
    hardware_counter: HardwareCounterCell,
}

impl<'a, TElement, TMetric, TEncodedVectors, TQuery>
    QuantizedCustomQueryScorer<'a, TElement, TMetric, TEncodedVectors, TQuery>
where
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TEncodedVectors: quantization::EncodedVectors,
    TQuery: Query<TEncodedVectors::EncodedQuery>,
{
    pub fn new<TOriginalQuery, TInputQuery>(
        raw_query: TInputQuery,
        quantized_storage: &'a TEncodedVectors,
        quantization_config: &QuantizationConfig,
        mut hardware_counter: HardwareCounterCell,
    ) -> Self
    where
        TOriginalQuery: Query<TypedDenseVector<TElement>>
            + TransformInto<TQuery, TypedDenseVector<TElement>, TEncodedVectors::EncodedQuery>
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

        hardware_counter.set_cpu_multiplier(size_of::<TElement>());

        hardware_counter.set_vector_io_read_multiplier(usize::from(quantized_storage.is_on_disk()));

        Self {
            query,
            quantized_storage,
            metric: PhantomData,
            element: PhantomData,
            hardware_counter,
        }
    }
}

impl<TElement, TMetric, TEncodedVectors, TQuery> QueryScorer
    for QuantizedCustomQueryScorer<'_, TElement, TMetric, TEncodedVectors, TQuery>
where
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TEncodedVectors: quantization::EncodedVectors,
    TQuery: Query<TEncodedVectors::EncodedQuery>,
{
    type TVector = [TElement];

    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        // account for read outside of `score_by` because the closure is called once per example
        self.hardware_counter
            .vector_io_read()
            .incr_delta(self.quantized_storage.quantized_vector_size());
        self.query.score_by(|this| {
            self.quantized_storage
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
