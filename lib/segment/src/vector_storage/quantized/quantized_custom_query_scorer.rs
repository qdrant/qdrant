use std::borrow::Cow;

use common::ambient::hw::{self, HwScale};
use common::types::{PointOffsetType, ScoreType};

use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{DenseVector, TypedDenseVector};
use crate::spaces::metric::Metric;
use crate::types::QuantizationConfig;
use crate::vector_storage::query::{Query, TransformInto};
use crate::vector_storage::query_scorer::QueryScorer;

pub struct QuantizedCustomQueryScorer<'a, TEncodedVectors, TQuery>
where
    TEncodedVectors: quantization::EncodedVectors,
    TQuery: Query<TEncodedVectors::EncodedQuery>,
{
    hw: HwScale,
    query: TQuery,
    quantized_storage: &'a TEncodedVectors,
}

impl<'a, TEncodedVectors, TQuery> QuantizedCustomQueryScorer<'a, TEncodedVectors, TQuery>
where
    TEncodedVectors: quantization::EncodedVectors,
    TQuery: Query<TEncodedVectors::EncodedQuery>,
{
    pub fn new<TElement, TMetric, TOriginalQuery, TInputQuery>(
        raw_query: TInputQuery,
        quantized_storage: &'a TEncodedVectors,
        quantization_config: &QuantizationConfig,
    ) -> Self
    where
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement>,
        TOriginalQuery: Query<TypedDenseVector<TElement>>
            + TransformInto<TQuery, TypedDenseVector<TElement>, TEncodedVectors::EncodedQuery>
            + Clone,
        TInputQuery: Query<DenseVector>
            + TransformInto<TOriginalQuery, DenseVector, TypedDenseVector<TElement>>,
    {
        let original_query: TOriginalQuery = raw_query
            .transform(&|raw_vector| {
                let preprocessed_vector = TMetric::preprocess(raw_vector);
                let original_vector = TypedDenseVector::from(TElement::slice_from_float_cow(
                    Cow::Owned(preprocessed_vector),
                ));
                Ok(original_vector)
            })
            .unwrap();
        let query: TQuery = original_query
            .transform(&|original_vector| {
                let original_vector_prequantized = TElement::quantization_preprocess(
                    quantization_config,
                    TMetric::distance(),
                    Cow::Borrowed(&original_vector),
                );
                Ok(quantized_storage.encode_query(&original_vector_prequantized))
            })
            .unwrap();

        Self {
            hw: HwScale {
                cpu: size_of::<TElement>(),
                vector_io_read: usize::from(quantized_storage.is_on_disk()),
            },
            query,
            quantized_storage,
        }
    }
}

impl<TEncodedVectors, TQuery> QueryScorer
    for QuantizedCustomQueryScorer<'_, TEncodedVectors, TQuery>
where
    TEncodedVectors: quantization::EncodedVectors,
    TQuery: Query<TEncodedVectors::EncodedQuery>,
{
    fn score_stored_batch(&self, ids: &[PointOffsetType], scores: &mut [ScoreType]) {
        debug_assert_eq!(ids.len(), scores.len());

        let storage = self.quantized_storage;

        self.hw
            .vector_io_read(ids.len() * storage.quantized_vector_size());

        hw::scale_cpu(self.hw.cpu, || {
            storage.for_each_batch(ids, |idx, vector| {
                scores[idx] = self.query.score_by(|query| {
                    storage.score(query, &vector) // inhibit `rustfmt`
                });
            })
        });
    }

    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        // account for read outside of `score_by` because the closure is called once per example
        self.hw
            .vector_io_read(self.quantized_storage.quantized_vector_size());
        hw::scale_cpu(self.hw.cpu, || {
            self.query
                .score_by(|this| self.quantized_storage.score_point(this, idx))
        })
    }

    fn score_internal(&self, _point_a: PointOffsetType, _point_b: PointOffsetType) -> ScoreType {
        unimplemented!("Custom scorer compares against multiple vectors, not just one")
    }

    type SupportsBytes = TEncodedVectors::SupportsBytes;
    fn score_bytes(&self, enabled: Self::SupportsBytes, bytes: &[u8]) -> ScoreType {
        hw::scale_cpu(self.hw.cpu, || {
            self.query
                .score_by(|this| self.quantized_storage.score_bytes(enabled, this, bytes))
        })
    }
}
