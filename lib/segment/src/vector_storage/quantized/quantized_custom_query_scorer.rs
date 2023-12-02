use common::types::{PointOffsetType, ScoreType};

use crate::data_types::vectors::{VectorElementType, VectorType};
use crate::types::Distance;
use crate::vector_storage::query::{Query, TransformInto};
use crate::vector_storage::query_scorer::QueryScorer;

pub struct QuantizedCustomQueryScorer<
    'a,
    TEncodedQuery,
    TEncodedVectors,
    TQuery: Query<TEncodedQuery>,
    TOriginalQuery: Query<VectorType>,
> where
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    original_query: TOriginalQuery,
    query: TQuery,
    quantized_storage: &'a TEncodedVectors,
    distance: Distance,
    phantom: std::marker::PhantomData<TEncodedQuery>,
}

impl<
        'a,
        TEncodedQuery,
        TEncodedVectors,
        TQuery: Query<TEncodedQuery>,
        TOriginalQuery: Query<VectorType>
            + Clone
            + TransformInto<TOriginalQuery>
            + TransformInto<TQuery, VectorType, TEncodedQuery>,
    > QuantizedCustomQueryScorer<'a, TEncodedQuery, TEncodedVectors, TQuery, TOriginalQuery>
where
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    pub fn new(
        raw_query: TOriginalQuery,
        quantized_storage: &'a TEncodedVectors,
        distance: Distance,
    ) -> Self {
        let original_query = raw_query
            .transform(|v| Ok(distance.preprocess_vector(v)))
            .unwrap();
        let query = original_query
            .clone()
            .transform(|v: VectorType| Ok(quantized_storage.encode_query(&v)))
            .unwrap();

        Self {
            original_query,
            query,
            quantized_storage,
            distance,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<
        TEncodedQuery,
        TEncodedVectors,
        TOriginalQuery: Query<VectorType>,
        TQuery: Query<TEncodedQuery>,
    > QueryScorer<[VectorElementType]>
    for QuantizedCustomQueryScorer<'_, TEncodedQuery, TEncodedVectors, TQuery, TOriginalQuery>
where
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        self.query
            .score_by(|this| self.quantized_storage.score_point(this, idx))
    }

    fn score(&self, v2: &[VectorElementType]) -> ScoreType {
        debug_assert!(
            false,
            "This method is not expected to be called for quantized scorer"
        );
        self.original_query
            .score_by(|this| self.distance.similarity(this, v2))
    }

    fn score_internal(&self, _point_a: PointOffsetType, _point_b: PointOffsetType) -> ScoreType {
        unimplemented!("Custom scorer compares against multiple vectors, not just one")
    }
}
