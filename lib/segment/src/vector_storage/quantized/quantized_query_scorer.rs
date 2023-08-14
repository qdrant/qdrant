use crate::data_types::vectors::VectorElementType;
use crate::types::{Distance, PointOffsetType, ScoreType};
use crate::vector_storage::query_scorer::QueryScorer;

pub struct QuantizedQueryScorer<'a, TEncodedQuery, TEncodedVectors>
where
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    original_query: Vec<VectorElementType>,
    query: TEncodedQuery,
    quantized_data: &'a TEncodedVectors,
    distance: Distance,
}

impl<'a, TEncodedQuery, TEncodedVectors> QuantizedQueryScorer<'a, TEncodedQuery, TEncodedVectors>
where
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    pub fn new(
        original_query: Vec<VectorElementType>,
        query: TEncodedQuery,
        quantized_data: &'a TEncodedVectors,
        distance: Distance,
    ) -> Self {
        Self {
            original_query,
            query,
            quantized_data,
            distance,
        }
    }
}

impl<TEncodedQuery, TEncodedVectors> QueryScorer
    for QuantizedQueryScorer<'_, TEncodedQuery, TEncodedVectors>
where
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        self.quantized_data.score_point(&self.query, idx)
    }

    fn score(&self, v2: &[VectorElementType]) -> ScoreType {
        debug_assert!(
            false,
            "This method is not expected to be called for quantized scorer"
        );
        self.distance.similarity(&self.original_query, v2)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.quantized_data.score_internal(point_a, point_b)
    }
}
