use common::types::{PointOffsetType, ScoreType};

use crate::data_types::vectors::{DenseVector, VectorElementType};
use crate::types::Distance;
use crate::vector_storage::query_scorer::QueryScorer;

pub struct QuantizedQueryScorer<'a, TEncodedQuery, TEncodedVectors>
where
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    original_query: DenseVector,
    query: TEncodedQuery,
    quantized_data: &'a TEncodedVectors,
    distance: Distance,
}

impl<'a, TEncodedQuery, TEncodedVectors> QuantizedQueryScorer<'a, TEncodedQuery, TEncodedVectors>
where
    TEncodedVectors: quantization::EncodedVectors<TEncodedQuery>,
{
    pub fn new(
        raw_query: DenseVector,
        quantized_data: &'a TEncodedVectors,
        distance: Distance,
    ) -> Self {
        let original_query = distance.preprocess_vector(raw_query);
        let query = quantized_data.encode_query(&original_query);

        Self {
            original_query,
            query,
            quantized_data,
            distance,
        }
    }
}

impl<TEncodedQuery, TEncodedVectors> QueryScorer<[VectorElementType]>
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
