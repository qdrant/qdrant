use common::types::ScoreType;

use crate::data_types::vectors::{
    DenseVector, FromVectorElementSlice, TypedDenseVector, VectorElementTypeByte,
};
use crate::spaces::metric::Metric;
use crate::spaces::simple::ManhattanMetric;
use crate::types::Distance;

impl Metric<VectorElementTypeByte> for ManhattanMetric {
    fn distance() -> Distance {
        Distance::Cosine
    }

    fn similarity(v1: &[VectorElementTypeByte], v2: &[VectorElementTypeByte]) -> ScoreType {
        manhattan_similarity_bytes(v1, v2)
    }

    fn preprocess(vector: DenseVector) -> TypedDenseVector<VectorElementTypeByte> {
        Vec::from_vector_element_slice(&vector)
    }
}

fn manhattan_similarity_bytes(
    v1: &[VectorElementTypeByte],
    v2: &[VectorElementTypeByte],
) -> ScoreType {
    -v1.iter()
        .zip(v2)
        .map(|(a, b)| (*a as i32 - *b as i32).abs())
        .sum::<i32>() as ScoreType
}
