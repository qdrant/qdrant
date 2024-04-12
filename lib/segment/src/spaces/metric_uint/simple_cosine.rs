use common::types::ScoreType;

use crate::data_types::vectors::{
    DenseVector, FromVectorElementSlice, TypedDenseVector, VectorElementTypeByte,
};
use crate::spaces::metric::Metric;
use crate::spaces::simple::CosineMetric;
use crate::types::Distance;

impl Metric<VectorElementTypeByte> for CosineMetric {
    fn distance() -> Distance {
        Distance::Cosine
    }

    fn similarity(v1: &[VectorElementTypeByte], v2: &[VectorElementTypeByte]) -> ScoreType {
        cosine_similarity_bytes(v1, v2)
    }

    fn preprocess(vector: DenseVector) -> TypedDenseVector<VectorElementTypeByte> {
        Vec::from_vector_element_slice(&vector)
    }
}

fn cosine_similarity_bytes(
    v1: &[VectorElementTypeByte],
    v2: &[VectorElementTypeByte],
) -> ScoreType {
    let mut dot_product = 0;
    let mut norm1 = 0;
    let mut norm2 = 0;

    for (a, b) in v1.iter().zip(v2) {
        dot_product += (*a as i32) * (*b as i32);
        norm1 += (*a as i32) * (*a as i32);
        norm2 += (*b as i32) * (*b as i32);
    }

    if norm1 == 0 || norm2 == 0 {
        return 0.0;
    }

    dot_product as ScoreType / ((norm1 as ScoreType * norm2 as ScoreType).sqrt())
}
