use common::types::ScoreType;

use crate::data_types::vectors::{DenseVector, TypedDenseVector, VectorElementTypeByte};
use crate::spaces::metric::Metric;
use crate::spaces::simple::DotProductMetric;
use crate::types::Distance;

impl Metric<VectorElementTypeByte> for DotProductMetric {
    fn distance() -> Distance {
        Distance::Cosine
    }

    fn similarity(v1: &[VectorElementTypeByte], v2: &[VectorElementTypeByte]) -> ScoreType {
        dot_similarity_bytes(v1, v2)
    }

    fn preprocess(vector: DenseVector) -> TypedDenseVector<VectorElementTypeByte> {
        vector
            .into_iter()
            .map(|x| x as VectorElementTypeByte)
            .collect()
    }
}

pub fn dot_similarity_bytes(
    v1: &[VectorElementTypeByte],
    v2: &[VectorElementTypeByte],
) -> ScoreType {
    let mut dot_product = 0;

    for (a, b) in v1.iter().zip(v2) {
        dot_product += (*a as i32) * (*b as i32);
    }

    dot_product as ScoreType
}
