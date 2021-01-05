use ndarray::Array1;

use crate::types::{Distance, ScoreType, VectorElementType};

use super::metric::Metric;

pub struct DotProductMetric {}

pub struct CosineMetric {}

impl Metric for DotProductMetric {
    fn distance(&self) -> Distance {
        Distance::Dot
    }

    fn similarity(&self, v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        let ip: f32 = v1.iter().zip(v2).map(|(a, b)| a * b).sum();
        return ip;
    }

    fn blas_similarity(&self, v1: &Array1<VectorElementType>, v2: &Array1<VectorElementType>) -> ScoreType {
        v1.dot(v2)
    }

    fn preprocess(&self, vector: Vec<VectorElementType>) -> Vec<VectorElementType> {
        return vector;
    }
}

impl Metric for CosineMetric {
    fn distance(&self) -> Distance {
        Distance::Cosine
    }

    fn similarity(&self, v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        let cos: VectorElementType = v1.iter().zip(v2).map(|(a, b)| a * b).sum();
        return cos;
    }

    fn blas_similarity(&self, v1: &Array1<VectorElementType>, v2: &Array1<VectorElementType>) -> ScoreType {
        v1.dot(v2)
    }

    fn preprocess(&self, vector: Vec<VectorElementType>) -> Vec<VectorElementType> {
        let length: f32 = vector.iter().map(|x| x * x).sum();
        let norm_vector = vector.iter().map(|x| x / length).collect();
        return norm_vector;
    }
}
