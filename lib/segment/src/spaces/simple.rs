use super::metric::Metric;
use crate::types::ScoreType;

pub struct DotProductMetric {}

pub struct CosineMetric {}

impl Metric<f32> for DotProductMetric {
    fn similarity(&self, v1: &[f32], v2: &[f32]) -> ScoreType {
        let ip: f32 = v1.iter().zip(v2).map(|(a, b)| a * b).sum();
        return ip
    }

    fn similarity_batch(&self, vector: &[f32], other_vectors: &[&[f32]]) -> Vec<ScoreType> {
        other_vectors.iter().map(|v2| self.similarity(vector, *v2)).collect()
    }

    fn preprocess(&self, vector: Vec<f32>) -> Vec<f32> {
        return vector;
    }
}


impl Metric<f32> for CosineMetric {
    fn similarity(&self, v1: &[f32], v2: &[f32]) -> ScoreType {
        let cos: f32 = v1.iter().zip(v2).map(|(a, b)| a * b).sum();
        return cos
    }

    fn similarity_batch(&self, vector: &[f32], other_vectors: &[&[f32]]) -> Vec<ScoreType> {
        other_vectors.iter().map(|v2| self.similarity(vector, *v2)).collect()
    }

    fn preprocess(&self, vector: Vec<f32>) -> Vec<f32> {
        let length: f32 = vector.iter().map(|x| x * x).sum();
        let norm_vector = vector.iter().map(|x| x / length).collect();
        return norm_vector;
    }
}

