use super::metric::Metric;

pub struct DotProductMetric {}
pub struct CosineMetric {}

impl Metric<f32> for DotProductMetric {
    fn distance(&self, v1: &Vec<f32>, v2: &Vec<f32>) -> f32 {
        v1.iter().zip(v2).map(|(a, b)| a * b ).sum()
    }
    fn preprocess(&self, vector: Vec<f32>) -> Vec<f32> {
        return vector;
    }
}


impl Metric<f32> for CosineMetric {
    fn distance(&self, v1: &Vec<f32>, v2: &Vec<f32>) -> f32 {
        v1.iter().zip(v2).map(|(a, b)| a * b ).sum()
    }
    fn preprocess(&self, vector: Vec<f32>) -> Vec<f32> {
        let length: f32 = vector.iter().map(|x| x * x).sum();
        let norm_vector: Vec<f32> = vector.iter().map(|x| x / length).collect();
        return norm_vector
    }
}

