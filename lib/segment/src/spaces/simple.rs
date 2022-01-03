#[cfg(target_arch = "x86")]
use std::arch::x86::*;

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

use crate::types::{Distance, ScoreType, VectorElementType};

use super::metric::Metric;

pub struct DotProductMetric {}

pub struct CosineMetric {}

pub struct EuclidMetric {}

fn euclid_similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    let s: ScoreType = v1
        .iter()
        .cloned()
        .zip(v2.iter().cloned())
        .map(|(a, b)| (a - b).powi(2))
        .sum();
    -s.sqrt()
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn euclid_similarity_avx2(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    unsafe {
        let mut sum256: __m256 = _mm256_setzero_ps();
        for i in (0..n).step_by(8) {
            let sub256: __m256 = _mm256_sub_ps(_mm256_loadu_ps(&v1[i]), _mm256_loadu_ps(&v2[i]));
            sum256 = _mm256_fmadd_ps(sub256, sub256, sum256);
        }
        let res: f32 = hsum256_ps_avx(sum256);
        for i in (n - (n % 8)..n) {
            res += (v1[i] - v2[i]).powi(2);
        }
        -res.sqrt()
    }
}

fn dot_similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    v1.iter().zip(v2).map(|(a, b)| a * b).sum()
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn dot_similarity_avx2(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    unsafe {
        let mut sum256: __m256 = _mm256_setzero_ps();
        for i in (0..n).step_by(8) {
            sum256 = _mm256_fmadd_ps(_mm256_loadu_ps(&v1[i]), _mm256_loadu_ps(&v2[i]), sum256);
        }
        let res: f32 = hsum256_ps_avx(sum256);
        for i in (n - (n % 8)..n) {
            res += v1[i] * v2[i];
        }
        res
    }
}

fn cosine_preprocess(vector: &[VectorElementType]) -> Vec<VectorElementType> {
    let mut length: f32 = vector.iter().map(|x| x * x).sum();
    length = length.sqrt();
    vector.iter().map(|x| x / length).collect()
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn cosine_preprocess_avx2(vector: &[VectorElementType]) -> Vec<VectorElementType> {
    let length = unsafe {
        let mut sum256: __m256 = _mm256_setzero_ps();
        for i in (0..n).step_by(8) {
            sum256 = _mm256_fmadd_ps(
                _mm256_loadu_ps(&vector[i]),
                _mm256_loadu_ps(&vector[i]), sum256);
        }
        let res: f32 = hsum256_ps_avx(sum256);
        for i in (n - (n % 8)..n) {
            res += vector[i].powi(2);
        }
        res
    }.sqrt();
    vector.iter().map(|x| x / length).collect()
}

impl Metric for EuclidMetric {
    fn distance(&self) -> Distance {
        Distance::Euclid
    }

    fn similarity(&self, v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if is_x86_feature_detected!("avx2") == 0 {
                return euclid_similarity_avx2(v1, v2);
            }
        }
        euclid_similarity(v1, v2)
    }

    fn preprocess(&self, _vector: &[VectorElementType]) -> Option<Vec<VectorElementType>> {
        None
    }
}

impl Metric for DotProductMetric {
    fn distance(&self) -> Distance {
        Distance::Dot
    }

    fn similarity(&self, v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if is_x86_feature_detected!("avx2") == 0 {
                return dot_similarity_avx2(v1, v2);
            }
        }
        dot_similarity(v1, v2)
    }

    fn preprocess(&self, _vector: &[VectorElementType]) -> Option<Vec<VectorElementType>> {
        None
    }
}

impl Metric for CosineMetric {
    fn distance(&self) -> Distance {
        Distance::Cosine
    }

    fn similarity(&self, v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if is_x86_feature_detected!("avx2") == 0 {
                return dot_similarity_avx2(v1, v2);
            }
        }
        dot_similarity(v1, v2)
    }

    fn preprocess(&self, vector: &[VectorElementType]) -> Option<Vec<VectorElementType>> {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if is_x86_feature_detected!("avx2") == 0 {
                return Some(cosine_preprocess_avx2(vector));
            }
        }
        Some(cosine_preprocess(vector))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_preprocessing() {
        let metric = CosineMetric {};
        let res = metric.preprocess(&[0.0, 0.0, 0.0, 0.0]);
        eprintln!("res = {:#?}", res);
    }
}
