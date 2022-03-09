use crate::types::{Distance, ScoreType, VectorElementType};

use super::metric::Metric;

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use super::simple_sse::*;

#[cfg(target_arch = "x86_64")]
use super::simple_avx::*;

#[cfg(target_arch = "x86_64")]
use super::simple_avx512::*;

#[cfg(target_arch = "aarch64")]
use super::simple_neon::*;

pub struct DotProductMetric {}

pub struct CosineMetric {}

pub struct EuclidMetric {}

impl Metric for EuclidMetric {
    fn distance(&self) -> Distance {
        Distance::Euclid
    }

    fn similarity(&self, v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        #[cfg(all(
            target_arch = "x86_64",
            target_feature = "avx512f"))]
        {
            if is_x86_feature_detected!("avx512f") {
                return unsafe { euclid_similarity_avx512f(v1, v2) };
            }
        }

        #[cfg(all(
            target_arch = "x86_64",
            target_feature = "fma",
            target_feature = "avx"))]
        {
            if is_x86_feature_detected!("avx") && is_x86_feature_detected!("fma") {
                return unsafe { euclid_similarity_avx(v1, v2) };
            }
        }

        #[cfg(all(
            any(target_arch = "x86", target_arch = "x86_64"),
            target_feature = "sse"))]
        {
            if is_x86_feature_detected!("sse") {
                return unsafe { euclid_similarity_sse(v1, v2) };
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            if std::arch::is_aarch64_feature_detected!("neon") {
                return unsafe { euclid_similarity_neon(v1, v2) };
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
        #[cfg(all(
            target_arch = "x86_64",
            target_feature = "avx512f"))]
        {
            if is_x86_feature_detected!("avx512f") {
                return unsafe { dot_similarity_avx512f(v1, v2) };
            }
        }

        #[cfg(all(
            target_arch = "x86_64",
            target_feature = "fma",
            target_feature = "avx"))]
        {
            if is_x86_feature_detected!("avx") && is_x86_feature_detected!("fma") {
                return unsafe { dot_similarity_avx(v1, v2) };
            }
        }

        #[cfg(all(
            any(target_arch = "x86", target_arch = "x86_64"),
            target_feature = "sse"))]
        {
            if is_x86_feature_detected!("sse") {
                return unsafe { dot_similarity_sse(v1, v2) };
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            if std::arch::is_aarch64_feature_detected!("neon") {
                return unsafe { dot_similarity_neon(v1, v2) };
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
        #[cfg(all(
            target_arch = "x86_64",
            target_feature = "avx512f"))]
        {
            if is_x86_feature_detected!("avx512f") {
                return unsafe { dot_similarity_avx512f(v1, v2) };
            }
        }

        #[cfg(all(
            target_arch = "x86_64",
            target_feature = "fma",
            target_feature = "avx"))]
        {
            if is_x86_feature_detected!("avx") && is_x86_feature_detected!("fma") {
                return unsafe { dot_similarity_avx(v1, v2) };
            }
        }

        #[cfg(all(
            any(target_arch = "x86", target_arch = "x86_64"),
            target_feature = "sse"))]
        {
            if is_x86_feature_detected!("sse") {
                return unsafe { dot_similarity_sse(v1, v2) };
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            if std::arch::is_aarch64_feature_detected!("neon") {
                return unsafe { dot_similarity_neon(v1, v2) };
            }
        }

        dot_similarity(v1, v2)
    }

    fn preprocess(&self, vector: &[VectorElementType]) -> Option<Vec<VectorElementType>> {
        #[cfg(all(
            target_arch = "x86_64",
            target_feature = "avx512f"))]
        {
            if is_x86_feature_detected!("avx512f") {
                return Some(unsafe { cosine_preprocess_avx512f(vector) });
            }
        }

        #[cfg(all(
            target_arch = "x86_64",
            target_feature = "fma",
            target_feature = "avx"))]
        {
            if is_x86_feature_detected!("avx") && is_x86_feature_detected!("fma") {
                return Some(unsafe { cosine_preprocess_avx(vector) });
            }
        }

        #[cfg(all(
            any(target_arch = "x86", target_arch = "x86_64"),
            target_feature = "sse"))]
        {
            if is_x86_feature_detected!("sse") {
                return Some(unsafe { cosine_preprocess_sse(vector) });
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            if std::arch::is_aarch64_feature_detected!("neon") {
                return Some(unsafe { cosine_preprocess_neon(vector) });
            }
        }

        Some(cosine_preprocess(vector))
    }
}

pub fn euclid_similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    let s: ScoreType = v1
        .iter()
        .copied()
        .zip(v2.iter().copied())
        .map(|(a, b)| (a - b).powi(2))
        .sum();
    -s.sqrt()
}

pub fn cosine_preprocess(vector: &[VectorElementType]) -> Vec<VectorElementType> {
    let mut length: f32 = vector.iter().map(|x| x * x).sum();
    length = length.sqrt();
    vector.iter().map(|x| x / length).collect()
}

pub fn dot_similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    v1.iter().zip(v2).map(|(a, b)| a * b).sum()
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
