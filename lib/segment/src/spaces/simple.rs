#[cfg(target_arch = "x86")]
use std::arch::x86::*;

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

use crate::types::{Distance, ScoreType, VectorElementType};

use super::metric::Metric;

pub struct DotProductMetric {}

pub struct CosineMetric {}

pub struct EuclidMetric {}

impl Metric for EuclidMetric {
    fn distance(&self) -> Distance {
        Distance::Euclid
    }

    fn similarity(&self, v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if is_x86_feature_detected!("avx2") {
                return unsafe { euclid_similarity_avx2(v1, v2) };
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
            if is_x86_feature_detected!("avx2") {
                return unsafe { dot_similarity_avx2(v1, v2) };
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
            if is_x86_feature_detected!("avx2") {
                return unsafe { dot_similarity_avx2(v1, v2) };
            }
        }
        dot_similarity(v1, v2)
    }

    fn preprocess(&self, vector: &[VectorElementType]) -> Option<Vec<VectorElementType>> {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if is_x86_feature_detected!("avx2") {
                return Some(unsafe { cosine_preprocess_avx2(vector) });
            }
        }
        Some(cosine_preprocess(vector))
    }
}

fn euclid_similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    let s: ScoreType = v1
        .iter()
        .copied()
        .zip(v2.iter().copied())
        .map(|(a, b)| (a - b).powi(2))
        .sum();
    -s.sqrt()
}

fn cosine_preprocess(vector: &[VectorElementType]) -> Vec<VectorElementType> {
    let mut length: f32 = vector.iter().map(|x| x * x).sum();
    length = length.sqrt();
    vector.iter().map(|x| x / length).collect()
}

fn dot_similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    v1.iter().zip(v2).map(|(a, b)| a * b).sum()
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn hsum256_ps_avx(x: __m256) -> f32 {
    /* ( x3+x7, x2+x6, x1+x5, x0+x4 ) */
    let x128 : __m128 = _mm_add_ps(_mm256_extractf128_ps(x, 1), _mm256_castps256_ps128(x));
    /* ( -, -, x1+x3+x5+x7, x0+x2+x4+x6 ) */
    let x64 : __m128 = _mm_add_ps(x128, _mm_movehl_ps(x128, x128));
    /* ( -, -, -, x0+x1+x2+x3+x4+x5+x6+x7 ) */
    let x32 : __m128 = _mm_add_ss(x64, _mm_shuffle_ps(x64, x64, 0x55));
    /* Conversion to float is a no-op on x86-64 */
    return _mm_cvtss_f32(x32);
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn euclid_similarity_avx2(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    let n = v1.len();
    let m = n - (n % 8);
    let mut sum256: __m256 = _mm256_setzero_ps();
    for i in (0..m).step_by(8) {
        let sub256: __m256 = _mm256_sub_ps(_mm256_loadu_ps(&v1[i]), _mm256_loadu_ps(&v2[i]));
        sum256 = _mm256_fmadd_ps(sub256, sub256, sum256);
    }
    let mut res = hsum256_ps_avx(sum256);
    for i in m..n {
        res += (v1[i] - v2[i]).powi(2);
    }
    -res.sqrt()
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn cosine_preprocess_avx2(vector: &[VectorElementType]) -> Vec<VectorElementType> {
    let n = vector.len();
    let m = n - (n % 8);
    let mut sum256: __m256 = _mm256_setzero_ps();
    for i in (0..m).step_by(8) {
        sum256 = _mm256_fmadd_ps(
            _mm256_loadu_ps(&vector[i]),
            _mm256_loadu_ps(&vector[i]), sum256);
    }
    let mut length = hsum256_ps_avx(sum256);
    for i in m..n {
        length += vector[i].powi(2);
    }
    length = length.sqrt();
    vector.iter().map(|x| x / length).collect()
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn dot_similarity_avx2(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    let n = v1.len();
    let m = n - (n % 8);
    let mut sum256: __m256 = _mm256_setzero_ps();
    for i in (0..m).step_by(8) {
        sum256 = _mm256_fmadd_ps(_mm256_loadu_ps(&v1[i]), _mm256_loadu_ps(&v2[i]), sum256);
    }
    let mut res = hsum256_ps_avx(sum256);
    for i in m..n {
        res += v1[i] * v2[i];
    }
    res
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

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    #[test]
    fn test_avx2() {
        if is_x86_feature_detected!("avx2") {
            let v1 : Vec<f32> = vec![10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25., 26., 27., 28., 29., 30.];
            let v2 : Vec<f32> = vec![40., 41., 42., 43., 44., 45., 46., 47., 48., 49., 50., 51., 52., 53., 54., 55., 56., 57., 58., 59., 60.];

            let euclid_avx2 = unsafe { euclid_similarity_avx2(&v1, &v2) };
            let euclid = euclid_similarity(&v1, &v2);
            assert_eq!(euclid_avx2, euclid);

            let dot_avx2 = unsafe { dot_similarity_avx2(&v1, &v2) };
            let dot = dot_similarity(&v1, &v2);
            assert_eq!(dot_avx2, dot);

            let cosine_avx2 = unsafe { cosine_preprocess_avx2(&v1) };
            let cosine = cosine_preprocess(&v1);
            assert_eq!(cosine_avx2, cosine);
        } else {
            println!("AVX2 test skiped");
        }
    }
}
