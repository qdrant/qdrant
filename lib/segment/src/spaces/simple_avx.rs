use crate::types::{ScoreType, VectorElementType};

use std::arch::x86_64::*;

#[target_feature(enable = "avx")]
unsafe fn hsum256_ps_avx(x: __m256) -> f32 {
    let x128: __m128 = _mm_add_ps(_mm256_extractf128_ps(x, 1), _mm256_castps256_ps128(x));
    let x64: __m128 = _mm_add_ps(x128, _mm_movehl_ps(x128, x128));
    let x32: __m128 = _mm_add_ss(x64, _mm_shuffle_ps(x64, x64, 0x55));
    _mm_cvtss_f32(x32)
}

#[target_feature(enable = "avx")]
#[target_feature(enable = "fma")]
pub unsafe fn euclid_similarity_avx(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
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

#[target_feature(enable = "avx")]
#[target_feature(enable = "fma")]
pub unsafe fn cosine_preprocess_avx(vector: &[VectorElementType]) -> Vec<VectorElementType> {
    let n = vector.len();
    let m = n - (n % 8);
    let mut sum256: __m256 = _mm256_setzero_ps();
    for i in (0..m).step_by(8) {
        sum256 = _mm256_fmadd_ps(
            _mm256_loadu_ps(&vector[i]),
            _mm256_loadu_ps(&vector[i]),
            sum256,
        );
    }
    let mut length = hsum256_ps_avx(sum256);
    for v in vector.iter().take(n).skip(m) {
        length += v.powi(2);
    }
    length = length.sqrt();
    vector.iter().map(|x| x / length).collect()
}

#[target_feature(enable = "avx")]
#[target_feature(enable = "fma")]
pub unsafe fn dot_similarity_avx(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
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
    use crate::spaces::simple::*;

    #[cfg(all(target_feature = "avx", target_feature = "fma"))]
    #[test]
    fn test_spaces_avx() {
        if is_x86_feature_detected!("avx") && is_x86_feature_detected!("fma") {
            let v1: Vec<f32> = vec![
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                26., 27., 28., 29., 30., 31.,
            ];
            let v2: Vec<f32> = vec![
                40., 41., 42., 43., 44., 45., 46., 47., 48., 49., 50., 51., 52., 53., 54., 55.,
                56., 57., 58., 59., 60., 61.,
            ];

            let euclid_simd = unsafe { euclid_similarity_avx(&v1, &v2) };
            let euclid = euclid_similarity(&v1, &v2);
            assert_eq!(euclid_simd, euclid);

            let dot_simd = unsafe { dot_similarity_avx(&v1, &v2) };
            let dot = dot_similarity(&v1, &v2);
            assert_eq!(dot_simd, dot);

            let cosine_simd = unsafe { cosine_preprocess_avx(&v1) };
            let cosine = cosine_preprocess(&v1);
            assert_eq!(cosine_simd, cosine);
        } else {
            println!("avx test skipped");
        }
    }
}
