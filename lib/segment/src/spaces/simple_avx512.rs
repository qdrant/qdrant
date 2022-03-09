use crate::types::{ScoreType, VectorElementType};

use std::arch::x86_64::*;

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
pub unsafe fn euclid_similarity_avx512f(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    let n = v1.len();
    let m = n - (n % 16);
    let mut sum512: __m512 = _mm512_setzero_ps();
    for i in (0..m).step_by(16) {
        let sub512: __m512 = _mm512_sub_ps(_mm512_loadu_ps(&v1[i]), _mm512_loadu_ps(&v2[i]));
        sum512 = _mm512_fmadd_ps(sub512, sub512, sum512);
    }
    let mut res = _mm512_mask_reduce_add_ps(u16::MAX, sum512);
    for i in m..n {
        res += (v1[i] - v2[i]).powi(2);
    }
    -res.sqrt()
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
pub unsafe fn cosine_preprocess_avx512f(vector: &[VectorElementType]) -> Vec<VectorElementType> {
    let n = vector.len();
    let m = n - (n % 16);
    let mut sum512: __m512 = _mm512_setzero_ps();
    for i in (0..m).step_by(16) {
        sum512 = _mm512_fmadd_ps(
            _mm512_loadu_ps(&vector[i]),
            _mm512_loadu_ps(&vector[i]),
            sum512,
        );
    }
    let mut length = _mm512_mask_reduce_add_ps(u16::MAX, sum512);
    for v in vector.iter().take(n).skip(m) {
        length += v.powi(2);
    }
    length = length.sqrt();
    vector.iter().map(|x| x / length).collect()
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
pub unsafe fn dot_similarity_avx512f(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    let n = v1.len();
    let m = n - (n % 16);
    let mut sum512: __m512 = _mm512_setzero_ps();
    for i in (0..m).step_by(16) {
        sum512 = _mm512_fmadd_ps(_mm512_loadu_ps(&v1[i]), _mm512_loadu_ps(&v2[i]), sum512);
    }
    let mut res = _mm512_mask_reduce_add_ps(u16::MAX, sum512);
    for i in m..n {
        res += v1[i] * v2[i];
    }
    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spaces::simple::*;

    #[cfg(target_feature = "avx512f")]
    #[test]
    fn test_spaces_avx512() {
        if is_x86_feature_detected!("avx512f") {
            let v1: Vec<f32> = vec![
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                26., 27., 28., 29., 30., 31.,
            ];
            let v2: Vec<f32> = vec![
                40., 41., 42., 43., 44., 45., 46., 47., 48., 49., 50., 51., 52., 53., 54., 55.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                56., 57., 58., 59., 60., 61.,
            ];

            let euclid_simd = unsafe { euclid_similarity_avx512f(&v1, &v2) };
            let euclid = euclid_similarity(&v1, &v2);
            assert_eq!(euclid_simd, euclid);

            let dot_simd = unsafe { dot_similarity_avx512f(&v1, &v2) };
            let dot = dot_similarity(&v1, &v2);
            assert_eq!(dot_simd, dot);

            let cosine_simd = unsafe { cosine_preprocess_avx512f(&v1) };
            let cosine = cosine_preprocess(&v1);
            assert_eq!(cosine_simd, cosine);
        } else {
            println!("avx512 test skipped");
        }
    }
}
