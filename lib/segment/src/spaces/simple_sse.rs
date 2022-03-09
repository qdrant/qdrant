use crate::types::{ScoreType, VectorElementType};

#[cfg(target_arch = "x86")]
use std::arch::x86::*;

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "sse")]
unsafe fn hsum128_ps_sse(x: __m128) -> f32 {
    let x64: __m128 = _mm_add_ps(x, _mm_movehl_ps(x, x));
    let x32: __m128 = _mm_add_ss(x64, _mm_shuffle_ps(x64, x64, 0x55));
    _mm_cvtss_f32(x32)
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "sse")]
pub unsafe fn euclid_similarity_sse(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    let n = v1.len();
    let m = n - (n % 4);
    let mut sum128: __m128 = _mm_setzero_ps();
    for i in (0..m).step_by(4) {
        let sub128: __m128 = _mm_sub_ps(_mm_loadu_ps(&v1[i]), _mm_loadu_ps(&v2[i]));
        let a = _mm_mul_ps(sub128, sub128);
        sum128 = _mm_add_ps(a, sum128);
    }
    let mut res = hsum128_ps_sse(sum128);
    for i in m..n {
        res += (v1[i] - v2[i]).powi(2);
    }
    -res.sqrt()
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "sse")]
pub unsafe fn cosine_preprocess_sse(vector: &[VectorElementType]) -> Vec<VectorElementType> {
    let n = vector.len();
    let m = n - (n % 4);
    let mut sum128: __m128 = _mm_setzero_ps();
    for i in (0..m).step_by(4) {
        let a = _mm_loadu_ps(&vector[i]);
        let b = _mm_mul_ps(a, a);
        sum128 = _mm_add_ps(b, sum128);
    }
    let mut length = hsum128_ps_sse(sum128);
    for v in vector.iter().take(n).skip(m) {
        length += v.powi(2);
    }
    length = length.sqrt();
    vector.iter().map(|x| x / length).collect()
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "sse")]
pub unsafe fn dot_similarity_sse(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    let n = v1.len();
    let m = n - (n % 4);
    let mut sum128: __m128 = _mm_setzero_ps();
    for i in (0..m).step_by(4) {
        let a = _mm_loadu_ps(&v1[i]);
        let b = _mm_loadu_ps(&v2[i]);
        let c = _mm_mul_ps(a, b);
        sum128 = _mm_add_ps(c, sum128);
    }
    let mut res = hsum128_ps_sse(sum128);
    for i in m..n {
        res += v1[i] * v2[i];
    }
    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spaces::simple::*;

    #[cfg(target_feature = "sse")]
    #[test]
    fn test_spaces_sse() {
        if is_x86_feature_detected!("sse") {
            let v1: Vec<f32> = vec![
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                26., 27., 28., 29., 30., 31.,
            ];
            let v2: Vec<f32> = vec![
                40., 41., 42., 43., 44., 45., 46., 47., 48., 49., 50., 51., 52., 53., 54., 55.,
                56., 57., 58., 59., 60., 61.,
            ];

            let euclid_simd = unsafe { euclid_similarity_sse(&v1, &v2) };
            let euclid = euclid_similarity(&v1, &v2);
            assert_eq!(euclid_simd, euclid);

            let dot_simd = unsafe { dot_similarity_sse(&v1, &v2) };
            let dot = dot_similarity(&v1, &v2);
            assert_eq!(dot_simd, dot);

            let cosine_simd = unsafe { cosine_preprocess_sse(&v1) };
            let cosine = cosine_preprocess(&v1);
            assert_eq!(cosine_simd, cosine);
        } else {
            println!("sse test skipped");
        }
    }
}
