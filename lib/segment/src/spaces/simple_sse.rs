#[cfg(target_arch = "x86")]
use std::arch::x86::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

use common::types::ScoreType;

use crate::data_types::vectors::{VectorElementType, VectorType};

#[target_feature(enable = "sse")]
unsafe fn hsum128_ps_sse(x: __m128) -> f32 {
    let x64: __m128 = _mm_add_ps(x, _mm_movehl_ps(x, x));
    let x32: __m128 = _mm_add_ss(x64, _mm_shuffle_ps(x64, x64, 0x55));
    _mm_cvtss_f32(x32)
}

#[target_feature(enable = "sse")]
pub(crate) unsafe fn euclid_similarity_sse(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    let n = v1.len();
    let m = n - (n % 16);
    let mut ptr1: *const f32 = v1.as_ptr();
    let mut ptr2: *const f32 = v2.as_ptr();
    let mut sum128_1: __m128 = _mm_setzero_ps();
    let mut sum128_2: __m128 = _mm_setzero_ps();
    let mut sum128_3: __m128 = _mm_setzero_ps();
    let mut sum128_4: __m128 = _mm_setzero_ps();
    let mut i: usize = 0;
    while i < m {
        let sub128_1 = _mm_sub_ps(_mm_loadu_ps(ptr1), _mm_loadu_ps(ptr2));
        sum128_1 = _mm_add_ps(_mm_mul_ps(sub128_1, sub128_1), sum128_1);

        let sub128_2 = _mm_sub_ps(_mm_loadu_ps(ptr1.add(4)), _mm_loadu_ps(ptr2.add(4)));
        sum128_2 = _mm_add_ps(_mm_mul_ps(sub128_2, sub128_2), sum128_2);

        let sub128_3 = _mm_sub_ps(_mm_loadu_ps(ptr1.add(8)), _mm_loadu_ps(ptr2.add(8)));
        sum128_3 = _mm_add_ps(_mm_mul_ps(sub128_3, sub128_3), sum128_3);

        let sub128_4 = _mm_sub_ps(_mm_loadu_ps(ptr1.add(12)), _mm_loadu_ps(ptr2.add(12)));
        sum128_4 = _mm_add_ps(_mm_mul_ps(sub128_4, sub128_4), sum128_4);

        ptr1 = ptr1.add(16);
        ptr2 = ptr2.add(16);
        i += 16;
    }

    let mut result = hsum128_ps_sse(sum128_1)
        + hsum128_ps_sse(sum128_2)
        + hsum128_ps_sse(sum128_3)
        + hsum128_ps_sse(sum128_4);
    for i in 0..n - m {
        result += (*ptr1.add(i) - *ptr2.add(i)).powi(2);
    }
    -result
}

#[target_feature(enable = "sse")]
pub(crate) unsafe fn manhattan_similarity_sse(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    let mask: __m128 = _mm_set1_ps(-0.0f32); // 1 << 31 used to clear sign bit to mimic abs

    let n = v1.len();
    let m = n - (n % 16);
    let mut ptr1: *const f32 = v1.as_ptr();
    let mut ptr2: *const f32 = v2.as_ptr();
    let mut sum128_1: __m128 = _mm_setzero_ps();
    let mut sum128_2: __m128 = _mm_setzero_ps();
    let mut sum128_3: __m128 = _mm_setzero_ps();
    let mut sum128_4: __m128 = _mm_setzero_ps();
    let mut i: usize = 0;
    while i < m {
        let sub128_1 = _mm_sub_ps(_mm_loadu_ps(ptr1), _mm_loadu_ps(ptr2));
        sum128_1 = _mm_add_ps(_mm_andnot_ps(mask, sub128_1), sum128_1);

        let sub128_2 = _mm_sub_ps(_mm_loadu_ps(ptr1.add(4)), _mm_loadu_ps(ptr2.add(4)));
        sum128_2 = _mm_add_ps(_mm_andnot_ps(mask, sub128_2), sum128_2);

        let sub128_3 = _mm_sub_ps(_mm_loadu_ps(ptr1.add(8)), _mm_loadu_ps(ptr2.add(8)));
        sum128_3 = _mm_add_ps(_mm_andnot_ps(mask, sub128_3), sum128_3);

        let sub128_4 = _mm_sub_ps(_mm_loadu_ps(ptr1.add(12)), _mm_loadu_ps(ptr2.add(12)));
        sum128_4 = _mm_add_ps(_mm_andnot_ps(mask, sub128_4), sum128_4);

        ptr1 = ptr1.add(16);
        ptr2 = ptr2.add(16);
        i += 16;
    }

    let mut result = hsum128_ps_sse(sum128_1)
        + hsum128_ps_sse(sum128_2)
        + hsum128_ps_sse(sum128_3)
        + hsum128_ps_sse(sum128_4);
    for i in 0..n - m {
        result += (*ptr1.add(i) - *ptr2.add(i)).abs();
    }
    -result
}

#[target_feature(enable = "sse")]
pub(crate) unsafe fn cosine_preprocess_sse(vector: VectorType) -> VectorType {
    let n = vector.len();
    let m = n - (n % 16);
    let mut ptr: *const f32 = vector.as_ptr();
    let mut sum128_1: __m128 = _mm_setzero_ps();
    let mut sum128_2: __m128 = _mm_setzero_ps();
    let mut sum128_3: __m128 = _mm_setzero_ps();
    let mut sum128_4: __m128 = _mm_setzero_ps();

    let mut i: usize = 0;
    while i < m {
        let m128_1 = _mm_loadu_ps(ptr);
        sum128_1 = _mm_add_ps(_mm_mul_ps(m128_1, m128_1), sum128_1);

        let m128_2 = _mm_loadu_ps(ptr.add(4));
        sum128_2 = _mm_add_ps(_mm_mul_ps(m128_2, m128_2), sum128_2);

        let m128_3 = _mm_loadu_ps(ptr.add(8));
        sum128_3 = _mm_add_ps(_mm_mul_ps(m128_3, m128_3), sum128_3);

        let m128_4 = _mm_loadu_ps(ptr.add(12));
        sum128_4 = _mm_add_ps(_mm_mul_ps(m128_4, m128_4), sum128_4);

        ptr = ptr.add(16);
        i += 16;
    }

    let mut length = hsum128_ps_sse(sum128_1)
        + hsum128_ps_sse(sum128_2)
        + hsum128_ps_sse(sum128_3)
        + hsum128_ps_sse(sum128_4);
    for i in 0..n - m {
        length += (*ptr.add(i)).powi(2);
    }
    if length < f32::EPSILON {
        return vector;
    }
    length = length.sqrt();
    vector.into_iter().map(|x| x / length).collect()
}

#[target_feature(enable = "sse")]
pub(crate) unsafe fn dot_similarity_sse(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    let n = v1.len();
    let m = n - (n % 16);
    let mut ptr1: *const f32 = v1.as_ptr();
    let mut ptr2: *const f32 = v2.as_ptr();
    let mut sum128_1: __m128 = _mm_setzero_ps();
    let mut sum128_2: __m128 = _mm_setzero_ps();
    let mut sum128_3: __m128 = _mm_setzero_ps();
    let mut sum128_4: __m128 = _mm_setzero_ps();

    let mut i: usize = 0;
    while i < m {
        sum128_1 = _mm_add_ps(_mm_mul_ps(_mm_loadu_ps(ptr1), _mm_loadu_ps(ptr2)), sum128_1);

        sum128_2 = _mm_add_ps(
            _mm_mul_ps(_mm_loadu_ps(ptr1.add(4)), _mm_loadu_ps(ptr2.add(4))),
            sum128_2,
        );

        sum128_3 = _mm_add_ps(
            _mm_mul_ps(_mm_loadu_ps(ptr1.add(8)), _mm_loadu_ps(ptr2.add(8))),
            sum128_3,
        );

        sum128_4 = _mm_add_ps(
            _mm_mul_ps(_mm_loadu_ps(ptr1.add(12)), _mm_loadu_ps(ptr2.add(12))),
            sum128_4,
        );

        ptr1 = ptr1.add(16);
        ptr2 = ptr2.add(16);
        i += 16;
    }

    let mut result = hsum128_ps_sse(sum128_1)
        + hsum128_ps_sse(sum128_2)
        + hsum128_ps_sse(sum128_3)
        + hsum128_ps_sse(sum128_4);
    for i in 0..n - m {
        result += (*ptr1.add(i)) * (*ptr2.add(i));
    }
    result
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_spaces_sse() {
        use super::*;
        use crate::spaces::simple::*;

        if is_x86_feature_detected!("sse") {
            let v1: Vec<f32> = vec![
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                26., 27., 28., 29., 30., 31.,
            ];
            let v2: Vec<f32> = vec![
                40., 41., 42., 43., 44., 45., 46., 47., 48., 49., 50., 51., 52., 53., 54., 55.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                56., 57., 58., 59., 60., 61.,
            ];

            let euclid_simd = unsafe { euclid_similarity_sse(&v1, &v2) };
            let euclid = euclid_similarity(&v1, &v2);
            assert_eq!(euclid_simd, euclid);

            let manhattan_simd = unsafe { manhattan_similarity_sse(&v1, &v2) };
            let manhattan = manhattan_similarity(&v1, &v2);
            assert_eq!(manhattan_simd, manhattan);

            let dot_simd = unsafe { dot_similarity_sse(&v1, &v2) };
            let dot = dot_similarity(&v1, &v2);
            assert_eq!(dot_simd, dot);

            let cosine_simd = unsafe { cosine_preprocess_sse(v1.clone()) };
            let cosine = cosine_preprocess(v1);
            assert_eq!(cosine_simd, cosine);
        } else {
            println!("sse test skipped");
        }
    }
}
