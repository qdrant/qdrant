use std::arch::x86_64::*;

use common::types::ScoreType;

use super::tools::is_length_zero_or_normalized;
use crate::data_types::vectors::{DenseVector, VectorElementType};

#[target_feature(enable = "avx")]
#[allow(clippy::missing_safety_doc)]
pub unsafe fn hsum256_ps_avx(x: __m256) -> f32 {
    let lr_sum: __m128 = _mm_add_ps(_mm256_extractf128_ps(x, 1), _mm256_castps256_ps128(x));
    let hsum = _mm_hadd_ps(lr_sum, lr_sum);
    let p1 = _mm_extract_ps(hsum, 0);
    let p2 = _mm_extract_ps(hsum, 1);
    f32::from_bits(p1 as u32) + f32::from_bits(p2 as u32)
}

/// Calculates the hsum (horizontal sum) of four 32 byte registers.
#[target_feature(enable = "avx")]
#[allow(clippy::missing_safety_doc)]
pub unsafe fn four_way_hsum(a: __m256, b: __m256, c: __m256, d: __m256) -> f32 {
    unsafe {
        let sum1 = _mm256_add_ps(a, b);
        let sum2 = _mm256_add_ps(c, d);
        let total = _mm256_add_ps(sum1, sum2);
        hsum256_ps_avx(total)
    }
}

#[target_feature(enable = "avx")]
#[target_feature(enable = "fma")]
pub(crate) unsafe fn euclid_similarity_avx(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    unsafe {
        let n = v1.len();
        let m = n - (n % 32);
        let mut ptr1: *const f32 = v1.as_ptr();
        let mut ptr2: *const f32 = v2.as_ptr();
        let mut sum256_1: __m256 = _mm256_setzero_ps();
        let mut sum256_2: __m256 = _mm256_setzero_ps();
        let mut sum256_3: __m256 = _mm256_setzero_ps();
        let mut sum256_4: __m256 = _mm256_setzero_ps();
        let mut i: usize = 0;
        while i < m {
            let sub256_1: __m256 =
                _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(0)), _mm256_loadu_ps(ptr2.add(0)));
            sum256_1 = _mm256_fmadd_ps(sub256_1, sub256_1, sum256_1);

            let sub256_2: __m256 =
                _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(8)), _mm256_loadu_ps(ptr2.add(8)));
            sum256_2 = _mm256_fmadd_ps(sub256_2, sub256_2, sum256_2);

            let sub256_3: __m256 =
                _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(16)), _mm256_loadu_ps(ptr2.add(16)));
            sum256_3 = _mm256_fmadd_ps(sub256_3, sub256_3, sum256_3);

            let sub256_4: __m256 =
                _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(24)), _mm256_loadu_ps(ptr2.add(24)));
            sum256_4 = _mm256_fmadd_ps(sub256_4, sub256_4, sum256_4);

            ptr1 = ptr1.add(32);
            ptr2 = ptr2.add(32);
            i += 32;
        }

        let mut result = four_way_hsum(sum256_1, sum256_2, sum256_3, sum256_4);

        for i in 0..n - m {
            result += (*ptr1.add(i) - *ptr2.add(i)).powi(2);
        }
        -result
    }
}

#[target_feature(enable = "avx")]
#[target_feature(enable = "fma")]
pub(crate) unsafe fn manhattan_similarity_avx(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    unsafe {
        let mask: __m256 = _mm256_set1_ps(-0.0f32); // 1 << 31 used to clear sign bit to mimic abs

        let n = v1.len();
        let m = n - (n % 32);
        let mut ptr1: *const f32 = v1.as_ptr();
        let mut ptr2: *const f32 = v2.as_ptr();
        let mut sum256_1: __m256 = _mm256_setzero_ps();
        let mut sum256_2: __m256 = _mm256_setzero_ps();
        let mut sum256_3: __m256 = _mm256_setzero_ps();
        let mut sum256_4: __m256 = _mm256_setzero_ps();
        let mut i: usize = 0;
        while i < m {
            let sub256_1: __m256 = _mm256_sub_ps(_mm256_loadu_ps(ptr1), _mm256_loadu_ps(ptr2));
            sum256_1 = _mm256_add_ps(_mm256_andnot_ps(mask, sub256_1), sum256_1);

            let sub256_2: __m256 =
                _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(8)), _mm256_loadu_ps(ptr2.add(8)));
            sum256_2 = _mm256_add_ps(_mm256_andnot_ps(mask, sub256_2), sum256_2);

            let sub256_3: __m256 =
                _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(16)), _mm256_loadu_ps(ptr2.add(16)));
            sum256_3 = _mm256_add_ps(_mm256_andnot_ps(mask, sub256_3), sum256_3);

            let sub256_4: __m256 =
                _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(24)), _mm256_loadu_ps(ptr2.add(24)));
            sum256_4 = _mm256_add_ps(_mm256_andnot_ps(mask, sub256_4), sum256_4);

            ptr1 = ptr1.add(32);
            ptr2 = ptr2.add(32);
            i += 32;
        }

        let mut result = four_way_hsum(sum256_1, sum256_2, sum256_3, sum256_4);

        for i in 0..n - m {
            result += (*ptr1.add(i) - *ptr2.add(i)).abs();
        }
        -result
    }
}

#[target_feature(enable = "avx")]
#[target_feature(enable = "fma")]
pub(crate) unsafe fn cosine_preprocess_avx(vector: DenseVector) -> DenseVector {
    unsafe {
        let n = vector.len();
        let m = n - (n % 32);
        let mut ptr: *const f32 = vector.as_ptr();
        let mut sum256_1: __m256 = _mm256_setzero_ps();
        let mut sum256_2: __m256 = _mm256_setzero_ps();
        let mut sum256_3: __m256 = _mm256_setzero_ps();
        let mut sum256_4: __m256 = _mm256_setzero_ps();
        let mut i: usize = 0;
        while i < m {
            let m256_1 = _mm256_loadu_ps(ptr);
            sum256_1 = _mm256_fmadd_ps(m256_1, m256_1, sum256_1);

            let m256_2 = _mm256_loadu_ps(ptr.add(8));
            sum256_2 = _mm256_fmadd_ps(m256_2, m256_2, sum256_2);

            let m256_3 = _mm256_loadu_ps(ptr.add(16));
            sum256_3 = _mm256_fmadd_ps(m256_3, m256_3, sum256_3);

            let m256_4 = _mm256_loadu_ps(ptr.add(24));
            sum256_4 = _mm256_fmadd_ps(m256_4, m256_4, sum256_4);

            ptr = ptr.add(32);
            i += 32;
        }

        let mut length = four_way_hsum(sum256_1, sum256_2, sum256_3, sum256_4);

        for i in 0..n - m {
            length += (*ptr.add(i)).powi(2);
        }
        if is_length_zero_or_normalized(length) {
            return vector;
        }
        length = length.sqrt();
        vector.into_iter().map(|x| x / length).collect()
    }
}

#[target_feature(enable = "avx")]
#[target_feature(enable = "fma")]
pub(crate) unsafe fn dot_similarity_avx(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    unsafe {
        let n = v1.len();
        let m = n - (n % 32);
        let mut ptr1: *const f32 = v1.as_ptr();
        let mut ptr2: *const f32 = v2.as_ptr();
        let mut sum256_1: __m256 = _mm256_setzero_ps();
        let mut sum256_2: __m256 = _mm256_setzero_ps();
        let mut sum256_3: __m256 = _mm256_setzero_ps();
        let mut sum256_4: __m256 = _mm256_setzero_ps();
        let mut i: usize = 0;
        while i < m {
            sum256_1 = _mm256_fmadd_ps(_mm256_loadu_ps(ptr1), _mm256_loadu_ps(ptr2), sum256_1);
            sum256_2 = _mm256_fmadd_ps(
                _mm256_loadu_ps(ptr1.add(8)),
                _mm256_loadu_ps(ptr2.add(8)),
                sum256_2,
            );
            sum256_3 = _mm256_fmadd_ps(
                _mm256_loadu_ps(ptr1.add(16)),
                _mm256_loadu_ps(ptr2.add(16)),
                sum256_3,
            );
            sum256_4 = _mm256_fmadd_ps(
                _mm256_loadu_ps(ptr1.add(24)),
                _mm256_loadu_ps(ptr2.add(24)),
                sum256_4,
            );

            ptr1 = ptr1.add(32);
            ptr2 = ptr2.add(32);
            i += 32;
        }

        let mut result = four_way_hsum(sum256_1, sum256_2, sum256_3, sum256_4);

        for i in 0..n - m {
            result += (*ptr1.add(i)) * (*ptr2.add(i));
        }
        result
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_spaces_avx() {
        use super::*;
        use crate::spaces::simple::*;

        if is_x86_feature_detected!("avx") && is_x86_feature_detected!("fma") {
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

            let euclid_simd = unsafe { euclid_similarity_avx(&v1, &v2) };
            let euclid = euclid_similarity(&v1, &v2);
            assert_eq!(euclid_simd, euclid);

            let manhattan_simd = unsafe { manhattan_similarity_avx(&v1, &v2) };
            let manhattan = manhattan_similarity(&v1, &v2);
            assert_eq!(manhattan_simd, manhattan);

            let dot_simd = unsafe { dot_similarity_avx(&v1, &v2) };
            let dot = dot_similarity(&v1, &v2);
            assert_eq!(dot_simd, dot);

            let cosine_simd = unsafe { cosine_preprocess_avx(v1.clone()) };
            let cosine = cosine_preprocess(v1);
            assert_eq!(cosine_simd, cosine);
        } else {
            println!("avx test skipped");
        }
    }
}
