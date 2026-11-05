use std::arch::x86_64::*;

use common::types::ScoreType;

use super::tools::is_length_zero_or_normalized;
use crate::data_types::vectors::{DenseVector, VectorElementType};

/// Horizontal sum of four `__m512` accumulators.
#[target_feature(enable = "avx512f")]
#[allow(clippy::missing_safety_doc)]
pub unsafe fn four_way_hsum512(a: __m512, b: __m512, c: __m512, d: __m512) -> f32 {
    unsafe {
        let sum1 = _mm512_add_ps(a, b);
        let sum2 = _mm512_add_ps(c, d);
        _mm512_reduce_add_ps(_mm512_add_ps(sum1, sum2))
    }
}

/// Euclidean similarity using AVX-512F (+ FMA via `fmadd`).
///
/// Processes 64 floats per iteration with four `__m512` accumulators,
/// mirroring the AVX unroll width (4×256-bit → 4×512-bit).
#[target_feature(enable = "avx512f")]
pub(crate) unsafe fn euclid_similarity_avx512(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    unsafe {
        let n = v1.len();
        let m = n - (n % 64);
        let mut ptr1: *const f32 = v1.as_ptr();
        let mut ptr2: *const f32 = v2.as_ptr();
        let mut sum512_1: __m512 = _mm512_setzero_ps();
        let mut sum512_2: __m512 = _mm512_setzero_ps();
        let mut sum512_3: __m512 = _mm512_setzero_ps();
        let mut sum512_4: __m512 = _mm512_setzero_ps();
        let mut i: usize = 0;
        while i < m {
            let sub512_1 =
                _mm512_sub_ps(_mm512_loadu_ps(ptr1.add(0)), _mm512_loadu_ps(ptr2.add(0)));
            sum512_1 = _mm512_fmadd_ps(sub512_1, sub512_1, sum512_1);

            let sub512_2 =
                _mm512_sub_ps(_mm512_loadu_ps(ptr1.add(16)), _mm512_loadu_ps(ptr2.add(16)));
            sum512_2 = _mm512_fmadd_ps(sub512_2, sub512_2, sum512_2);

            let sub512_3 =
                _mm512_sub_ps(_mm512_loadu_ps(ptr1.add(32)), _mm512_loadu_ps(ptr2.add(32)));
            sum512_3 = _mm512_fmadd_ps(sub512_3, sub512_3, sum512_3);

            let sub512_4 =
                _mm512_sub_ps(_mm512_loadu_ps(ptr1.add(48)), _mm512_loadu_ps(ptr2.add(48)));
            sum512_4 = _mm512_fmadd_ps(sub512_4, sub512_4, sum512_4);

            ptr1 = ptr1.add(64);
            ptr2 = ptr2.add(64);
            i += 64;
        }

        let mut result = four_way_hsum512(sum512_1, sum512_2, sum512_3, sum512_4);

        for i in 0..n - m {
            result += (*ptr1.add(i) - *ptr2.add(i)).powi(2);
        }
        -result
    }
}

/// Manhattan similarity using AVX-512F.
#[target_feature(enable = "avx512f")]
pub(crate) unsafe fn manhattan_similarity_avx512(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    unsafe {
        let n = v1.len();
        let m = n - (n % 64);
        let mut ptr1: *const f32 = v1.as_ptr();
        let mut ptr2: *const f32 = v2.as_ptr();
        let mut sum512_1: __m512 = _mm512_setzero_ps();
        let mut sum512_2: __m512 = _mm512_setzero_ps();
        let mut sum512_3: __m512 = _mm512_setzero_ps();
        let mut sum512_4: __m512 = _mm512_setzero_ps();
        let mut i: usize = 0;
        while i < m {
            let sub512_1 = _mm512_sub_ps(_mm512_loadu_ps(ptr1), _mm512_loadu_ps(ptr2));
            sum512_1 = _mm512_add_ps(_mm512_abs_ps(sub512_1), sum512_1);

            let sub512_2 =
                _mm512_sub_ps(_mm512_loadu_ps(ptr1.add(16)), _mm512_loadu_ps(ptr2.add(16)));
            sum512_2 = _mm512_add_ps(_mm512_abs_ps(sub512_2), sum512_2);

            let sub512_3 =
                _mm512_sub_ps(_mm512_loadu_ps(ptr1.add(32)), _mm512_loadu_ps(ptr2.add(32)));
            sum512_3 = _mm512_add_ps(_mm512_abs_ps(sub512_3), sum512_3);

            let sub512_4 =
                _mm512_sub_ps(_mm512_loadu_ps(ptr1.add(48)), _mm512_loadu_ps(ptr2.add(48)));
            sum512_4 = _mm512_add_ps(_mm512_abs_ps(sub512_4), sum512_4);

            ptr1 = ptr1.add(64);
            ptr2 = ptr2.add(64);
            i += 64;
        }

        let mut result = four_way_hsum512(sum512_1, sum512_2, sum512_3, sum512_4);

        for i in 0..n - m {
            result += (*ptr1.add(i) - *ptr2.add(i)).abs();
        }
        -result
    }
}

/// Cosine preprocess (L2 normalize) using AVX-512F.
#[target_feature(enable = "avx512f")]
pub(crate) unsafe fn cosine_preprocess_avx512(mut vector: DenseVector) -> DenseVector {
    unsafe {
        let n = vector.len();
        let m = n - (n % 64);
        let mut ptr: *const f32 = vector.as_ptr();
        let mut sum512_1: __m512 = _mm512_setzero_ps();
        let mut sum512_2: __m512 = _mm512_setzero_ps();
        let mut sum512_3: __m512 = _mm512_setzero_ps();
        let mut sum512_4: __m512 = _mm512_setzero_ps();
        let mut i: usize = 0;
        while i < m {
            let m512_1 = _mm512_loadu_ps(ptr);
            sum512_1 = _mm512_fmadd_ps(m512_1, m512_1, sum512_1);

            let m512_2 = _mm512_loadu_ps(ptr.add(16));
            sum512_2 = _mm512_fmadd_ps(m512_2, m512_2, sum512_2);

            let m512_3 = _mm512_loadu_ps(ptr.add(32));
            sum512_3 = _mm512_fmadd_ps(m512_3, m512_3, sum512_3);

            let m512_4 = _mm512_loadu_ps(ptr.add(48));
            sum512_4 = _mm512_fmadd_ps(m512_4, m512_4, sum512_4);

            ptr = ptr.add(64);
            i += 64;
        }

        let mut length = four_way_hsum512(sum512_1, sum512_2, sum512_3, sum512_4);

        for i in 0..n - m {
            length += (*ptr.add(i)).powi(2);
        }
        if is_length_zero_or_normalized(length) {
            return vector;
        }

        let inv_length = 1.0 / length.sqrt();
        let v_inv_length = _mm512_set1_ps(inv_length);
        let mut_ptr: *mut f32 = vector.as_mut_ptr();

        let mut i: usize = 0;

        while i + 31 < n {
            let v1 = _mm512_loadu_ps(mut_ptr.add(i));
            let v2 = _mm512_loadu_ps(mut_ptr.add(i + 16));
            _mm512_storeu_ps(mut_ptr.add(i), _mm512_mul_ps(v1, v_inv_length));
            _mm512_storeu_ps(mut_ptr.add(i + 16), _mm512_mul_ps(v2, v_inv_length));
            i += 32;
        }

        while i + 15 < n {
            let v = _mm512_loadu_ps(mut_ptr.add(i));
            _mm512_storeu_ps(mut_ptr.add(i), _mm512_mul_ps(v, v_inv_length));
            i += 16;
        }

        for v in vector.iter_mut().take(n).skip(i) {
            *v *= inv_length;
        }

        vector
    }
}

/// Dot-product similarity using AVX-512F (+ FMA via `fmadd`).
#[target_feature(enable = "avx512f")]
pub(crate) unsafe fn dot_similarity_avx512(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    unsafe {
        let n = v1.len();
        let m = n - (n % 64);
        let mut ptr1: *const f32 = v1.as_ptr();
        let mut ptr2: *const f32 = v2.as_ptr();
        let mut sum512_1: __m512 = _mm512_setzero_ps();
        let mut sum512_2: __m512 = _mm512_setzero_ps();
        let mut sum512_3: __m512 = _mm512_setzero_ps();
        let mut sum512_4: __m512 = _mm512_setzero_ps();
        let mut i: usize = 0;
        while i < m {
            sum512_1 = _mm512_fmadd_ps(_mm512_loadu_ps(ptr1), _mm512_loadu_ps(ptr2), sum512_1);
            sum512_2 = _mm512_fmadd_ps(
                _mm512_loadu_ps(ptr1.add(16)),
                _mm512_loadu_ps(ptr2.add(16)),
                sum512_2,
            );
            sum512_3 = _mm512_fmadd_ps(
                _mm512_loadu_ps(ptr1.add(32)),
                _mm512_loadu_ps(ptr2.add(32)),
                sum512_3,
            );
            sum512_4 = _mm512_fmadd_ps(
                _mm512_loadu_ps(ptr1.add(48)),
                _mm512_loadu_ps(ptr2.add(48)),
                sum512_4,
            );

            ptr1 = ptr1.add(64);
            ptr2 = ptr2.add(64);
            i += 64;
        }

        let mut result = four_way_hsum512(sum512_1, sum512_2, sum512_3, sum512_4);

        for i in 0..n - m {
            result += (*ptr1.add(i)) * (*ptr2.add(i));
        }
        result
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_spaces_avx512() {
        use super::*;
        use crate::spaces::simple::*;

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

            let euclid_simd = unsafe { euclid_similarity_avx512(&v1, &v2) };
            let euclid = euclid_similarity(&v1, &v2);
            assert_eq!(euclid_simd, euclid);

            let manhattan_simd = unsafe { manhattan_similarity_avx512(&v1, &v2) };
            let manhattan = manhattan_similarity(&v1, &v2);
            assert_eq!(manhattan_simd, manhattan);

            let dot_simd = unsafe { dot_similarity_avx512(&v1, &v2) };
            let dot = dot_similarity(&v1, &v2);
            assert_eq!(dot_simd, dot);

            let cosine_simd = unsafe { cosine_preprocess_avx512(v1.clone()) };
            let cosine = cosine_preprocess(v1);
            for (a, b) in cosine_simd.iter().zip(cosine.iter()) {
                let tol = 1e-6_f32.max(8.0 * f32::EPSILON * a.abs().max(b.abs()).max(1.0));
                assert!((a - b).abs() <= tol, "Cosine SIMD mismatch: {a} vs {b}",);
            }
        } else {
            println!("avx512f test skipped");
        }
    }
}
