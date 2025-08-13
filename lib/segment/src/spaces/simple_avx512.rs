use std::arch::x86_64::{
    __m512, _mm256_add_ps, _mm512_add_ps, _mm512_castps512_ps256, _mm512_extractf32x8_ps,
    _mm512_fmadd_ps, _mm512_loadu_ps, _mm512_setzero_ps,
};

use common::types::ScoreType;

use crate::data_types::vectors::VectorElementType;
use crate::spaces::simple_avx::hsum256_ps_avx;
use crate::spaces::tools::is_length_zero_or_normalized;

#[target_feature(enable = "avx")]
#[target_feature(enable = "avx512f")]
#[target_feature(enable = "avx512dq")]
#[target_feature(enable = "sse")]
#[target_feature(enable = "sse3")]
#[target_feature(enable = "sse4.1")]
pub unsafe fn dot_similarity_avx512(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    const STEP_SIZE: usize = 64;

    unsafe {
        let n = v1.len();
        let m = n - (n % STEP_SIZE);

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

            ptr1 = ptr1.add(STEP_SIZE);
            ptr2 = ptr2.add(STEP_SIZE);
            i += STEP_SIZE;
        }

        let mut result = four_way_hsum_512(sum512_1, sum512_2, sum512_3, sum512_4);

        for i in 0..n - m {
            result += (*ptr1.add(i)) * (*ptr2.add(i));
        }

        result
    }
}

#[target_feature(enable = "avx")]
#[target_feature(enable = "avx512f")]
#[target_feature(enable = "avx512dq")]
pub unsafe fn hsum512_ps_avx512(x: __m512) -> f32 {
    unsafe {
        let low_half = _mm512_castps512_ps256(x);
        let high_half = _mm512_extractf32x8_ps::<1>(x);
        let sum = _mm256_add_ps(low_half, high_half);
        hsum256_ps_avx(sum)
    }
}

/// Calculates the hsum (horizontal sum) of eight 32 byte registers.
#[target_feature(enable = "avx")]
#[target_feature(enable = "fma")]
#[target_feature(enable = "avx512f")]
#[target_feature(enable = "avx512dq")]
#[target_feature(enable = "sse")]
#[target_feature(enable = "sse3")]
#[target_feature(enable = "sse4.1")]
#[allow(clippy::missing_safety_doc)]
pub unsafe fn four_way_hsum_512(a: __m512, b: __m512, c: __m512, d: __m512) -> f32 {
    unsafe {
        let sum1 = _mm512_add_ps(a, b);
        let sum2 = _mm512_add_ps(c, d);
        let total = _mm512_add_ps(sum1, sum2);
        hsum512_ps_avx512(total)
    }
}

#[target_feature(enable = "avx")]
#[target_feature(enable = "avx512f")]
#[target_feature(enable = "avx512dq")]
#[target_feature(enable = "sse")]
#[target_feature(enable = "sse3")]
#[target_feature(enable = "sse4.1")]
pub unsafe fn cosine_preprocess_avx512(vector: Vec<f32>) -> Vec<f32> {
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
            let m256_1 = _mm512_loadu_ps(ptr);
            sum512_1 = _mm512_fmadd_ps(m256_1, m256_1, sum512_1);

            let m256_2 = _mm512_loadu_ps(ptr.add(16));
            sum512_2 = _mm512_fmadd_ps(m256_2, m256_2, sum512_2);

            let m256_3 = _mm512_loadu_ps(ptr.add(32));
            sum512_3 = _mm512_fmadd_ps(m256_3, m256_3, sum512_3);

            let m256_4 = _mm512_loadu_ps(ptr.add(48));
            sum512_4 = _mm512_fmadd_ps(m256_4, m256_4, sum512_4);

            ptr = ptr.add(64);
            i += 64;
        }

        let mut length = four_way_hsum_512(sum512_1, sum512_2, sum512_3, sum512_4);

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
