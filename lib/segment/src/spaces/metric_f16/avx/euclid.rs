use std::arch::x86_64::*;

use common::types::ScoreType;
use half::f16;

use crate::data_types::vectors::VectorElementTypeHalf;
use crate::spaces::simple_avx::hsum256_ps_avx;

#[target_feature(enable = "avx")]
#[target_feature(enable = "fma")]
#[target_feature(enable = "f16c")]
pub unsafe fn avx_euclid_similarity_half(
    v1: &[VectorElementTypeHalf],
    v2: &[VectorElementTypeHalf],
) -> ScoreType {
    let n = v1.len();
    let m = n - (n % 32);
    let mut ptr1: *const __m128i = v1.as_ptr() as *const __m128i;
    let mut ptr2: *const __m128i = v2.as_ptr() as *const __m128i;
    let mut sum256_1: __m256 = _mm256_setzero_ps();
    let mut sum256_2: __m256 = _mm256_setzero_ps();
    let mut sum256_3: __m256 = _mm256_setzero_ps();
    let mut sum256_4: __m256 = _mm256_setzero_ps();

    let mut addr1s: __m128i;
    let mut addr2s: __m128i;

    let mut i: usize = 0;
    while i < m {
        addr1s = _mm_loadu_si128(ptr1);
        addr2s = _mm_loadu_si128(ptr2);
        let sub256_1: __m256 = _mm256_sub_ps(_mm256_cvtph_ps(addr1s), _mm256_cvtph_ps(addr2s));
        sum256_1 = _mm256_fmadd_ps(sub256_1, sub256_1, sum256_1);

        addr1s = _mm_loadu_si128(ptr1.wrapping_add(1));
        addr2s = _mm_loadu_si128(ptr2.wrapping_add(1));

        let sub256_2: __m256 = _mm256_sub_ps(_mm256_cvtph_ps(addr1s), _mm256_cvtph_ps(addr2s));
        sum256_2 = _mm256_fmadd_ps(sub256_2, sub256_2, sum256_2);

        addr1s = _mm_loadu_si128(ptr1.wrapping_add(2));
        addr2s = _mm_loadu_si128(ptr2.wrapping_add(2));

        let sub256_3: __m256 = _mm256_sub_ps(_mm256_cvtph_ps(addr1s), _mm256_cvtph_ps(addr2s));
        sum256_3 = _mm256_fmadd_ps(sub256_3, sub256_3, sum256_3);

        addr1s = _mm_loadu_si128(ptr1.wrapping_add(3));
        addr2s = _mm_loadu_si128(ptr2.wrapping_add(3));

        let sub256_4: __m256 = _mm256_sub_ps(_mm256_cvtph_ps(addr1s), _mm256_cvtph_ps(addr2s));
        sum256_4 = _mm256_fmadd_ps(sub256_4, sub256_4, sum256_4);

        ptr1 = ptr1.wrapping_add(4);
        ptr2 = ptr2.wrapping_add(4);
        i += 32;
    }

    let ptr1_f16: *const f16 = ptr1 as *const f16;
    let ptr2_f16: *const f16 = ptr2 as *const f16;

    let mut result = hsum256_ps_avx(sum256_1)
        + hsum256_ps_avx(sum256_2)
        + hsum256_ps_avx(sum256_3)
        + hsum256_ps_avx(sum256_4);
    for i in 0..n - m {
        result += (f16::to_f32(*ptr1_f16.add(i)) - f16::to_f32(*ptr2_f16.add(i))).powi(2);
    }
    -result
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_spaces_avx() {
        use super::*;
        use crate::spaces::metric_f16::simple_euclid::*;

        if is_x86_feature_detected!("avx")
            && is_x86_feature_detected!("fma")
            && is_x86_feature_detected!("f16c")
        {
            let v1_f32: Vec<f32> = vec![
                3.7, 4.3, 5.6, 7.7, 7.6, 4.2, 4.2, 7.3, 4.1, 6., 6.4, 1., 2.4, 7., 2.4, 6.4, 4.8,
                2.4, 2.9, 3.9, 3.9, 7.4, 6.9, 5.3, 6.2, 5.2, 5.2, 4.2, 5.9, 1.8, 4.5, 3.5, 3.1,
                6.1, 6.5, 2.4, 2.1, 7.5, 2.3, 5.9, 3.6, 2.9, 6.1, 5.9, 3.3, 2.9, 3.7, 6.8, 7.2,
                6.5, 3.1, 5.7, 1.1, 7.2, 5.6, 5.1, 7., 2.5, 6.2, 7.6, 7., 6.9, 7.5, 3.2, 5.4, 5.8,
                1.9, 4.9, 7.7, 6.5, 3., 2., 6.9, 6.8, 3.3, 1.4, 4.7, 3.7, 1.9, 3.6, 3.9, 7.2, 7.7,
                7., 6.9, 5.8, 4.4, 1.8, 4.9, 3.1, 7.9, 6.5, 7.5, 3.7, 4.6, 1.5, 3.4, 1.7, 6.4, 7.3,
                4.7, 1.9, 7.7, 8., 4.3, 3.9, 1.5, 6.1, 2.1, 6.9, 2.5, 7.2, 4.1, 4.8, 1., 4.1, 6.3,
                5.9, 6.2, 3.9, 4.1, 1.2, 7.3, 1., 4., 3.1, 6., 5.8, 6.8, 2.6, 5.1, 2.3, 1.2, 5.6,
                3.3, 1.6, 4.7, 7., 4.7, 7.7, 1.5, 4.1, 4.1, 5.8, 7.5, 7.6, 5.2, 2.8, 6.9, 6.1, 4.3,
                5.9, 5.2, 8., 2.1, 1.3, 3.2, 4.3, 5.5, 7.7, 6.8, 2.6, 5.2, 4.1, 4.9, 3.7, 6.2, 1.6,
                4.9, 2.6, 6.9, 2.3, 3.9, 7.7, 6.6, 5.3, 3.1, 5.5, 3., 2.4, 1.9, 6.7, 7.1, 6.3, 7.4,
                6.8, 2.3, 6.1, 3.6, 1.1, 2.8, 7., 3.5, 4.1, 3.4, 7.4, 1.4, 5.5, 6.3, 6.8, 2., 2.1,
                2.7, 7.8, 6., 3.6, 5.9, 3.9, 3.6, 7.8, 5.4, 6.8, 4.6, 7.8, 2.3, 6.2, 7.6, 5.8, 3.3,
                3.2, 6.2, 1.9, 6., 5.3, 3.2, 5.8, 7., 1.6, 1.3, 7.7, 6.1, 1.2, 2.8, 2., 2.2, 2.2,
                5.4, 4.8, 1.8, 3.6, 1.9, 6., 3.3, 3.1, 4.9, 6.2, 2.9, 6.1, 6.6, 3.9, 3.8, 4.8, 6.1,
                6.9, 6.7, 5.9, 6.3, 3.3, 3.2, 5.9,
            ];
            let v2_f32: Vec<f32> = vec![
                1.5, 1.3, 1.7, 6.4, 4.6, 6.2, 1.7, 2.6, 4.3, 6.1, 7.2, 3.7, 1.3, 7.3, 3.6, 5.6,
                5.9, 5.6, 2.3, 3.7, 7.4, 3.6, 7.5, 7.6, 4.8, 5.6, 2.2, 4.3, 4.4, 4.9, 6.1, 2.9,
                5.6, 1.6, 2.4, 7.6, 6., 6.3, 7.3, 1., 3.1, 7., 3.1, 5.5, 2.6, 6.7, 2.2, 1.8, 6.6,
                7.1, 1.6, 3.7, 7.7, 6.3, 2.8, 3., 6.5, 3.3, 3.6, 2.7, 7., 4.2, 7.7, 5.6, 3., 7.4,
                1.6, 4.2, 3.7, 2.7, 3.4, 7., 2.9, 6.6, 8., 5.7, 4.9, 3.8, 4.9, 7.1, 3.9, 4.8, 5.3,
                4.2, 7.2, 6.3, 2.4, 1.5, 3.9, 5.5, 4.1, 6.2, 1., 2.8, 2.7, 6.8, 1.7, 6.7, 1.7, 7.2,
                2.1, 6.3, 5.1, 7.3, 4.7, 1.1, 4.4, 6.4, 4.9, 5.8, 5., 7.6, 6.5, 4., 4., 5.9, 5.3,
                2.1, 3., 7.9, 6.1, 6.1, 5.3, 5.8, 1.4, 3.2, 3.3, 1.2, 1., 6.2, 4.2, 4.5, 3.5, 5.1,
                7., 6., 3.9, 5.5, 6.6, 6.9, 5., 1., 4.8, 4.2, 5.1, 1.1, 1.3, 1.5, 7.9, 7.7, 5.2,
                5.4, 1.4, 1.4, 4.6, 4., 3.2, 2.2, 4.3, 7.1, 3.9, 4.5, 6.1, 5.3, 3.2, 1.4, 6.7, 1.6,
                2.2, 2.8, 4.7, 6.1, 6.2, 6.1, 1.4, 7., 7.4, 7.3, 4.1, 1.5, 3.3, 7.4, 5.3, 7.9, 4.3,
                2.6, 3.6, 4.1, 5.1, 6.4, 5.8, 2.4, 1.8, 4.8, 6.2, 3.5, 5.9, 6.3, 5.1, 4.9, 7.5,
                7.1, 2.4, 1.9, 6.3, 4.2, 7.9, 7.4, 5.6, 4.7, 7.4, 7.9, 3.2, 4.8, 5.7, 5.9, 7.4,
                2.8, 5.2, 6.4, 5.1, 4., 7.2, 3.6, 2., 3.1, 7.5, 3.7, 2.9, 3.4, 6.1, 1., 1.2, 1.3,
                3.8, 2.7, 7.4, 6.6, 5.3, 4.6, 1.8, 3.7, 1.4, 1.1, 1.9, 5.9, 6.5, 4.1, 4.9, 5.7,
                3.9, 4.1, 7.2, 5., 7.3, 2.8, 7.1, 7.2, 4., 2.7,
            ];

            let v1: Vec<f16> = v1_f32.iter().map(|x| f16::from_f32(*x)).collect();
            let v2: Vec<f16> = v2_f32.iter().map(|x| f16::from_f32(*x)).collect();

            let euclid_simd = unsafe { avx_euclid_similarity_half(&v1, &v2) };
            let euclid = euclid_similarity_half(&v1, &v2);
            assert!((euclid_simd - euclid).abs() / euclid.abs() < 0.0005);
        } else {
            println!("avx test skipped");
        }
    }
}
