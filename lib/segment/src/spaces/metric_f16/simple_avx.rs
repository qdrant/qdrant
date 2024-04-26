use std::arch::x86_64::*;

use common::types::ScoreType;
use half::f16;
use num_traits::Float;

use crate::data_types::vectors::VectorElementTypeHalf;

#[target_feature(enable = "avx")]
#[target_feature(enable = "fma")]
#[allow(clippy::missing_safety_doc)]
pub unsafe fn hsum256_ps_avx(x: __m256) -> f32 {
    let x128: __m128 = _mm_add_ps(_mm256_extractf128_ps(x, 1), _mm256_castps256_ps128(x));
    let x64: __m128 = _mm_add_ps(x128, _mm_movehl_ps(x128, x128));
    let x32: __m128 = _mm_add_ss(x64, _mm_shuffle_ps(x64, x64, 0x55));
    _mm_cvtss_f32(x32)
}

#[target_feature(enable = "avx")]
#[target_feature(enable = "fma")]
#[target_feature(enable = "f16c")]
pub(crate) unsafe fn euclid_similarity_avx(
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

#[target_feature(enable = "avx")]
#[target_feature(enable = "fma")]
#[target_feature(enable = "f16c")]
pub(crate) unsafe fn manhattan_similarity_avx(
    v1: &[VectorElementTypeHalf],
    v2: &[VectorElementTypeHalf],
) -> ScoreType {
    let mask: __m256 = _mm256_set1_ps(-0.0f32); // 1 << 31 used to clear sign bit to mimic abs

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
        sum256_1 = _mm256_add_ps(_mm256_andnot_ps(mask, sub256_1), sum256_1);

        addr1s = _mm_loadu_si128(ptr1.wrapping_add(1));
        addr2s = _mm_loadu_si128(ptr2.wrapping_add(1));

        let sub256_2: __m256 = _mm256_sub_ps(_mm256_cvtph_ps(addr1s), _mm256_cvtph_ps(addr2s));
        sum256_2 = _mm256_add_ps(_mm256_andnot_ps(mask, sub256_2), sum256_2);

        addr1s = _mm_loadu_si128(ptr1.wrapping_add(2));
        addr2s = _mm_loadu_si128(ptr2.wrapping_add(2));

        let sub256_3: __m256 = _mm256_sub_ps(_mm256_cvtph_ps(addr1s), _mm256_cvtph_ps(addr2s));
        sum256_3 = _mm256_add_ps(_mm256_andnot_ps(mask, sub256_3), sum256_3);

        addr1s = _mm_loadu_si128(ptr1.wrapping_add(3));
        addr2s = _mm_loadu_si128(ptr2.wrapping_add(3));

        let sub256_4: __m256 = _mm256_sub_ps(_mm256_cvtph_ps(addr1s), _mm256_cvtph_ps(addr2s));
        sum256_4 = _mm256_add_ps(_mm256_andnot_ps(mask, sub256_4), sum256_4);

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
        result += (f16::to_f32(*ptr1_f16.add(i)) - f16::to_f32(*ptr2_f16.add(i))).abs();
    }
    -result
}

#[target_feature(enable = "avx")]
#[target_feature(enable = "fma")]
#[target_feature(enable = "f16c")]
pub(crate) unsafe fn dot_similarity_avx(
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

        sum256_1 = _mm256_fmadd_ps(_mm256_cvtph_ps(addr1s), _mm256_cvtph_ps(addr2s), sum256_1);

        addr1s = _mm_loadu_si128(ptr1.wrapping_add(1));
        addr2s = _mm_loadu_si128(ptr2.wrapping_add(1));

        sum256_2 = _mm256_fmadd_ps(_mm256_cvtph_ps(addr1s), _mm256_cvtph_ps(addr2s), sum256_2);

        addr1s = _mm_loadu_si128(ptr1.wrapping_add(2));
        addr2s = _mm_loadu_si128(ptr2.wrapping_add(2));

        sum256_3 = _mm256_fmadd_ps(_mm256_cvtph_ps(addr1s), _mm256_cvtph_ps(addr2s), sum256_3);

        addr1s = _mm_loadu_si128(ptr1.wrapping_add(3));
        addr2s = _mm_loadu_si128(ptr2.wrapping_add(3));

        sum256_4 = _mm256_fmadd_ps(_mm256_cvtph_ps(addr1s), _mm256_cvtph_ps(addr2s), sum256_4);

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
        result += f16::to_f32(*ptr1_f16.add(i)) * f16::to_f32(*ptr2_f16.add(i));
    }
    result
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_spaces_avx() {
        use super::*;
        use crate::spaces::metric_f16::simple::*;

        if is_x86_feature_detected!("avx") && is_x86_feature_detected!("fma") {
            let v1: Vec<f16> = vec![
                f16::from_f32(1.),
                f16::from_f32(2.),
                f16::from_f32(3.),
                f16::from_f32(4.),
                f16::from_f32(5.),
                f16::from_f32(6.),
                f16::from_f32(7.),
                f16::from_f32(8.),
                f16::from_f32(9.),
                f16::from_f32(10.),
                f16::from_f32(11.),
                f16::from_f32(12.),
                f16::from_f32(13.),
                f16::from_f32(14.),
                f16::from_f32(15.),
                f16::from_f32(16.),
                f16::from_f32(17.),
                f16::from_f32(18.),
                f16::from_f32(19.),
                f16::from_f32(20.),
                f16::from_f32(21.),
                f16::from_f32(22.),
                f16::from_f32(1.),
                f16::from_f32(2.),
                f16::from_f32(3.),
                f16::from_f32(4.),
                f16::from_f32(5.),
                f16::from_f32(6.),
                f16::from_f32(7.),
                f16::from_f32(8.),
                f16::from_f32(9.),
                f16::from_f32(10.),
                f16::from_f32(11.),
                f16::from_f32(12.),
                f16::from_f32(13.),
                f16::from_f32(14.),
                f16::from_f32(15.),
                f16::from_f32(16.),
                f16::from_f32(17.),
                f16::from_f32(18.),
                f16::from_f32(19.),
                f16::from_f32(20.),
                f16::from_f32(21.),
                f16::from_f32(22.),
            ];
            let v2: Vec<f16> = vec![
                f16::from_f32(2.),
                f16::from_f32(3.),
                f16::from_f32(4.),
                f16::from_f32(5.),
                f16::from_f32(6.),
                f16::from_f32(7.),
                f16::from_f32(8.),
                f16::from_f32(9.),
                f16::from_f32(10.),
                f16::from_f32(11.),
                f16::from_f32(12.),
                f16::from_f32(13.),
                f16::from_f32(14.),
                f16::from_f32(15.),
                f16::from_f32(16.),
                f16::from_f32(17.),
                f16::from_f32(18.),
                f16::from_f32(19.),
                f16::from_f32(20.),
                f16::from_f32(21.),
                f16::from_f32(22.),
                f16::from_f32(23.),
                f16::from_f32(2.),
                f16::from_f32(3.),
                f16::from_f32(4.),
                f16::from_f32(5.),
                f16::from_f32(6.),
                f16::from_f32(7.),
                f16::from_f32(8.),
                f16::from_f32(9.),
                f16::from_f32(10.),
                f16::from_f32(11.),
                f16::from_f32(12.),
                f16::from_f32(13.),
                f16::from_f32(14.),
                f16::from_f32(15.),
                f16::from_f32(16.),
                f16::from_f32(17.),
                f16::from_f32(18.),
                f16::from_f32(19.),
                f16::from_f32(20.),
                f16::from_f32(21.),
                f16::from_f32(22.),
                f16::from_f32(23.),
            ];

            let euclid_simd = unsafe { euclid_similarity_avx(&v1, &v2) };
            let euclid = euclid_similarity_half(&v1, &v2);
            assert_eq!(euclid_simd, euclid);

            let manhattan_simd = unsafe { manhattan_similarity_avx(&v1, &v2) };
            let manhattan = manhattan_similarity_half(&v1, &v2);
            assert_eq!(manhattan_simd, manhattan);

            let dot_simd = unsafe { dot_similarity_avx(&v1, &v2) };
            let dot = dot_similarity_half(&v1, &v2);
            assert_eq!(dot_simd, dot);
        } else {
            println!("avx test skipped");
        }
    }
}
