#[cfg(target_arch = "x86")]
use std::arch::x86::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

use common::types::ScoreType;
use half::f16;
use num_traits::Float;

use crate::data_types::vectors::VectorElementTypeHalf;

#[target_feature(enable = "sse")]
#[allow(clippy::missing_safety_doc)]
pub unsafe fn hsum128_ps_sse(x: __m128) -> f32 {
    let x64: __m128 = _mm_add_ps(x, _mm_movehl_ps(x, x));
    let x32: __m128 = _mm_add_ss(x64, _mm_shuffle_ps(x64, x64, 0x55));
    _mm_cvtss_f32(x32)
}

#[target_feature(enable = "sse")]
#[target_feature(enable = "f16c")]
pub(crate) unsafe fn euclid_similarity_sse(
    v1: &[VectorElementTypeHalf],
    v2: &[VectorElementTypeHalf],
) -> ScoreType {
    let n = v1.len();
    let m = n - (n % 8);
    let mut ptr1: *const __m128i = v1.as_ptr() as *const __m128i;
    let mut ptr2: *const __m128i = v2.as_ptr() as *const __m128i;
    let mut sum128_1: __m128 = _mm_setzero_ps();

    let mut addr1s: __m128i;
    let mut addr2s: __m128i;

    let mut i: usize = 0;
    while i < m {
        addr1s = _mm_loadu_si128(ptr1);
        addr2s = _mm_loadu_si128(ptr2);
        let mut sub128_1 = _mm_sub_ps(_mm_cvtph_ps(addr1s), _mm_cvtph_ps(addr2s));
        sum128_1 = _mm_add_ps(_mm_mul_ps(sub128_1, sub128_1), sum128_1);

        sub128_1 = _mm_sub_ps(
            _mm_cvtph_ps(_mm_srli_si128(addr1s, 8)),
            _mm_cvtph_ps(_mm_srli_si128(addr2s, 8)),
        );
        sum128_1 = _mm_add_ps(_mm_mul_ps(sub128_1, sub128_1), sum128_1);

        ptr1 = ptr1.wrapping_add(1);
        ptr2 = ptr2.wrapping_add(1);
        i += 8;
    }

    let ptr1_f16: *const f16 = ptr1 as *const f16;
    let ptr2_f16: *const f16 = ptr2 as *const f16;

    let mut result = hsum128_ps_sse(sum128_1);
    for i in 0..n - m {
        result += (f16::to_f32(*ptr1_f16.wrapping_add(i)) - f16::to_f32(*ptr2_f16.wrapping_add(i)))
            .powi(2);
    }
    -result
}

#[target_feature(enable = "sse")]
#[target_feature(enable = "f16c")]
pub(crate) unsafe fn manhattan_similarity_sse(
    v1: &[VectorElementTypeHalf],
    v2: &[VectorElementTypeHalf],
) -> ScoreType {
    let mask: __m128 = _mm_set1_ps(-0.0f32); // 1 << 31 used to clear sign bit to mimic abs

    let n = v1.len();
    let m = n - (n % 8);
    let mut ptr1: *const __m128i = v1.as_ptr() as *const __m128i;
    let mut ptr2: *const __m128i = v2.as_ptr() as *const __m128i;
    let mut sum128_1: __m128 = _mm_setzero_ps();

    let mut addr1s: __m128i;
    let mut addr2s: __m128i;

    let mut i: usize = 0;
    while i < m {
        addr1s = _mm_loadu_si128(ptr1);
        addr2s = _mm_loadu_si128(ptr2);

        let mut sub128_1 = _mm_sub_ps(_mm_cvtph_ps(addr1s), _mm_cvtph_ps(addr2s));
        sum128_1 = _mm_add_ps(_mm_andnot_ps(mask, sub128_1), sum128_1);

        sub128_1 = _mm_sub_ps(
            _mm_cvtph_ps(_mm_srli_si128(addr1s, 8)),
            _mm_cvtph_ps(_mm_srli_si128(addr2s, 8)),
        );
        sum128_1 = _mm_add_ps(_mm_andnot_ps(mask, sub128_1), sum128_1);

        ptr1 = ptr1.wrapping_add(1);
        ptr2 = ptr2.wrapping_add(1);
        i += 8;
    }

    let ptr1_f16: *const f16 = ptr1 as *const f16;
    let ptr2_f16: *const f16 = ptr2 as *const f16;

    let mut result = hsum128_ps_sse(sum128_1);
    for i in 0..n - m {
        result += (f16::to_f32(*ptr1_f16.add(i)) - f16::to_f32(*ptr2_f16.add(i))).abs();
    }
    -result
}

#[target_feature(enable = "sse")]
#[target_feature(enable = "f16c")]
pub(crate) unsafe fn dot_similarity_sse(
    v1: &[VectorElementTypeHalf],
    v2: &[VectorElementTypeHalf],
) -> ScoreType {
    let n = v1.len();
    let m = n - (n % 16);
    let mut ptr1: *const __m128i = v1.as_ptr() as *const __m128i;
    let mut ptr2: *const __m128i = v2.as_ptr() as *const __m128i;
    let mut sum128_1: __m128 = _mm_setzero_ps();

    let mut addr1s: __m128i;
    let mut addr2s: __m128i;

    let mut i: usize = 0;
    while i < m {
        addr1s = _mm_loadu_si128(ptr1);
        addr2s = _mm_loadu_si128(ptr2);
        sum128_1 = _mm_add_ps(
            _mm_mul_ps(_mm_cvtph_ps(addr1s), _mm_cvtph_ps(addr2s)),
            sum128_1,
        );

        sum128_1 = _mm_add_ps(
            _mm_mul_ps(
                _mm_cvtph_ps(_mm_srli_si128(addr1s, 8)),
                _mm_cvtph_ps(_mm_srli_si128(addr2s, 8)),
            ),
            sum128_1,
        );

        ptr1 = ptr1.wrapping_add(1);
        ptr2 = ptr2.wrapping_add(1);
        i += 8;
    }

    let ptr1_f16: *const f16 = ptr1 as *const f16;
    let ptr2_f16: *const f16 = ptr2 as *const f16;

    let mut result = hsum128_ps_sse(sum128_1);
    for i in 0..n - m {
        result += f16::to_f32(*ptr1_f16.add(i)) * f16::to_f32(*ptr2_f16.add(i));
    }
    result
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_spaces_sse() {
        use super::*;
        use crate::spaces::metric_f16::simple::*;

        if is_x86_feature_detected!("sse") {
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

            let euclid_simd = unsafe { euclid_similarity_sse(&v1, &v2) };
            let euclid = euclid_similarity_half(&v1, &v2);
            assert_eq!(euclid_simd, euclid);

            let manhattan_simd = unsafe { manhattan_similarity_sse(&v1, &v2) };
            let manhattan = manhattan_similarity_half(&v1, &v2);
            assert_eq!(manhattan_simd, manhattan);

            let dot_simd = unsafe { dot_similarity_sse(&v1, &v2) };
            let dot = dot_similarity_half(&v1, &v2);
            assert_eq!(dot_simd, dot);
        } else {
            println!("sse test skipped");
        }
    }
}
