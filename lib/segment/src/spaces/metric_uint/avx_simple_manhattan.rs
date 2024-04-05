use std::arch::x86_64::*;

use common::types::ScoreType;

use crate::data_types::vectors::VectorElementTypeByte;
use crate::spaces::simple_avx::hsum256_ps_avx;

#[target_feature(enable = "avx")]
#[target_feature(enable = "fma")]
#[allow(unused)]
pub unsafe fn avx_dot_similarity_bytes(
    v1: &[VectorElementTypeByte],
    v2: &[VectorElementTypeByte],
) -> ScoreType {
    debug_assert!(v1.len() == v2.len());
    let mut ptr1: *const VectorElementTypeByte = v1.as_ptr();
    let mut ptr2: *const VectorElementTypeByte = v2.as_ptr();

    // sum accumulator for 8x32 bit integers
    let mut acc = _mm256_setzero_si256();
    // mask to take only lower 8 bits from 16 bits
    let mask_epu16_epu8 = _mm256_set1_epi16(0xFF);
    // mask to take only lower 16 bits from 32 bits
    let mask_epu32_epu16 = _mm256_set1_epi32(0xFFFF);
    let len = v1.len();
    for _ in 0..len / 32 {
        // load 32 bytes
        let p1 = _mm256_loadu_si256(ptr1 as *const __m256i);
        let p2 = _mm256_loadu_si256(ptr2 as *const __m256i);
        ptr1 = ptr1.add(32);
        ptr2 = ptr2.add(32);

        let sad = _mm256_sad_epu8(p1, p2);
        acc = _mm256_add_epi32(acc, sad);
    }

    let mul_ps = _mm256_cvtepi32_ps(acc);
    let score = hsum256_ps_avx(mul_ps);

    let mut remainder = 0;
    for _ in 0..len % 32 {
        let v1 = *ptr1 as i32;
        let v2 = *ptr2 as i32;
        ptr1 = ptr1.add(1);
        ptr2 = ptr2.add(1);
        remainder += (v1 - v2).abs();
    }

    if remainder != 0 {
        -score - remainder as ScoreType
    } else {
        -score
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spaces::metric::Metric;
    use crate::spaces::simple::*;

    #[test]
    fn test_spaces_avx() {
        if is_x86_feature_detected!("avx") && is_x86_feature_detected!("fma") {
            let v1: Vec<VectorElementTypeByte> = vec![
                255, 255, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 255, 255,
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 255, 255, 0, 1, 2, 3,
                4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 255, 255, 0, 1, 2, 3, 4, 5, 6, 7,
                8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 255, 255, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17,
            ];
            let v2: Vec<VectorElementTypeByte> = vec![
                255, 255, 0, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245, 244, 243, 242, 241,
                240, 239, 238, 255, 255, 255, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245,
                244, 243, 242, 241, 240, 239, 238, 255, 255, 255, 254, 253, 252, 251, 250, 249,
                248, 247, 246, 245, 244, 243, 242, 241, 240, 239, 238, 255, 255, 255, 254, 253,
                252, 251, 250, 249, 248, 247, 246, 245, 244, 243, 242, 241, 240, 239, 238, 255,
                255, 255, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245, 244, 243, 242, 241,
                240, 239, 238,
            ];

            let dot_simd = unsafe { avx_dot_similarity_bytes(&v1, &v2) };
            let dot = <ManhattanMetric as Metric<VectorElementTypeByte>>::similarity(&v1, &v2);
            assert_eq!(dot_simd, dot);
        } else {
            println!("avx test skipped");
        }
    }
}
