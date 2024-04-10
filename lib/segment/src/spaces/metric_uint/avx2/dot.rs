use std::arch::x86_64::*;

use crate::spaces::simple_avx::hsum256_ps_avx;

#[target_feature(enable = "avx")]
#[target_feature(enable = "avx2")]
#[target_feature(enable = "fma")]
#[allow(clippy::missing_safety_doc)]
pub unsafe fn avx_dot_similarity_bytes(v1: &[u8], v2: &[u8]) -> f32 {
    debug_assert!(v1.len() == v2.len());
    debug_assert!(is_x86_feature_detected!("avx"));
    debug_assert!(is_x86_feature_detected!("avx2"));
    debug_assert!(is_x86_feature_detected!("fma"));

    let mut ptr1: *const u8 = v1.as_ptr();
    let mut ptr2: *const u8 = v2.as_ptr();

    // sum accumulator for 8x32 bit integers
    let mut acc = _mm256_setzero_si256();
    // mask to take only lower 8 bits from 16 bits
    let mask_epu16_epu8 = _mm256_set1_epi16(0xFF);
    let len = v1.len();
    for _ in 0..len / 32 {
        // load 32 bytes
        let p1 = _mm256_loadu_si256(ptr1 as *const __m256i);
        let p2 = _mm256_loadu_si256(ptr2 as *const __m256i);
        ptr1 = ptr1.add(32);
        ptr2 = ptr2.add(32);

        // take from lane p1 and p2 parts (using bitwise AND):
        // p1 = [byte0, byte1, byte2, byte3, ..] -> [0, byte1, 0, byte3, ..]
        // p2 = [byte0, byte1, byte2, byte3, ..] -> [0, byte1, 0, byte3, ..]
        // and calculate 16bit multiplication with taking lower 16 bits
        // wa can use signed multiplication because sign bit is always 0
        let mul16 = _mm256_madd_epi16(
            _mm256_and_si256(p1, mask_epu16_epu8),
            _mm256_and_si256(p2, mask_epu16_epu8),
        );
        acc = _mm256_add_epi32(acc, mul16);

        // shift right by 1 byte for p1 and p2 and repeat previous steps
        let p1 = _mm256_bsrli_epi128(p1, 1);
        let p2 = _mm256_bsrli_epi128(p2, 1);

        let mul16 = _mm256_madd_epi16(
            _mm256_and_si256(p1, mask_epu16_epu8),
            _mm256_and_si256(p2, mask_epu16_epu8),
        );
        acc = _mm256_add_epi32(acc, mul16);
    }

    let mul_ps = _mm256_cvtepi32_ps(acc);
    let mut score = hsum256_ps_avx(mul_ps);

    let remainder = len % 32;
    if remainder != 0 {
        let mut remainder_dot = 0;
        for _ in 0..remainder {
            let v1 = *ptr1;
            let v2 = *ptr2;
            ptr1 = ptr1.add(1);
            ptr2 = ptr2.add(1);
            remainder_dot += (v1 as i32) * (v2 as i32);
        }
        score += remainder_dot as f32;
    }

    score
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spaces::metric_uint::simple_dot::dot_similarity_bytes;

    #[test]
    fn test_spaces_avx() {
        if is_x86_feature_detected!("avx")
            && is_x86_feature_detected!("avx2")
            && is_x86_feature_detected!("fma")
        {
            let v1: Vec<u8> = vec![
                255, 255, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 255, 255,
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 255, 255, 0, 1, 2, 3,
                4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 255, 255, 0, 1, 2, 3, 4, 5, 6, 7,
                8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 255, 255, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17,
            ];
            let v2: Vec<u8> = vec![
                255, 255, 0, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245, 244, 243, 242, 241,
                240, 239, 238, 255, 255, 255, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245,
                244, 243, 242, 241, 240, 239, 238, 255, 255, 255, 254, 253, 252, 251, 250, 249,
                248, 247, 246, 245, 244, 243, 242, 241, 240, 239, 238, 255, 255, 255, 254, 253,
                252, 251, 250, 249, 248, 247, 246, 245, 244, 243, 242, 241, 240, 239, 238, 255,
                255, 255, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245, 244, 243, 242, 241,
                240, 239, 238,
            ];

            let dot_simd = unsafe { avx_dot_similarity_bytes(&v1, &v2) };
            let dot = dot_similarity_bytes(&v1, &v2);
            assert_eq!(dot_simd, dot);
        } else {
            println!("avx2 test skipped");
        }
    }
}
