use std::arch::x86_64::*;

use crate::spaces::simple_avx::hsum256_ps_avx;

#[target_feature(enable = "avx")]
#[target_feature(enable = "avx2")]
#[target_feature(enable = "fma")]
#[allow(clippy::missing_safety_doc)]
pub unsafe fn avx_cosine_similarity_bytes(v1: &[u8], v2: &[u8]) -> f32 {
    debug_assert!(v1.len() == v2.len());
    debug_assert!(is_x86_feature_detected!("avx"));
    debug_assert!(is_x86_feature_detected!("avx2"));
    debug_assert!(is_x86_feature_detected!("fma"));

    let mut ptr1: *const u8 = v1.as_ptr();
    let mut ptr2: *const u8 = v2.as_ptr();

    // sum accumulators for 8x32 bit integers
    let mut dot_acc = _mm256_setzero_si256();
    let mut norm1_acc = _mm256_setzero_si256();
    let mut norm2_acc = _mm256_setzero_si256();
    // mask to take only lower 8 bits from 16 bits
    let mask_epu16_epu8 = _mm256_set1_epi16(0xFF);
    let len = v1.len();
    for _ in 0..len / 32 {
        // load 32 bytes
        let p1 = _mm256_loadu_si256(ptr1 as *const __m256i);
        let p2 = _mm256_loadu_si256(ptr2 as *const __m256i);
        ptr1 = ptr1.add(32);
        ptr2 = ptr2.add(32);

        // convert 32x8 bit integers into 16x16 bit integers using bitwise AND
        // conversion is done by taking only lower 8 bits from 16 bits
        // p1 = [byte0, byte1, byte2, byte3, ..]
        // p1_low = [0, byte1, 0, byte3, ..]
        // p1_high = [0, byte0, 0, byte2, ..]
        let p1_low = _mm256_and_si256(p1, mask_epu16_epu8);
        let p1_high = _mm256_and_si256(_mm256_bsrli_epi128(p1, 1), mask_epu16_epu8);
        let p2_low = _mm256_and_si256(p2, mask_epu16_epu8);
        let p2_high = _mm256_and_si256(_mm256_bsrli_epi128(p2, 1), mask_epu16_epu8);

        // calculate 16bit multiplication with taking lower 16 bits and adding to accumulator
        let norm1_low = _mm256_madd_epi16(p1_low, p1_low);
        norm1_acc = _mm256_add_epi32(norm1_acc, norm1_low);

        // calculate 16bit multiplication with taking lower 16 bits and adding to accumulator
        let norm2_low = _mm256_madd_epi16(p2_low, p2_low);
        norm2_acc = _mm256_add_epi32(norm2_acc, norm2_low);

        // calculate 16bit multiplication with taking lower 16 bits and adding to accumulator
        let dot_low = _mm256_madd_epi16(p1_low, p2_low);
        dot_acc = _mm256_add_epi32(dot_acc, dot_low);

        // calculate 16bit multiplication with taking lower 16 bits and adding to accumulator
        let norm1_high = _mm256_madd_epi16(p1_high, p1_high);
        norm1_acc = _mm256_add_epi32(norm1_acc, norm1_high);

        // calculate 16bit multiplication with taking lower 16 bits and adding to accumulator
        let norm2_high = _mm256_madd_epi16(p2_high, p2_high);
        norm2_acc = _mm256_add_epi32(norm2_acc, norm2_high);

        // calculate 16bit multiplication with taking lower 16 bits and adding to accumulator
        let dot_high = _mm256_madd_epi16(p1_high, p2_high);
        dot_acc = _mm256_add_epi32(dot_acc, dot_high);
    }

    // convert 8x32 bit integers into 8x32 bit floats and calculate horizontal sum
    let dot_ps = _mm256_cvtepi32_ps(dot_acc);
    let mut dot_product = hsum256_ps_avx(dot_ps);

    // convert 8x32 bit integers into 8x32 bit floats and calculate horizontal sum
    let norm1_ps = _mm256_cvtepi32_ps(norm1_acc);
    let mut norm1 = hsum256_ps_avx(norm1_ps);

    // convert 8x32 bit integers into 8x32 bit floats and calculate horizontal sum
    let norm2_ps = _mm256_cvtepi32_ps(norm2_acc);
    let mut norm2 = hsum256_ps_avx(norm2_ps);

    let remainder = len % 32;
    if remainder != 0 {
        let mut remainder_dot_product = 0;
        let mut remainder_norm1 = 0;
        let mut remainder_norm2 = 0;
        for _ in 0..remainder {
            let v1 = *ptr1;
            let v2 = *ptr2;
            ptr1 = ptr1.add(1);
            ptr2 = ptr2.add(1);
            remainder_dot_product += (v1 as i32) * (v2 as i32);
            remainder_norm1 += (v1 as i32) * (v1 as i32);
            remainder_norm2 += (v2 as i32) * (v2 as i32);
        }
        dot_product += remainder_dot_product as f32;
        norm1 += remainder_norm1 as f32;
        norm2 += remainder_norm2 as f32;
    }

    dot_product / ((norm1 * norm2).sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spaces::metric_uint::simple_cosine::cosine_similarity_bytes;

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

            let dot_simd = unsafe { avx_cosine_similarity_bytes(&v1, &v2) };
            let dot = cosine_similarity_bytes(&v1, &v2);
            assert_eq!(dot_simd, dot);
        } else {
            println!("avx2 test skipped");
        }
    }
}
