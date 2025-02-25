use std::arch::x86_64::*;

use crate::spaces::simple_sse::hsum128_ps_sse;

#[target_feature(enable = "sse")]
#[target_feature(enable = "sse2")]
#[allow(clippy::missing_safety_doc)]
pub unsafe fn sse_manhattan_similarity_bytes(v1: &[u8], v2: &[u8]) -> f32 {
    debug_assert!(v1.len() == v2.len());
    debug_assert!(is_x86_feature_detected!("sse"));
    debug_assert!(is_x86_feature_detected!("sse2"));

    let mut ptr1: *const u8 = v1.as_ptr();
    let mut ptr2: *const u8 = v2.as_ptr();

    unsafe {
        // sum accumulator for 4x32 bit integers
        let mut acc = _mm_setzero_si128();
        let len = v1.len();
        for _ in 0..len / 16 {
            // load 16 bytes
            let p1 = _mm_loadu_si128(ptr1.cast::<__m128i>());
            let p2 = _mm_loadu_si128(ptr2.cast::<__m128i>());
            ptr1 = ptr1.add(16);
            ptr2 = ptr2.add(16);

            // Computes the absolute differences of packed unsigned 8-bit integers in a and b
            // with horizontal sum and adding to accumulator
            let sad = _mm_sad_epu8(p1, p2);
            acc = _mm_add_epi32(acc, sad);
        }

        // convert 4x32 bit integers into 8x32 bit floats and calculate horizontal sum
        let mul_ps = _mm_cvtepi32_ps(acc);
        let mut score = hsum128_ps_sse(mul_ps);

        let remainder = len % 16;
        if remainder != 0 {
            let mut remainder_score = 0;
            for _ in 0..len % 16 {
                let v1 = i32::from(*ptr1);
                let v2 = i32::from(*ptr2);
                ptr1 = ptr1.add(1);
                ptr2 = ptr2.add(1);
                remainder_score += (v1 - v2).abs();
            }
            score += remainder_score as f32;
        }

        -score
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spaces::metric_uint::simple_manhattan::manhattan_similarity_bytes;

    #[test]
    fn test_spaces_sse() {
        if is_x86_feature_detected!("sse2") && is_x86_feature_detected!("sse") {
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

            let dot_simd = unsafe { sse_manhattan_similarity_bytes(&v1, &v2) };
            let dot = manhattan_similarity_bytes(&v1, &v2);
            assert_eq!(dot_simd, dot);
        } else {
            println!("sse2 test skipped");
        }
    }
}
