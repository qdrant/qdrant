use std::arch::aarch64::*;

#[target_feature(enable = "neon")]
#[allow(clippy::missing_safety_doc)]
pub unsafe fn neon_manhattan_similarity_bytes(v1: &[u8], v2: &[u8]) -> f32 {
    debug_assert!(v1.len() == v2.len());
    let mut ptr1: *const u8 = v1.as_ptr();
    let mut ptr2: *const u8 = v2.as_ptr();

    let mut sum16_low = vdupq_n_u16(0);
    let mut sum16_high = vdupq_n_u16(0);
    let len = v1.len();
    for _ in 0..len / 16 {
        let p1 = vld1q_u8(ptr1);
        let p2 = vld1q_u8(ptr2);
        ptr1 = ptr1.add(16);
        ptr2 = ptr2.add(16);

        let abs_diff = vabdq_u8(p1, p2);
        let abs_diff16_low = vmovl_u8(vget_low_u8(abs_diff));
        let abs_diff16_high = vmovl_u8(vget_high_u8(abs_diff));

        sum16_low = vaddq_u16(sum16_low, abs_diff16_low);
        sum16_high = vaddq_u16(sum16_high, abs_diff16_high);
    }
    // Horizontal sum of 16-bit integers
    let sum32_low = vpaddlq_u16(sum16_low);
    let sum32_high = vpaddlq_u16(sum16_high);
    let sum32 = vaddq_u32(sum32_low, sum32_high);

    let sum64_low = vadd_u32(vget_low_u32(sum32), vget_high_u32(sum32));
    let sum64_high = vpadd_u32(sum64_low, sum64_low);
    let mut score = vget_lane_u32(sum64_high, 0) as f32;

    let remainder = len % 16;
    if remainder != 0 {
        let mut remainder_score = 0;
        for _ in 0..len % 16 {
            let v1 = *ptr1 as i32;
            let v2 = *ptr2 as i32;
            ptr1 = ptr1.add(1);
            ptr2 = ptr2.add(1);
            remainder_score += (v1 - v2).abs();
        }
        score += remainder_score as f32;
    }

    -score
}

#[cfg(test)]
mod tests {
    use std::arch::is_aarch64_feature_detected;

    use super::*;
    use crate::spaces::metric_uint::simple_manhattan::manhattan_similarity_bytes;

    #[test]
    fn test_spaces_neon() {
        if is_aarch64_feature_detected!("neon") {
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

            let dot_simd = unsafe { neon_manhattan_similarity_bytes(&v1, &v2) };
            let dot = manhattan_similarity_bytes(&v1, &v2);
            assert_eq!(dot_simd, dot);
        } else {
            println!("avx test skipped");
        }
    }
}
