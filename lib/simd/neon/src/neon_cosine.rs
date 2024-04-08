use std::arch::aarch64::*;

#[target_feature(enable = "neon")]
#[allow(unused)]
pub unsafe fn neon_cosine_similarity_bytes(v1: &[u8], v2: &[u8]) -> f32 {
    debug_assert!(v1.len() == v2.len());
    let mut ptr1: *const u8 = v1.as_ptr();
    let mut ptr2: *const u8 = v2.as_ptr();

    let mut mul1 = vdupq_n_u32(0);
    let mut mul2 = vdupq_n_u32(0);
    let mut norm11 = vdupq_n_u32(0);
    let mut norm12 = vdupq_n_u32(0);
    let mut norm21 = vdupq_n_u32(0);
    let mut norm22 = vdupq_n_u32(0);
    let len = v1.len();
    for _ in 0..len / 16 {
        let p1 = vld1q_u8(ptr1);
        let p2 = vld1q_u8(ptr2);
        ptr1 = ptr1.add(16);
        ptr2 = ptr2.add(16);

        let p1_low = vget_low_u8(p1);
        let p1_high = vget_high_u8(p1);
        let p2_low = vget_low_u8(p2);
        let p2_high = vget_high_u8(p2);

        let mul_low = vmull_u8(p1_low, p2_low);
        let mul_high = vmull_u8(p1_high, p2_high);
        mul1 = vpadalq_u16(mul1, mul_low);
        mul2 = vpadalq_u16(mul2, mul_high);

        let mul_low = vmull_u8(p1_low, p1_low);
        let mul_high = vmull_u8(p1_high, p1_high);
        norm11 = vpadalq_u16(norm11, mul_low);
        norm12 = vpadalq_u16(norm12, mul_high);

        let mul_low = vmull_u8(p2_low, p2_low);
        let mul_high = vmull_u8(p2_high, p2_high);
        norm21 = vpadalq_u16(norm21, mul_low);
        norm22 = vpadalq_u16(norm22, mul_high);
    }
    let mut dot_product = vaddvq_u32(vaddq_u32(mul1, mul2)) as f32;
    let mut norm1 = vaddvq_u32(vaddq_u32(norm11, norm12)) as f32;
    let mut norm2 = vaddvq_u32(vaddq_u32(norm21, norm22)) as f32;

    let remainder = len % 16;
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

    dot_product as f32 / ((norm1 as f32 * norm2 as f32).sqrt())
}

#[cfg(test)]
mod tests {
    use std::arch::is_aarch64_feature_detected;

    use super::*;

    fn cosine_similarity_bytes(v1: &[u8], v2: &[u8]) -> f32 {
        let mut dot_product = 0;
        let mut norm1 = 0;
        let mut norm2 = 0;

        for (a, b) in v1.iter().zip(v2) {
            dot_product += (*a as i32) * (*b as i32);
            norm1 += (*a as i32) * (*a as i32);
            norm2 += (*b as i32) * (*b as i32);
        }

        if norm1 == 0 || norm2 == 0 {
            return 0.0;
        }

        dot_product as f32 / ((norm1 as f32 * norm2 as f32).sqrt())
    }

    #[test]
    fn test_spaces_avx() {
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

            let dot_simd = unsafe { neon_cosine_similarity_bytes(&v1, &v2) };
            let dot = cosine_similarity_bytes(&v1, &v2);
            assert_eq!(dot_simd, dot);
        } else {
            println!("avx test skipped");
        }
    }
}
