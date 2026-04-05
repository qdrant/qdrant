//! ARM NEON implementation of 4-bit codebook dot product.
//!
//! Processes 16 coordinates per iteration:
//! 1. Load 8 packed bytes (16 × 4-bit indices)
//! 2. Extract nibbles into 16 u8 indices
//! 3. Use `vqtbl1q_u8` to look up i16 centroid bytes (lo/hi tables)
//! 4. Recombine bytes into 16 i16 centroid values
//! 5. Multiply i16 × i16 → i32 and accumulate
//! 6. Convert to f32 and sum

use super::{SimdCodebook4, SimdQuery4};

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;

/// NEON-accelerated 4-bit codebook dot product.
///
/// # Safety
/// Requires NEON support (always available on aarch64).
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
pub unsafe fn codebook_dot_neon(
    packed: &[u8],
    codebook: &SimdCodebook4,
    query: &SimdQuery4,
    padded_dim: usize,
) -> f32 {
    unsafe {
        // Load centroid byte tables into NEON registers
        let c_lo = vld1q_u8(codebook.centroid_bytes_lo.as_ptr());
        let c_hi = vld1q_u8(codebook.centroid_bytes_hi.as_ptr());
        let nibble_mask = vdup_n_u8(0x0F);

        let mut acc = vdupq_n_f32(0.0);
        let num_iters = padded_dim / 16;

        for iter in 0..num_iters {
            let byte_offset = iter * 8; // 16 coords × 4 bits / 8 = 8 bytes
            let query_offset = iter * 16;

            // 1. Load 8 bytes of packed 4-bit indices
            let packed_bytes = vld1_u8(packed.as_ptr().add(byte_offset));

            // 2. Extract nibbles
            // High nibble = even coords, low nibble = odd coords
            let hi_nibbles = vshr_n_u8(packed_bytes, 4);
            let lo_nibbles = vand_u8(packed_bytes, nibble_mask);

            // 3. Interleave to sequential order: [idx0, idx1, idx2, idx3, ...]
            let zipped = vzip_u8(hi_nibbles, lo_nibbles);
            let indices = vcombine_u8(zipped.0, zipped.1);

            // 4. Look up centroid bytes via 16-entry table lookup
            let looked_up_lo = vqtbl1q_u8(c_lo, indices);
            let looked_up_hi = vqtbl1q_u8(c_hi, indices);

            // 5. Combine lo/hi bytes into i16 centroid values
            // zip1: interleave first 8 bytes → 8 i16 values (coords 0-7)
            let centroids_0_7 =
                vreinterpretq_s16_u8(vzip1q_u8(looked_up_lo, looked_up_hi));
            // zip2: interleave last 8 bytes → 8 i16 values (coords 8-15)
            let centroids_8_15 =
                vreinterpretq_s16_u8(vzip2q_u8(looked_up_lo, looked_up_hi));

            // 6. Load 16 i16 query values
            let q_0_7 = vld1q_s16(query.values.as_ptr().add(query_offset));
            let q_8_15 = vld1q_s16(query.values.as_ptr().add(query_offset + 8));

            // 7. Multiply and accumulate: i16 × i16 → i32
            // Process coords 0-7 (2 products per i32 lane to avoid overflow)
            let prod_lo =
                vmull_s16(vget_low_s16(centroids_0_7), vget_low_s16(q_0_7));
            let prod_0_7 = vmlal_s16(
                prod_lo,
                vget_high_s16(centroids_0_7),
                vget_high_s16(q_0_7),
            );
            acc = vaddq_f32(acc, vcvtq_f32_s32(prod_0_7));

            // Process coords 8-15
            let prod_mid = vmull_s16(
                vget_low_s16(centroids_8_15),
                vget_low_s16(q_8_15),
            );
            let prod_8_15 = vmlal_s16(
                prod_mid,
                vget_high_s16(centroids_8_15),
                vget_high_s16(q_8_15),
            );
            acc = vaddq_f32(acc, vcvtq_f32_s32(prod_8_15));
        }

        // Horizontal sum of 4 f32 lanes
        let sum = vaddvq_f32(acc);

        // Convert integer dot product back to f32
        sum / (codebook.centroid_scale * query.query_scale)
    }
}
