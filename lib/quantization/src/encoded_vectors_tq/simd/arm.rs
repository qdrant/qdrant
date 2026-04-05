//! ARM NEON implementation of 4-bit codebook dot product.
//!
//! Processes 16 coordinates per iteration with i64 accumulation.
//! Converts to float only once after the loop for maximum precision.

use super::{SimdCodebook4, SimdQuery4};

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;

/// NEON-accelerated 4-bit codebook dot product with i64 accumulation.
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
        let c_lo = vld1q_u8(codebook.centroid_bytes_lo.as_ptr());
        let c_hi = vld1q_u8(codebook.centroid_bytes_hi.as_ptr());
        let nibble_mask = vdup_n_u8(0x0F);

        // i64 accumulators (4 lanes total: 2 × int64x2_t)
        let mut acc_lo = vdupq_n_s64(0);
        let mut acc_hi = vdupq_n_s64(0);
        let num_iters = padded_dim / 16;

        for iter in 0..num_iters {
            let byte_offset = iter * 8;
            let query_offset = iter * 16;

            // Load 8 packed bytes, extract nibbles, interleave to 16 indices
            let packed_bytes = vld1_u8(packed.as_ptr().add(byte_offset));
            let hi_nibbles = vshr_n_u8(packed_bytes, 4);
            let lo_nibbles = vand_u8(packed_bytes, nibble_mask);
            let zipped = vzip_u8(hi_nibbles, lo_nibbles);
            let indices = vcombine_u8(zipped.0, zipped.1);

            // Table lookup → centroid bytes → combine to i16
            let looked_up_lo = vqtbl1q_u8(c_lo, indices);
            let looked_up_hi = vqtbl1q_u8(c_hi, indices);
            let centroids_0_7 =
                vreinterpretq_s16_u8(vzip1q_u8(looked_up_lo, looked_up_hi));
            let centroids_8_15 =
                vreinterpretq_s16_u8(vzip2q_u8(looked_up_lo, looked_up_hi));

            // Load query values
            let q_0_7 = vld1q_s16(query.values.as_ptr().add(query_offset));
            let q_8_15 = vld1q_s16(query.values.as_ptr().add(query_offset + 8));

            // i16 × i16 → i32 (2 products per lane via vmull+vmlal)
            let prod_0_7 = vmlal_s16(
                vmull_s16(vget_low_s16(centroids_0_7), vget_low_s16(q_0_7)),
                vget_high_s16(centroids_0_7),
                vget_high_s16(q_0_7),
            );
            let prod_8_15 = vmlal_s16(
                vmull_s16(vget_low_s16(centroids_8_15), vget_low_s16(q_8_15)),
                vget_high_s16(centroids_8_15),
                vget_high_s16(q_8_15),
            );

            // Widen i32 → i64 and accumulate
            acc_lo = vaddq_s64(acc_lo, vmovl_s32(vget_low_s32(prod_0_7)));
            acc_hi = vaddq_s64(acc_hi, vmovl_s32(vget_high_s32(prod_0_7)));
            acc_lo = vaddq_s64(acc_lo, vmovl_s32(vget_low_s32(prod_8_15)));
            acc_hi = vaddq_s64(acc_hi, vmovl_s32(vget_high_s32(prod_8_15)));
        }

        // Horizontal sum of 4 i64 lanes
        let combined = vaddq_s64(acc_lo, acc_hi);
        let sum = vgetq_lane_s64::<0>(combined) + vgetq_lane_s64::<1>(combined);

        // Scalar tail for remaining coords (< 16)
        let sum = scalar_tail(packed, codebook, query, num_iters * 16, padded_dim, sum);

        // Single float conversion at the end
        (sum as f64 / (codebook.centroid_scale as f64 * query.query_scale as f64)) as f32
    }
}

/// Scalar tail for coords not covered by SIMD iterations.
fn scalar_tail(
    packed: &[u8],
    codebook: &SimdCodebook4,
    query: &SimdQuery4,
    start_coord: usize,
    padded_dim: usize,
    mut sum: i64,
) -> i64 {
    let centroids_i16 = super::reconstruct_centroids_i16(codebook);
    for k in (start_coord / 2)..(padded_dim / 2) {
        let byte = packed[k];
        let coord = k * 2;
        sum += query.values[coord] as i64 * centroids_i16[(byte >> 4) as usize] as i64;
        sum += query.values[coord + 1] as i64 * centroids_i16[(byte & 0x0F) as usize] as i64;
    }
    sum
}
