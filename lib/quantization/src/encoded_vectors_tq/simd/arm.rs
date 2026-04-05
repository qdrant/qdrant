//! ARM NEON implementations of 4-bit and 2-bit codebook dot product.
//!
//! All kernels accumulate in i64 and convert to float once at the end.

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;

use super::{SimdCodebook2, SimdCodebook4, SimdQuery1, SimdQuery2, SimdQuery4};

// ============================================================================
// 4-bit kernel — 16 coords/iter
// ============================================================================

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

        let mut acc_lo = vdupq_n_s64(0);
        let mut acc_hi = vdupq_n_s64(0);
        let num_iters = padded_dim / 16;

        for iter in 0..num_iters {
            let byte_offset = iter * 8;
            let query_offset = iter * 16;

            let packed_bytes = vld1_u8(packed.as_ptr().add(byte_offset));
            let hi_nibbles = vshr_n_u8(packed_bytes, 4);
            let lo_nibbles = vand_u8(packed_bytes, nibble_mask);
            let zipped = vzip_u8(hi_nibbles, lo_nibbles);
            let indices = vcombine_u8(zipped.0, zipped.1);

            let looked_up_lo = vqtbl1q_u8(c_lo, indices);
            let looked_up_hi = vqtbl1q_u8(c_hi, indices);
            let centroids_0_7 = vreinterpretq_s16_u8(vzip1q_u8(looked_up_lo, looked_up_hi));
            let centroids_8_15 = vreinterpretq_s16_u8(vzip2q_u8(looked_up_lo, looked_up_hi));

            let q_0_7 = vld1q_s16(query.values.as_ptr().add(query_offset));
            let q_8_15 = vld1q_s16(query.values.as_ptr().add(query_offset + 8));

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

            acc_lo = vaddq_s64(acc_lo, vmovl_s32(vget_low_s32(prod_0_7)));
            acc_hi = vaddq_s64(acc_hi, vmovl_s32(vget_high_s32(prod_0_7)));
            acc_lo = vaddq_s64(acc_lo, vmovl_s32(vget_low_s32(prod_8_15)));
            acc_hi = vaddq_s64(acc_hi, vmovl_s32(vget_high_s32(prod_8_15)));
        }

        let combined = vaddq_s64(acc_lo, acc_hi);
        let sum = vgetq_lane_s64::<0>(combined) + vgetq_lane_s64::<1>(combined);

        let sum = scalar_tail_4bit(packed, codebook, query, num_iters * 16, padded_dim, sum);
        (sum as f64 / (codebook.centroid_scale as f64 * query.query_scale as f64)) as f32
    }
}

fn scalar_tail_4bit(
    packed: &[u8],
    codebook: &SimdCodebook4,
    query: &SimdQuery4,
    start_coord: usize,
    padded_dim: usize,
    mut sum: i64,
) -> i64 {
    let centroids_i16 =
        super::reconstruct_centroids_i16(&codebook.centroid_bytes_lo, &codebook.centroid_bytes_hi);
    for k in (start_coord / 2)..(padded_dim / 2) {
        let byte = packed[k];
        let coord = k * 2;
        sum += query.values[coord] as i64 * centroids_i16[(byte >> 4) as usize] as i64;
        sum += query.values[coord + 1] as i64 * centroids_i16[(byte & 0x0F) as usize] as i64;
    }
    sum
}

// ============================================================================
// 2-bit fused kernel — 32 coords/iter (16 nibbles, 2 coords each)
// ============================================================================

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
pub unsafe fn codebook_dot_2bit_neon(
    packed: &[u8],
    codebook: &SimdCodebook2,
    query: &SimdQuery2,
    padded_dim: usize,
) -> f32 {
    unsafe {
        // Load 4 centroid tables (even lo/hi, odd lo/hi)
        let ce_lo = vld1q_u8(codebook.even_lo.as_ptr());
        let ce_hi = vld1q_u8(codebook.even_hi.as_ptr());
        let co_lo = vld1q_u8(codebook.odd_lo.as_ptr());
        let co_hi = vld1q_u8(codebook.odd_hi.as_ptr());
        let nibble_mask = vdup_n_u8(0x0F);

        let mut acc_lo = vdupq_n_s64(0);
        let mut acc_hi = vdupq_n_s64(0);
        // 16 nibbles per iter = 32 coords, consumed from padded_dim/2 nibbles total
        let num_nibbles = padded_dim / 2;
        let num_iters = num_nibbles / 16;

        for iter in 0..num_iters {
            let byte_offset = iter * 8; // 16 nibbles = 8 bytes
            let q_offset = iter * 16; // 16 even/odd query values each

            // Load 8 packed bytes → 16 nibble indices
            let packed_bytes = vld1_u8(packed.as_ptr().add(byte_offset));
            let hi_nibbles = vshr_n_u8(packed_bytes, 4);
            let lo_nibbles = vand_u8(packed_bytes, nibble_mask);
            let zipped = vzip_u8(hi_nibbles, lo_nibbles);
            let indices = vcombine_u8(zipped.0, zipped.1);

            // Even centroid lookup → 16 i16 values
            let elu = vqtbl1q_u8(ce_lo, indices);
            let ehu = vqtbl1q_u8(ce_hi, indices);
            let even_0_7 = vreinterpretq_s16_u8(vzip1q_u8(elu, ehu));
            let even_8_15 = vreinterpretq_s16_u8(vzip2q_u8(elu, ehu));

            // Odd centroid lookup → 16 i16 values
            let olu = vqtbl1q_u8(co_lo, indices);
            let ohu = vqtbl1q_u8(co_hi, indices);
            let odd_0_7 = vreinterpretq_s16_u8(vzip1q_u8(olu, ohu));
            let odd_8_15 = vreinterpretq_s16_u8(vzip2q_u8(olu, ohu));

            // Load 16 even + 16 odd query i16 values
            let qe_0_7 = vld1q_s16(query.even_values.as_ptr().add(q_offset));
            let qe_8_15 = vld1q_s16(query.even_values.as_ptr().add(q_offset + 8));
            let qo_0_7 = vld1q_s16(query.odd_values.as_ptr().add(q_offset));
            let qo_8_15 = vld1q_s16(query.odd_values.as_ptr().add(q_offset + 8));

            // Even: i16 × i16 → i32 (2 products per lane)
            let eprod_0_7 = vmlal_s16(
                vmull_s16(vget_low_s16(even_0_7), vget_low_s16(qe_0_7)),
                vget_high_s16(even_0_7),
                vget_high_s16(qe_0_7),
            );
            let eprod_8_15 = vmlal_s16(
                vmull_s16(vget_low_s16(even_8_15), vget_low_s16(qe_8_15)),
                vget_high_s16(even_8_15),
                vget_high_s16(qe_8_15),
            );

            // Odd: i16 × i16 → i32 (2 products per lane)
            let oprod_0_7 = vmlal_s16(
                vmull_s16(vget_low_s16(odd_0_7), vget_low_s16(qo_0_7)),
                vget_high_s16(odd_0_7),
                vget_high_s16(qo_0_7),
            );
            let oprod_8_15 = vmlal_s16(
                vmull_s16(vget_low_s16(odd_8_15), vget_low_s16(qo_8_15)),
                vget_high_s16(odd_8_15),
                vget_high_s16(qo_8_15),
            );

            // Widen each i32 → i64 separately to avoid i32 overflow
            acc_lo = vaddq_s64(acc_lo, vmovl_s32(vget_low_s32(eprod_0_7)));
            acc_hi = vaddq_s64(acc_hi, vmovl_s32(vget_high_s32(eprod_0_7)));
            acc_lo = vaddq_s64(acc_lo, vmovl_s32(vget_low_s32(eprod_8_15)));
            acc_hi = vaddq_s64(acc_hi, vmovl_s32(vget_high_s32(eprod_8_15)));
            acc_lo = vaddq_s64(acc_lo, vmovl_s32(vget_low_s32(oprod_0_7)));
            acc_hi = vaddq_s64(acc_hi, vmovl_s32(vget_high_s32(oprod_0_7)));
            acc_lo = vaddq_s64(acc_lo, vmovl_s32(vget_low_s32(oprod_8_15)));
            acc_hi = vaddq_s64(acc_hi, vmovl_s32(vget_high_s32(oprod_8_15)));
        }

        let combined = vaddq_s64(acc_lo, acc_hi);
        let sum = vgetq_lane_s64::<0>(combined) + vgetq_lane_s64::<1>(combined);

        let sum = scalar_tail_2bit(packed, codebook, query, num_iters * 16, num_nibbles, sum);
        (sum as f64 / (codebook.centroid_scale as f64 * query.query_scale as f64)) as f32
    }
}

fn scalar_tail_2bit(
    packed: &[u8],
    codebook: &SimdCodebook2,
    query: &SimdQuery2,
    start_nib: usize,
    num_nibbles: usize,
    mut sum: i64,
) -> i64 {
    let even_i16 = super::reconstruct_centroids_i16(&codebook.even_lo, &codebook.even_hi);
    let odd_i16 = super::reconstruct_centroids_i16(&codebook.odd_lo, &codebook.odd_hi);
    for nib_idx in start_nib..num_nibbles {
        let byte = packed[nib_idx / 2];
        let nibble = if nib_idx % 2 == 0 {
            (byte >> 4) as usize
        } else {
            (byte & 0x0F) as usize
        };
        sum += query.even_values[nib_idx] as i64 * even_i16[nibble] as i64;
        sum += query.odd_values[nib_idx] as i64 * odd_i16[nibble] as i64;
    }
    sum
}

// ============================================================================
// 1-bit AND+popcount kernel — 64 dims/iter
// ============================================================================

/// # Safety
/// Requires NEON support (always available on aarch64).
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
pub unsafe fn and_popcount_u16_neon(packed: &[u8], query: &SimdQuery1) -> (u64, u32) {
    unsafe {
        let mut s1 = 0u64;
        let mut popcnt_v = 0u32;
        for chunk in 0..query.num_chunks {
            // Load 8 bytes (64 bits) of packed vector
            let v = vld1_u8(packed.as_ptr().add(chunk * 8));

            // popcount(v): byte-level popcount → horizontal sum
            popcnt_v += vaddv_u8(vcnt_u8(v)) as u32;

            // AND+popcount for each of 16 bit planes
            let base = chunk * 16;
            for b in 0..16u64 {
                let plane = vld1_u8(&query.planes[base + b as usize] as *const u64 as *const u8);
                let count = vaddv_u8(vcnt_u8(vand_u8(v, plane))) as u64;
                s1 += count << b;
            }
        }
        (s1, popcnt_v)
    }
}

// ============================================================================
// 1-bit XOR+popcount for score_internal
// ============================================================================

/// # Safety
/// Requires NEON support (always available on aarch64).
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
pub unsafe fn xor_popcount_neon(packed1: &[u8], packed2: &[u8], num_bytes: usize) -> u32 {
    unsafe {
        let mut acc = vdupq_n_u32(0);
        let num_u128 = num_bytes / 16;
        for i in 0..num_u128 {
            let off = i * 16;
            let a = vld1q_u8(packed1.as_ptr().add(off));
            let b = vld1q_u8(packed2.as_ptr().add(off));
            let xored = veorq_u8(a, b);
            // vcntq_u8 → per-byte popcount, vpaddlq → widen-accumulate
            let cnt8 = vcntq_u8(xored);
            let cnt16 = vpaddlq_u8(cnt8);
            let cnt32 = vpaddlq_u16(cnt16);
            acc = vaddq_u32(acc, cnt32);
        }
        let mut count = vaddvq_u32(acc);
        // Tail bytes
        for i in (num_u128 * 16)..num_bytes {
            count += (*packed1.as_ptr().add(i) ^ *packed2.as_ptr().add(i)).count_ones();
        }
        count
    }
}
