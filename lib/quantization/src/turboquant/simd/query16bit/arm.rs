//! aarch64 kernel for 16-bit scoring (`score_16bit_internal`).
//!
//! `vmull_s16` + `vmlal_s16` put one pair of products into each i32 lane
//! (at most `2 · 32767² < 2³¹`, see the module docs) and `vpadalq_s32`
//! widens the pairs into i64 accumulators, so the result is exact.

use core::arch::aarch64::*;

use super::score_16bit_internal_integer;

/// NEON implementation of [`super::score_16bit_internal`].
///
/// # Safety
/// CPU must support `neon`; `a.len() == b.len()`.
#[target_feature(enable = "neon")]
pub unsafe fn score_16bit_internal_neon(a: &[u8], b: &[u8]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    // 16 codes (two registers) per step.
    const BLOCK: usize = 32;
    unsafe {
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        // Byte loads: no alignment requirement on the codes.
        macro_rules! load {
            ($p:expr) => {
                vreinterpretq_s16_u8(vld1q_u8($p))
            };
        }
        let mut acc = [vdupq_n_s64(0); 2];
        let blocks = a.len() / BLOCK;
        for i in 0..blocks {
            let off = i * BLOCK;
            let (a0, a1) = (load!(pa.add(off)), load!(pa.add(off + 16)));
            let (b0, b1) = (load!(pb.add(off)), load!(pb.add(off + 16)));
            let lo = vmull_s16(vget_low_s16(a0), vget_low_s16(b0));
            let lo = vmlal_s16(lo, vget_low_s16(a1), vget_low_s16(b1));
            let hi = vmlal_high_s16(vmull_high_s16(a0, b0), a1, b1);
            acc[0] = vpadalq_s32(acc[0], lo);
            acc[1] = vpadalq_s32(acc[1], hi);
        }
        let tail = blocks * BLOCK;
        let simd = vaddvq_s64(vaddq_s64(acc[0], acc[1]));
        (simd + score_16bit_internal_integer(&a[tail..], &b[tail..])) as f32
    }
}
