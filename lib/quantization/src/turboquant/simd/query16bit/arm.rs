//! aarch64 kernel for 16-bit scoring (`score_16bit_internal`).
//!
//! `vmull_s16` + `vmlal_s16` put one pair of products into each i32 lane
//! (at most `2 · 32767² < 2³¹`, see the module docs) and `vpadalq_s32`
//! widens the pairs into i64 accumulators, so the result is exact.
//!
//! The main loop keeps four independent `vpadalq_s32` accumulator chains.
//! Measured on Neoverse N2 against two chains: equal with the vectors in L2
//! (the loop is bound by L2 bandwidth there), 1-12% faster from DRAM.

use core::arch::aarch64::*;

use super::score_16bit_internal_integer;

/// NEON implementation of [`super::score_16bit_internal`].
///
/// # Safety
/// CPU must support `neon`; `a.len() == b.len()`.
#[target_feature(enable = "neon")]
pub unsafe fn score_16bit_internal_neon(a: &[u8], b: &[u8]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    // One register holds 8 codes (16 bytes).
    const REG: usize = 16;
    unsafe {
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        let len = a.len();
        // Byte loads: no alignment requirement on the codes.
        macro_rules! load {
            ($p:expr) => {
                vreinterpretq_s16_u8(vld1q_u8($p))
            };
        }
        // Pair sums of two registers' products, one pair per i32 lane.
        macro_rules! pairs_lo {
            ($a0:expr, $b0:expr, $a1:expr, $b1:expr) => {
                vmlal_s16(
                    vmull_s16(vget_low_s16($a0), vget_low_s16($b0)),
                    vget_low_s16($a1),
                    vget_low_s16($b1),
                )
            };
        }
        macro_rules! pairs_hi {
            ($a0:expr, $b0:expr, $a1:expr, $b1:expr) => {
                vmlal_high_s16(vmull_high_s16($a0, $b0), $a1, $b1)
            };
        }
        let mut acc = [vdupq_n_s64(0); 4];
        let mut off = 0;
        // 32 codes per step, four chains.
        while off + 4 * REG <= len {
            let (a0, a1, a2, a3) = (
                load!(pa.add(off)),
                load!(pa.add(off + REG)),
                load!(pa.add(off + 2 * REG)),
                load!(pa.add(off + 3 * REG)),
            );
            let (b0, b1, b2, b3) = (
                load!(pb.add(off)),
                load!(pb.add(off + REG)),
                load!(pb.add(off + 2 * REG)),
                load!(pb.add(off + 3 * REG)),
            );
            acc[0] = vpadalq_s32(acc[0], pairs_lo!(a0, b0, a1, b1));
            acc[1] = vpadalq_s32(acc[1], pairs_hi!(a0, b0, a1, b1));
            acc[2] = vpadalq_s32(acc[2], pairs_lo!(a2, b2, a3, b3));
            acc[3] = vpadalq_s32(acc[3], pairs_hi!(a2, b2, a3, b3));
            off += 4 * REG;
        }
        // 16 codes.
        if off + 2 * REG <= len {
            let (a0, a1) = (load!(pa.add(off)), load!(pa.add(off + REG)));
            let (b0, b1) = (load!(pb.add(off)), load!(pb.add(off + REG)));
            acc[0] = vpadalq_s32(acc[0], pairs_lo!(a0, b0, a1, b1));
            acc[1] = vpadalq_s32(acc[1], pairs_hi!(a0, b0, a1, b1));
            off += 2 * REG;
        }
        // 8 codes: a single product per lane.
        if off + REG <= len {
            let (a0, b0) = (load!(pa.add(off)), load!(pb.add(off)));
            acc[2] = vpadalq_s32(acc[2], vmull_s16(vget_low_s16(a0), vget_low_s16(b0)));
            acc[3] = vpadalq_s32(acc[3], vmull_high_s16(a0, b0));
            off += REG;
        }
        let sum = vaddq_s64(vaddq_s64(acc[0], acc[1]), vaddq_s64(acc[2], acc[3]));
        // At most 7 codes left.
        (vaddvq_s64(sum) + score_16bit_internal_integer(&a[off..], &b[off..])) as f32
    }
}
