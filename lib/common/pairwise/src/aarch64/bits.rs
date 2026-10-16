//! Binary codes: the number of differing bits of every pair by XOR and byte popcounts.
//!
//! NEON counts the bits of 16 bytes in one instruction (`cnt`), so 16 depth bytes of a pair
//! (128 bit pairs) take three: `eor`, `cnt`, and an `add` into byte counters. A port of the
//! x86 nibble lookups would take two (`tbl` and `add`) per 64 bit pairs, and a driver of its
//! own. Here each pair has its own counters, reduced once per depth slice, and the blocked
//! product does everything else.

use std::arch::aarch64::*;

use crate::Bits;
use crate::blocked::Micro;

/// 4 x 4 tile of byte counters: 16 accumulators, 4 row and 4 column vectors, 24 of the 32
/// vector registers. Per depth step 8 loads serve 16 pairs of 16 bytes.
pub(super) struct BitsTile;

impl Micro for BitsTile {
    type E = Bits;
    type Acc = u16;
    type Operand = Bits;
    const MR: usize = 4;
    const NR: usize = 4;
    /// A vector of depth bytes per pair
    const KU: usize = 16;
    /// 31 steps: a byte counter gains at most 8 per step, 248 in all
    const KC: usize = 31 * 16;
    const MC: usize = 48;
    /// Unmeasured. A block of 128 codes of 1536 bits is 24 KB, in L1 or L2
    const NC: usize = 128;

    #[inline(always)]
    unsafe fn tile(
        kc: usize,
        a: *const Self::Operand,
        b: *const Self::Operand,
        c: *mut u16,
        ldc: usize,
        acc: bool,
        (): (),
        _: usize,
    ) {
        // SAFETY: forwarded from the caller, the same contract; `Bits` is a transparent `u8`
        unsafe { tile_bits(kc, a.cast(), b.cast(), c, ldc, acc) }
    }
}

/// See [`Micro::tile`], with `kc <= BitsTile::KC`. The byte counters of each row are widened
/// and summed pairwise into one u16 per column: exact, as a pair's count over a slice is at
/// most `8 * kc`, and over all slices `8 * depth <= u16::MAX`.
#[target_feature(enable = "neon")]
#[inline]
unsafe fn tile_bits(kc: usize, a: *const u8, b: *const u8, c: *mut u16, ldc: usize, acc: bool) {
    const MR: usize = BitsTile::MR;
    const NR: usize = BitsTile::NR;
    const KU: usize = BitsTile::KU;
    debug_assert!(kc <= BitsTile::KC);
    // SAFETY: per step, `MR * KU` bytes of `a` and `NR * KU` of `b`, `kc / KU` steps, and `NR`
    // values of each of the `MR` rows of `c`: all within the panels and the tile, from the
    // caller.
    unsafe {
        let mut sums = [[vdupq_n_u8(0); NR]; MR];
        let (mut a, mut b) = (a, b);
        for _ in 0..kc / KU {
            let av: [_; MR] = std::array::from_fn(|r| vld1q_u8(a.add(r * KU)));
            let bv: [_; NR] = std::array::from_fn(|j| vld1q_u8(b.add(j * KU)));
            for (sums, &ar) in sums.iter_mut().zip(&av) {
                for (sum, &bj) in sums.iter_mut().zip(&bv) {
                    *sum = vaddq_u8(*sum, vcntq_u8(veorq_u8(ar, bj)));
                }
            }
            a = a.add(MR * KU);
            b = b.add(NR * KU);
        }
        for (r, sums) in sums.iter().enumerate() {
            // 8 partial counts per column, then 4, 2, and 1: `[c0, c1, c2, c3]` twice
            let [w0, w1, w2, w3] = sums.map(|s| vpaddlq_u8(s));
            let halves = vpaddq_u16(vpaddq_u16(w0, w1), vpaddq_u16(w2, w3));
            let mut counts = vget_low_u16(vpaddq_u16(halves, halves));
            let cr = c.add(r * ldc);
            if acc {
                counts = vadd_u16(counts, vld1_u16(cr));
            }
            vst1_u16(cr, counts);
        }
    }
}
