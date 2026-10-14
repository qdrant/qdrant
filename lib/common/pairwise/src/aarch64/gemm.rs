//! Register tiles of the blocked product for f32 and u8.
//!
//! Both are 8 x 12: 24 accumulators of 4 lanes, 3 vectors of columns, and 2 of rows, 29 of
//! the 32 vector registers. Per depth step 5 loads serve 24 multiply-adds by element, each
//! multiplying a column vector by one row lane, so rows need no broadcasts.

use std::arch::aarch64::*;

use crate::blocked::Micro;

/// 8 x 12 f32 FMA tile.
pub(super) struct F32Tile;

impl Micro for F32Tile {
    type E = f32;
    type Acc = f32;
    const MR: usize = 8;
    const NR: usize = 12;
    const KU: usize = 1;
    /// Panels of 8 KB and 12 KB, in L1
    const KC: usize = 256;
    const MC: usize = 48;
    /// The multiple of 12 nearest the x86 tiles' 128, unmeasured
    const NC: usize = 132;

    #[inline(always)]
    unsafe fn tile(kc: usize, a: *const f32, b: *const f32, c: *mut f32, ldc: usize, acc: bool) {
        // SAFETY: forwarded from the caller, the same contract
        unsafe { tile_f32(kc, a, b, c, ldc, acc) }
    }

    #[inline(always)]
    unsafe fn transpose8x8(src: *const u32, ss: usize, dst: *mut u32, ds: usize) {
        // SAFETY: forwarded from the caller, the same contract
        unsafe { transpose8x8(src, ss, dst, ds) }
    }
}

/// 8 x 12 u8 tile with i32 accumulators, by `udot`: each lane of a column vector sums the 4
/// products of one column's 4 depth bytes with one row lane's 4. Exact for any bytes as long
/// as the sums fit an `i32`, which values up to 127 guarantee.
pub(super) struct U8Tile;

impl Micro for U8Tile {
    type E = u8;
    type Acc = i32;
    const MR: usize = 8;
    const NR: usize = 12;
    const KU: usize = 4;
    /// The f32 tile's slice in bytes
    const KC: usize = 1024;
    const MC: usize = 48;
    /// As for the f32 tile
    const NC: usize = 132;

    #[inline(always)]
    unsafe fn tile(kc: usize, a: *const u8, b: *const u8, c: *mut i32, ldc: usize, acc: bool) {
        // SAFETY: forwarded from the caller, the same contract
        unsafe { tile_u8(kc, a, b, c, ldc, acc) }
    }

    #[inline(always)]
    unsafe fn transpose8x8(src: *const u32, ss: usize, dst: *mut u32, ds: usize) {
        // SAFETY: forwarded from the caller, the same contract
        unsafe { transpose8x8(src, ss, dst, ds) }
    }
}

/// `acc + b · a[lane]`, fused. The unrolled row loops of the tiles make `lane` a constant
/// after inlining, so the match folds into one `fmla` by element.
#[target_feature(enable = "neon")]
#[inline]
fn fma_lane(acc: float32x4_t, b: float32x4_t, a: float32x4_t, lane: usize) -> float32x4_t {
    match lane {
        0 => vfmaq_laneq_f32::<0>(acc, b, a),
        1 => vfmaq_laneq_f32::<1>(acc, b, a),
        2 => vfmaq_laneq_f32::<2>(acc, b, a),
        _ => vfmaq_laneq_f32::<3>(acc, b, a),
    }
}

/// `acc + Σ₄ b · a[lane]` per lane, over the 4 bytes of each 32-bit lane: one `udot` by
/// element, as [`fma_lane`] is one `fmla`.
#[target_feature(enable = "neon,dotprod")]
#[inline]
fn dot_lane(acc: uint32x4_t, b: uint8x16_t, a: uint8x16_t, lane: usize) -> uint32x4_t {
    match lane {
        0 => vdotq_laneq_u32::<0>(acc, b, a),
        1 => vdotq_laneq_u32::<1>(acc, b, a),
        2 => vdotq_laneq_u32::<2>(acc, b, a),
        _ => vdotq_laneq_u32::<3>(acc, b, a),
    }
}

/// See [`Micro::tile`]. The accumulators start from `c` instead of adding it at the end, so
/// each score is one chain of fused multiply-adds in depth order, whatever the slice size.
#[target_feature(enable = "neon")]
#[inline]
unsafe fn tile_f32(kc: usize, a: *const f32, b: *const f32, c: *mut f32, ldc: usize, acc: bool) {
    const MR: usize = F32Tile::MR;
    const NR: usize = F32Tile::NR;
    const V: usize = NR / 4;
    // SAFETY: per step, `MR` values of `a` and `NR` of `b`, `kc` steps, and `NR` values of each
    // of the `MR` rows of `c`: all within the panels and the tile, from the caller.
    unsafe {
        let mut sums = [[vdupq_n_f32(0.0); V]; MR];
        if acc {
            for (r, sums) in sums.iter_mut().enumerate() {
                for (v, sum) in sums.iter_mut().enumerate() {
                    *sum = vld1q_f32(c.add(r * ldc + 4 * v));
                }
            }
        }
        let (mut a, mut b) = (a, b);
        for _ in 0..kc {
            let bv: [_; V] = std::array::from_fn(|v| vld1q_f32(b.add(4 * v)));
            let av: [_; MR / 4] = std::array::from_fn(|h| vld1q_f32(a.add(4 * h)));
            for (r, sums) in sums.iter_mut().enumerate() {
                for (sum, &bv) in sums.iter_mut().zip(&bv) {
                    *sum = fma_lane(*sum, bv, av[r / 4], r % 4);
                }
            }
            a = a.add(MR);
            b = b.add(NR);
        }
        for (r, sums) in sums.iter().enumerate() {
            for (v, &sum) in sums.iter().enumerate() {
                vst1q_f32(c.add(r * ldc + 4 * v), sum);
            }
        }
    }
}

/// See [`Micro::tile`]. Starts from `c` as the f32 tile does, in `u32` lanes: the sums are
/// exact and below `i32::MAX`, so the bits are the same in both types.
#[target_feature(enable = "neon,dotprod")]
#[inline]
unsafe fn tile_u8(kc: usize, a: *const u8, b: *const u8, c: *mut i32, ldc: usize, acc: bool) {
    const MR: usize = U8Tile::MR;
    const NR: usize = U8Tile::NR;
    const V: usize = NR / 4;
    // SAFETY: per step, `MR * 4` bytes of `a` and `NR * 4` of `b`, `kc / 4` steps, and `NR`
    // values of each of the `MR` rows of `c`: all within the panels and the tile, from the
    // caller, who also checked the dot product instructions.
    unsafe {
        let mut sums = [[vdupq_n_u32(0); V]; MR];
        if acc {
            for (r, sums) in sums.iter_mut().enumerate() {
                for (v, sum) in sums.iter_mut().enumerate() {
                    *sum = vreinterpretq_u32_s32(vld1q_s32(c.add(r * ldc + 4 * v)));
                }
            }
        }
        let (mut a, mut b) = (a, b);
        for _ in 0..kc / 4 {
            let bv: [_; V] = std::array::from_fn(|v| vld1q_u8(b.add(16 * v)));
            let av: [_; MR / 4] = std::array::from_fn(|h| vld1q_u8(a.add(16 * h)));
            for (r, sums) in sums.iter_mut().enumerate() {
                for (sum, &bv) in sums.iter_mut().zip(&bv) {
                    *sum = dot_lane(*sum, bv, av[r / 4], r % 4);
                }
            }
            a = a.add(MR * 4);
            b = b.add(NR * 4);
        }
        for (r, sums) in sums.iter().enumerate() {
            for (v, &sum) in sums.iter().enumerate() {
                vst1q_s32(c.add(r * ldc + 4 * v), vreinterpretq_s32_u32(sum));
            }
        }
    }
}

/// `[r0, r1, r2, r3]` transposed, for 4 x 4 cells of 4 bytes.
#[target_feature(enable = "neon")]
#[inline]
fn transpose4x4(r: [uint32x4_t; 4]) -> [uint32x4_t; 4] {
    // Pairs of rows interleaved: `t0 = [r0[0], r1[0], r0[2], r1[2]]`, `t1` the odd lanes
    let t0 = vreinterpretq_u64_u32(vtrn1q_u32(r[0], r[1]));
    let t1 = vreinterpretq_u64_u32(vtrn2q_u32(r[0], r[1]));
    let t2 = vreinterpretq_u64_u32(vtrn1q_u32(r[2], r[3]));
    let t3 = vreinterpretq_u64_u32(vtrn2q_u32(r[2], r[3]));
    // Then their halves: `[r0[0], r1[0], r2[0], r3[0]]` and so on
    [
        vreinterpretq_u32_u64(vtrn1q_u64(t0, t2)),
        vreinterpretq_u32_u64(vtrn1q_u64(t1, t3)),
        vreinterpretq_u32_u64(vtrn2q_u64(t0, t2)),
        vreinterpretq_u32_u64(vtrn2q_u64(t1, t3)),
    ]
}

/// See [`Micro::transpose8x8`]: four 4 x 4 blocks, block `(i, j)` of `src` to `(j, i)` of
/// `dst`.
#[target_feature(enable = "neon")]
#[inline]
unsafe fn transpose8x8(src: *const u32, ss: usize, dst: *mut u32, ds: usize) {
    // SAFETY: reads and writes only the 8 x 8 cells, which the caller guarantees valid
    unsafe {
        for i in (0..8).step_by(4) {
            for j in (0..8).step_by(4) {
                let rows = std::array::from_fn(|k| vld1q_u32(src.add((i + k) * ss + j)));
                for (k, row) in transpose4x4(rows).into_iter().enumerate() {
                    vst1q_u32(dst.add((j + k) * ds + i), row);
                }
            }
        }
    }
}
