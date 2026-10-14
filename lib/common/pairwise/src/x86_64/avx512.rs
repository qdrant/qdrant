//! AVX-512F tile for f32: the AVX2 tile with 512-bit lanes.

use std::arch::x86_64::*;

use crate::blocked::Micro;

/// 6 x 32 f32 FMA tile: 12 accumulators; per depth step 2 loads, 6 broadcasts, 12 FMAs, as
/// the AVX2 tile with twice the columns. Each score is the same chain of fused multiply-adds
/// in depth order, so it's bitwise equal to the AVX2 tile and the fallback.
pub(super) struct F32Tile;

impl Micro for F32Tile {
    type E = f32;
    type Acc = f32;
    const MR: usize = 6;
    const NR: usize = 32;
    const KU: usize = 1;
    const KC: usize = 256;
    const MC: usize = 48;
    /// As for the AVX2 tile, unmeasured with this one
    const NC: usize = 128;

    #[inline(always)]
    unsafe fn tile(kc: usize, a: *const f32, b: *const f32, c: *mut f32, ldc: usize, acc: bool) {
        unsafe { tile_f32(kc, a, b, c, ldc, acc) }
    }

    // Packing as for the AVX2 tile: every CPU with AVX-512F here also has AVX2
    #[inline(always)]
    unsafe fn transpose8x8(src: *const u32, ss: usize, dst: *mut u32, ds: usize) {
        unsafe { super::gemm::transpose8x8(src, ss, dst, ds) }
    }

    #[inline(always)]
    unsafe fn transpose_narrow(src: *const u32, ss: usize, w: usize, dst: *mut u32, last: bool) {
        unsafe { super::gemm::transpose_narrow(src, ss, w, dst, last) }
    }
}

/// See [`Micro::tile`]. The accumulators start from `c`, as in the AVX2 tile.
#[target_feature(enable = "avx512f")]
#[inline]
unsafe fn tile_f32(kc: usize, a: *const f32, b: *const f32, c: *mut f32, ldc: usize, acc: bool) {
    const MR: usize = F32Tile::MR;
    const NR: usize = F32Tile::NR;
    unsafe {
        let mut sums = [[_mm512_setzero_ps(); 2]; MR];
        if acc {
            for (r, sums) in sums.iter_mut().enumerate() {
                sums[0] = _mm512_loadu_ps(c.add(r * ldc));
                sums[1] = _mm512_loadu_ps(c.add(r * ldc + 16));
            }
        }
        let (mut a, mut b) = (a, b);
        for _ in 0..kc {
            let b0 = _mm512_load_ps(b);
            let b1 = _mm512_load_ps(b.add(16));
            for (r, sums) in sums.iter_mut().enumerate() {
                let ar = _mm512_set1_ps(*a.add(r));
                sums[0] = _mm512_fmadd_ps(ar, b0, sums[0]);
                sums[1] = _mm512_fmadd_ps(ar, b1, sums[1]);
            }
            a = a.add(MR);
            b = b.add(NR);
        }
        for (r, sums) in sums.iter().enumerate() {
            _mm512_storeu_ps(c.add(r * ldc), sums[0]);
            _mm512_storeu_ps(c.add(r * ldc + 16), sums[1]);
        }
    }
}
