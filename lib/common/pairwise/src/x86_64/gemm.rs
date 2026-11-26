//! Register tiles of the blocked product.

use std::arch::x86_64::*;

use crate::blocked::Micro;
use crate::operands::F32_SLICE;

/// 6 x 16 f32 FMA tile: 12 accumulators; per depth step 2 loads, 6 broadcasts, 12 FMAs.
pub(super) struct F32Tile;

impl Micro for F32Tile {
    type E = f32;
    type Acc = f32;
    type Operand = f32;
    const MR: usize = 6;
    const NR: usize = 16;
    const KU: usize = 1;
    const KC: usize = F32_SLICE;
    const MC: usize = 48;
    /// Best at 6 and 12 threads at dim 1536: 512 columns lost 4% to 13%, 32 columns 4% to 22%.
    const NC: usize = 128;

    #[inline(always)]
    unsafe fn tile(
        kc: usize,
        a: *const Self::Operand,
        b: *const Self::Operand,
        c: *mut f32,
        ldc: usize,
        acc: bool,
        (): (),
        _: usize,
    ) {
        unsafe { tile_f32(kc, a, b, c, ldc, acc) }
    }

    #[inline(always)]
    unsafe fn transpose8x8(src: *const u32, ss: usize, dst: *mut u32, ds: usize) {
        unsafe { transpose8x8(src, ss, dst, ds) }
    }

    #[inline(always)]
    unsafe fn transpose_narrow(src: *const u32, ss: usize, w: usize, dst: *mut u32, last: bool) {
        unsafe { transpose_narrow(src, ss, w, dst, last) }
    }
}

/// 4 x 16 u8 tile with i32 accumulators. Values must be at most 127.
pub(super) struct U8Tile;

impl Micro for U8Tile {
    type E = u8;
    type Acc = i32;
    type Operand = u8;
    const MR: usize = 4;
    const NR: usize = 16;
    const KU: usize = 4;
    /// The f32 tile's slice in bytes
    const KC: usize = 1024;
    const MC: usize = 48;
    /// As fast as 512 at dim 1536, with less DRAM traffic at 12 threads.
    const NC: usize = 128;

    #[inline(always)]
    unsafe fn tile(
        kc: usize,
        a: *const Self::Operand,
        b: *const Self::Operand,
        c: *mut i32,
        ldc: usize,
        acc: bool,
        (): (),
        _: usize,
    ) {
        unsafe { tile_u8(kc, a, b, c, ldc, acc) }
    }

    #[inline(always)]
    unsafe fn transpose8x8(src: *const u32, ss: usize, dst: *mut u32, ds: usize) {
        unsafe { transpose8x8(src, ss, dst, ds) }
    }

    #[inline(always)]
    unsafe fn transpose_narrow(src: *const u32, ss: usize, w: usize, dst: *mut u32, last: bool) {
        unsafe { transpose_narrow(src, ss, w, dst, last) }
    }
}

/// See [`Micro::tile`]. The accumulators start from zero and `c` is added at the end, so each
/// slice is a chain of fused multiply-adds in depth order, as [`KernelElem::dot`] restarts it.
///
/// [`KernelElem::dot`]: crate::KernelElem::dot
#[target_feature(enable = "avx2,fma")]
#[inline]
unsafe fn tile_f32(kc: usize, a: *const f32, b: *const f32, c: *mut f32, ldc: usize, acc: bool) {
    const MR: usize = F32Tile::MR;
    const NR: usize = F32Tile::NR;
    unsafe {
        let mut sums = [[_mm256_setzero_ps(); 2]; MR];
        let (mut a, mut b) = (a, b);
        for _ in 0..kc {
            let b0 = _mm256_load_ps(b);
            let b1 = _mm256_load_ps(b.add(8));
            for (r, sums) in sums.iter_mut().enumerate() {
                let ar = _mm256_broadcast_ss(&*a.add(r));
                sums[0] = _mm256_fmadd_ps(ar, b0, sums[0]);
                sums[1] = _mm256_fmadd_ps(ar, b1, sums[1]);
            }
            a = a.add(MR);
            b = b.add(NR);
        }
        for (r, sums) in sums.iter().enumerate() {
            for (h, &sum) in sums.iter().enumerate() {
                let c = c.add(r * ldc + 8 * h);
                _mm256_storeu_ps(
                    c,
                    if acc {
                        _mm256_add_ps(_mm256_loadu_ps(c), sum)
                    } else {
                        sum
                    },
                );
            }
        }
    }
}

/// See [`Micro::tile`]. `vpmaddubsw` multiplies `a` as unsigned and `b` as signed bytes and
/// adds pairs into i16; with values up to 127 a pair is at most 32,258. Two of those are
/// summed as u16 (at most 64,516) before one `vpmaddwd`, so three multiply instructions
/// serve 64 multiply-adds instead of four. The u16 sums are flipped to i16 by subtracting
/// 32,768, which `vpmaddwd` with ones turns into 65,536 per step, added back at the end.
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn tile_u8(kc: usize, a: *const u8, b: *const u8, c: *mut i32, ldc: usize, acc: bool) {
    const MR: usize = U8Tile::MR;
    const NR: usize = U8Tile::NR;
    unsafe {
        let ones = _mm256_set1_epi16(1);
        let flip = _mm256_set1_epi16(i16::MIN);
        let mut sums = [[_mm256_setzero_si256(); 2]; MR];
        let (mut a, mut b) = (a, b);
        let groups = kc / 4;
        for _ in 0..groups / 2 {
            let b00 = _mm256_load_si256(b.cast());
            let b01 = _mm256_load_si256(b.add(32).cast());
            let b10 = _mm256_load_si256(b.add(64).cast());
            let b11 = _mm256_load_si256(b.add(96).cast());
            for (r, sums) in sums.iter_mut().enumerate() {
                let a0 = _mm256_set1_epi32(a.add(r * 4).cast::<i32>().read_unaligned());
                let a1 = _mm256_set1_epi32(a.add(MR * 4 + r * 4).cast::<i32>().read_unaligned());
                let y0 =
                    _mm256_add_epi16(_mm256_maddubs_epi16(a0, b00), _mm256_maddubs_epi16(a1, b10));
                let y1 =
                    _mm256_add_epi16(_mm256_maddubs_epi16(a0, b01), _mm256_maddubs_epi16(a1, b11));
                sums[0] =
                    _mm256_add_epi32(sums[0], _mm256_madd_epi16(_mm256_xor_si256(y0, flip), ones));
                sums[1] =
                    _mm256_add_epi32(sums[1], _mm256_madd_epi16(_mm256_xor_si256(y1, flip), ones));
            }
            a = a.add(2 * MR * 4);
            b = b.add(2 * NR * 4);
        }
        let fix = _mm256_set1_epi32(((groups / 2) as i32).wrapping_mul(65536));
        if groups % 2 == 1 {
            let b0 = _mm256_load_si256(b.cast());
            let b1 = _mm256_load_si256(b.add(32).cast());
            for (r, sums) in sums.iter_mut().enumerate() {
                let a0 = _mm256_set1_epi32(a.add(r * 4).cast::<i32>().read_unaligned());
                sums[0] = _mm256_add_epi32(
                    sums[0],
                    _mm256_madd_epi16(_mm256_maddubs_epi16(a0, b0), ones),
                );
                sums[1] = _mm256_add_epi32(
                    sums[1],
                    _mm256_madd_epi16(_mm256_maddubs_epi16(a0, b1), ones),
                );
            }
        }
        for (r, sums) in sums.iter().enumerate() {
            let cr = c.add(r * ldc);
            for (h, &x) in sums.iter().enumerate() {
                let mut x = _mm256_add_epi32(x, fix);
                if acc {
                    x = _mm256_add_epi32(x, _mm256_loadu_si256(cr.add(8 * h).cast()));
                }
                _mm256_storeu_si256(cr.add(8 * h).cast(), x);
            }
        }
    }
}

/// The 8 x 8 transpose of 32-bit cells in registers, rows `v`.
#[target_feature(enable = "avx2")]
#[inline]
fn transpose_registers(v: [__m256i; 8]) -> [__m256i; 8] {
    let t0 = _mm256_unpacklo_epi32(v[0], v[1]);
    let t1 = _mm256_unpackhi_epi32(v[0], v[1]);
    let t2 = _mm256_unpacklo_epi32(v[2], v[3]);
    let t3 = _mm256_unpackhi_epi32(v[2], v[3]);
    let t4 = _mm256_unpacklo_epi32(v[4], v[5]);
    let t5 = _mm256_unpackhi_epi32(v[4], v[5]);
    let t6 = _mm256_unpacklo_epi32(v[6], v[7]);
    let t7 = _mm256_unpackhi_epi32(v[6], v[7]);
    let u0 = _mm256_unpacklo_epi64(t0, t2);
    let u1 = _mm256_unpackhi_epi64(t0, t2);
    let u2 = _mm256_unpacklo_epi64(t1, t3);
    let u3 = _mm256_unpackhi_epi64(t1, t3);
    let u4 = _mm256_unpacklo_epi64(t4, t6);
    let u5 = _mm256_unpackhi_epi64(t4, t6);
    let u6 = _mm256_unpacklo_epi64(t5, t7);
    let u7 = _mm256_unpackhi_epi64(t5, t7);
    [
        _mm256_permute2x128_si256::<0x20>(u0, u4),
        _mm256_permute2x128_si256::<0x20>(u1, u5),
        _mm256_permute2x128_si256::<0x20>(u2, u6),
        _mm256_permute2x128_si256::<0x20>(u3, u7),
        _mm256_permute2x128_si256::<0x31>(u0, u4),
        _mm256_permute2x128_si256::<0x31>(u1, u5),
        _mm256_permute2x128_si256::<0x31>(u2, u6),
        _mm256_permute2x128_si256::<0x31>(u3, u7),
    ]
}

/// See [`Micro::transpose8x8`].
#[target_feature(enable = "avx2")]
#[inline]
pub(super) unsafe fn transpose8x8(src: *const u32, ss: usize, dst: *mut u32, ds: usize) {
    unsafe {
        let mut v = [_mm256_setzero_si256(); 8];
        for (i, x) in v.iter_mut().enumerate() {
            *x = _mm256_loadu_si256(src.add(i * ss).cast());
        }
        for (j, x) in transpose_registers(v).into_iter().enumerate() {
            _mm256_storeu_si256(dst.add(j * ds).cast(), x);
        }
    }
}

/// See [`Micro::transpose_narrow`]: rows as overlapping 8-wide stores in increasing order,
/// each overwriting the previous one's overflow, the last one exact if `last`.
#[target_feature(enable = "avx2")]
#[inline]
pub(super) unsafe fn transpose_narrow(
    src: *const u32,
    ss: usize,
    w: usize,
    dst: *mut u32,
    last: bool,
) {
    unsafe {
        let mut v = [_mm256_setzero_si256(); 8];
        for (i, x) in v.iter_mut().enumerate().take(w) {
            *x = _mm256_loadu_si256(src.add(i * ss).cast());
        }
        let rows = transpose_registers(v);
        for (j, &x) in rows.iter().enumerate().take(7) {
            _mm256_storeu_si256(dst.add(j * w).cast(), x);
        }
        if last {
            let mut tmp = [0u32; 8];
            _mm256_storeu_si256(tmp.as_mut_ptr().cast(), rows[7]);
            std::ptr::copy_nonoverlapping(tmp.as_ptr(), dst.add(7 * w), w);
        } else {
            _mm256_storeu_si256(dst.add(7 * w).cast(), rows[7]);
        }
    }
}
