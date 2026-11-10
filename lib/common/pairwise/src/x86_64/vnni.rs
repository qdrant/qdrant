//! AVX-512 VNNI tile for u8 and the code types: 8 rows by 32 columns of bytes, multiplied with
//! `vpdpbusd`, which adds four byte products into each i32 lane, one step of four depth values.
//! Exact.
//!
//! `vpdpbusd` multiplies unsigned bytes with signed ones. u8 values are at most 127, so they're
//! both. Codes are signed, so the columns hold `value + 128` as unsigned bytes and the rows the
//! values, which adds `128 · Σ row values` to every inner product. The rows are unpacked with
//! that sum, and the tile starts its accumulators from `−128 ·` it, per depth slice. With
//! wrapping i32 sums, the result is exact whenever the true inner product fits an i32, which
//! [`MAX_CODES`](super::MAX_CODES) guarantees.
//!
//! The 8-bit grid stores `value + 128` already, so its columns are read packed and only the rows
//! are unpacked, flipping the top bit. Codebook codes are looked up by `vpshufb`, the rows in
//! signed values, the columns in unsigned ones. Both sides are unpacked once ([`Unpacking::Whole`]):
//! the tile does four times the work per instruction of the AVX2 code tile, so unpacking again
//! for every row block, as that one does, would cost a noticeable part of a call.

use std::arch::x86_64::*;

use crate::blocked::{Micro, Unpacking};
use crate::codes::{Codebook, Codes2, Codes4, Codes8};
use crate::operands::KernelElem;

const MR: usize = 8;
const NR: usize = 32;
/// Depth values per step.
const STEP: usize = 4;

/// An element type of the VNNI tile.
pub(crate) trait Vnni: KernelElem + crate::scratch::Plain {
    /// Values per element.
    const VALUES: usize;
    /// Elements per cell of the packed layout.
    const KU: usize;
    /// Values per element that unpacking writes, or 0 if neither side is unpacked.
    const UNPACK: usize;
    const UNPACK_COLS: Unpacking;
    /// What the lookups need, in registers.
    type Tables: Copy;

    /// # Safety
    ///
    /// The CPU must support AVX2.
    unsafe fn tables(param: Self::Param<'_>) -> Self::Tables;

    /// Writes the signed values of a panel of `MR` rows and `kc` elements to `dst`,
    /// `[step][row][STEP]`, with the values missing from the last step 0.
    ///
    /// # Safety
    ///
    /// `panel` must be valid for the packed panel, `dst` for `MR · STEP` bytes per step. The CPU
    /// must support AVX2.
    unsafe fn rows(t: Self::Tables, kc: usize, panel: *const u8, dst: *mut u8);

    /// Writes the values plus 128 of a panel of `NR` columns, as unsigned bytes,
    /// `[step][column][STEP]`. Values missing from the last step may be anything.
    ///
    /// # Safety
    ///
    /// As [`Self::rows`], for `NR` lanes.
    unsafe fn cols(t: Self::Tables, kc: usize, panel: *const u8, dst: *mut u8);
}

/// Steps of `kc` elements.
#[inline(always)]
const fn steps<U: Vnni>(kc: usize) -> usize {
    (kc * U::VALUES).div_ceil(STEP)
}

/// Scalar quantization codes, at most 127: read packed by both sides, without a correction.
impl Vnni for u8 {
    const VALUES: usize = 1;
    const KU: usize = STEP;
    const UNPACK: usize = 0;
    const UNPACK_COLS: Unpacking = Unpacking::No;
    type Tables = ();

    unsafe fn tables((): ()) {}

    unsafe fn rows((): (), _: usize, _: *const u8, _: *mut u8) {
        unreachable!("u8 isn't unpacked");
    }

    unsafe fn cols((): (), _: usize, _: *const u8, _: *mut u8) {
        unreachable!("u8 isn't unpacked");
    }
}

/// `value + 128` stored, which the columns read as it is.
impl Vnni for Codes8 {
    const VALUES: usize = 1;
    const KU: usize = STEP;
    const UNPACK: usize = 1;
    const UNPACK_COLS: Unpacking = Unpacking::No;
    type Tables = ();

    unsafe fn tables((): ()) {}

    /// The packed layout is `[step][row][STEP]` already, padded with the value 0.
    #[inline(always)]
    unsafe fn rows((): (), kc: usize, panel: *const u8, dst: *mut u8) {
        unsafe {
            let flip = _mm256_set1_epi8(i8::MIN);
            for s in 0..kc / STEP {
                let x = _mm256_loadu_si256(panel.add(MR * STEP * s).cast());
                _mm256_storeu_si256(dst.add(MR * STEP * s).cast(), _mm256_xor_si256(x, flip));
            }
        }
    }

    unsafe fn cols((): (), _: usize, _: *const u8, _: *mut u8) {
        unreachable!("the 8-bit grid's columns are read packed");
    }
}

/// The signed values for the rows and the values plus 128 for the columns.
#[derive(Clone, Copy)]
pub(crate) struct Tables<T> {
    rows: T,
    cols: T,
}

/// `values ^ 0x80`: as unsigned bytes, the values plus 128.
#[target_feature(enable = "avx2")]
#[inline]
fn unsigned(values: __m128i) -> __m128i {
    _mm_xor_si128(values, _mm_set1_epi8(i8::MIN))
}

/// Low and high nibbles of the bytes of `x`.
#[target_feature(enable = "avx2")]
#[inline]
fn nibbles(x: __m128i) -> (__m128i, __m128i) {
    let mask = _mm_set1_epi8(0x0F);
    (
        _mm_and_si128(x, mask),
        _mm_and_si128(_mm_srli_epi16(x, 4), mask),
    )
}

/// The steps of 8 lanes of 4-bit codes, from the two elements of each: `x` holds the first
/// element of lanes 0..8 in bytes 0..8, the second in bytes 8..16. Lanes 0..4, then 4..8,
/// each its codes in order.
#[target_feature(enable = "avx2")]
#[inline]
fn quads4(x: __m128i, values: __m128i) -> [__m128i; 2] {
    let (lo, hi) = nibbles(x);
    let (lo, hi) = (_mm_shuffle_epi8(values, lo), _mm_shuffle_epi8(values, hi));
    // The codes of the first elements, then of the second ones, two per lane
    let first = _mm_unpacklo_epi8(lo, hi);
    let second = _mm_unpackhi_epi8(lo, hi);
    [
        _mm_unpacklo_epi16(first, second),
        _mm_unpackhi_epi16(first, second),
    ]
}

impl Vnni for Codes4 {
    const VALUES: usize = 2;
    const KU: usize = 1;
    const UNPACK: usize = 2;
    const UNPACK_COLS: Unpacking = Unpacking::Whole;
    type Tables = Tables<__m128i>;

    unsafe fn tables(codebook: Codebook) -> Self::Tables {
        unsafe {
            let values = _mm_loadu_si128(codebook.values.as_ptr().cast());
            Tables {
                rows: values,
                cols: unsigned(values),
            }
        }
    }

    /// Two elements per step, `[element][row]` packed.
    #[inline(always)]
    unsafe fn rows(t: Self::Tables, kc: usize, panel: *const u8, dst: *mut u8) {
        unsafe {
            for s in 0..kc / 2 {
                let [q0, q1] = quads4(_mm_loadu_si128(panel.add(2 * MR * s).cast()), t.rows);
                _mm_storeu_si128(dst.add(MR * STEP * s).cast(), q0);
                _mm_storeu_si128(dst.add(MR * STEP * s + 16).cast(), q1);
            }
            if kc % 2 == 1 {
                let s = kc / 2;
                let [q0, q1] = quads4(_mm_loadl_epi64(panel.add(2 * MR * s).cast()), t.rows);
                // The second element's two values of each lane are missing
                let first = _mm_set1_epi32(0xFFFF);
                _mm_storeu_si128(dst.add(MR * STEP * s).cast(), _mm_and_si128(q0, first));
                _mm_storeu_si128(dst.add(MR * STEP * s + 16).cast(), _mm_and_si128(q1, first));
            }
        }
    }

    #[inline(always)]
    unsafe fn cols(t: Self::Tables, kc: usize, panel: *const u8, dst: *mut u8) {
        unsafe {
            for s in 0..kc.div_ceil(2) {
                let first = panel.add(2 * NR * s);
                let both = 2 * s + 1 < kc;
                for l in (0..NR).step_by(8) {
                    let a = _mm_loadl_epi64(first.add(l).cast());
                    let b = if both {
                        _mm_loadl_epi64(first.add(NR + l).cast())
                    } else {
                        _mm_setzero_si128()
                    };
                    let [q0, q1] = quads4(_mm_unpacklo_epi64(a, b), t.cols);
                    let dst = dst.add(NR * STEP * s + STEP * l);
                    _mm_storeu_si128(dst.cast(), q0);
                    _mm_storeu_si128(dst.add(16).cast(), q1);
                }
            }
        }
    }
}

/// The steps of 8 lanes of 2-bit codes, one byte each in the low half of `x`, with `first`
/// and `second` as [`Codebook`] has them. Lanes 0..4, then 4..8, each its codes in order.
#[target_feature(enable = "avx2")]
#[inline]
fn quads2(x: __m128i, t: Tables2) -> [__m128i; 2] {
    let (lo, hi) = nibbles(x);
    // Per lane its low nibble, holding codes 0 and 1, then its high one, codes 2 and 3
    let n = _mm_unpacklo_epi8(lo, hi);
    let first = _mm_shuffle_epi8(t.first, n);
    let second = _mm_shuffle_epi8(t.second, n);
    [
        _mm_unpacklo_epi8(first, second),
        _mm_unpackhi_epi8(first, second),
    ]
}

/// The value of the first and the second code of each nibble.
#[derive(Clone, Copy)]
pub(crate) struct Tables2 {
    first: __m128i,
    second: __m128i,
}

impl Vnni for Codes2 {
    const VALUES: usize = 4;
    const KU: usize = 1;
    const UNPACK: usize = 4;
    const UNPACK_COLS: Unpacking = Unpacking::Whole;
    type Tables = Tables<Tables2>;

    unsafe fn tables(codebook: Codebook) -> Self::Tables {
        unsafe {
            let first = _mm_loadu_si128(codebook.first.as_ptr().cast());
            let second = _mm_loadu_si128(codebook.second.as_ptr().cast());
            Tables {
                rows: Tables2 { first, second },
                cols: Tables2 {
                    first: unsigned(first),
                    second: unsigned(second),
                },
            }
        }
    }

    /// One element per step.
    #[inline(always)]
    unsafe fn rows(t: Self::Tables, kc: usize, panel: *const u8, dst: *mut u8) {
        unsafe {
            for s in 0..kc {
                let [q0, q1] = quads2(_mm_loadl_epi64(panel.add(MR * s).cast()), t.rows);
                _mm_storeu_si128(dst.add(MR * STEP * s).cast(), q0);
                _mm_storeu_si128(dst.add(MR * STEP * s + 16).cast(), q1);
            }
        }
    }

    #[inline(always)]
    unsafe fn cols(t: Self::Tables, kc: usize, panel: *const u8, dst: *mut u8) {
        unsafe {
            for s in 0..kc {
                for l in (0..NR).step_by(8) {
                    let x = _mm_loadl_epi64(panel.add(NR * s + l).cast());
                    let [q0, q1] = quads2(x, t.cols);
                    let dst = dst.add(NR * STEP * s + STEP * l);
                    _mm_storeu_si128(dst.cast(), q0);
                    _mm_storeu_si128(dst.add(16).cast(), q1);
                }
            }
        }
    }
}

/// [`tile`] for one element type.
pub(super) struct VnniTile<U>(std::marker::PhantomData<U>);

impl<U: Vnni> Micro for VnniTile<U> {
    type E = U;
    type Acc = i32;
    type Operand = u8;
    const UNPACK: usize = U::UNPACK;
    const UNPACK_ROWS: Unpacking = Unpacking::Whole;
    const UNPACK_COLS: Unpacking = U::UNPACK_COLS;
    /// `−128 ·` the sum of each row's values, see the module
    const ROW_EXTRA: usize = MR * size_of::<i32>();
    const MR: usize = MR;
    const NR: usize = NR;
    const KU: usize = U::KU;
    /// 512 values: a column panel of 16 KB, which stays in L1 while a row block reads it.
    /// Unmeasured.
    const KC: usize = 512 / U::VALUES;
    const MC: usize = 48;
    /// As for the other tiles, unmeasured with this one
    const NC: usize = 128;

    #[inline(always)]
    fn unpacked_lane(kc: usize) -> usize {
        steps::<U>(kc) * STEP
    }

    #[inline(always)]
    unsafe fn unpack(kc: usize, panel: *const U, rows: bool, dst: *mut u8, param: U::Param<'_>) {
        // SAFETY: forwarded from the caller
        unsafe {
            let t = U::tables(param);
            if rows {
                U::rows(t, kc, panel.cast(), dst);
                row_corrections(steps::<U>(kc), dst);
            } else {
                U::cols(t, kc, panel.cast(), dst);
            }
        }
    }

    #[inline(always)]
    unsafe fn tile(
        kc: usize,
        a: *const u8,
        b: *const u8,
        c: *mut i32,
        ldc: usize,
        acc: bool,
        _: U::Param<'_>,
        _: usize,
    ) {
        // SAFETY: forwarded from the caller
        unsafe {
            if U::UNPACK > 0 {
                tile::<true>(steps::<U>(kc), a, b, c, ldc, acc);
            } else {
                tile::<false>(steps::<U>(kc), a, b, c, ldc, acc);
            }
        }
    }

    #[inline(always)]
    unsafe fn transpose8x8(src: *const u32, ss: usize, dst: *mut u32, ds: usize) {
        unsafe { super::gemm::transpose8x8(src, ss, dst, ds) }
    }

    #[inline(always)]
    unsafe fn transpose_narrow(src: *const u32, ss: usize, w: usize, dst: *mut u32, last: bool) {
        unsafe { super::gemm::transpose_narrow(src, ss, w, dst, last) }
    }
}

/// Writes `−128 ·` the sum of each row's values after an unpacked row panel of `steps`.
///
/// # Safety
///
/// `panel` must be valid for the panel and the `MR` sums after it. The CPU must support AVX2.
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn row_corrections(steps: usize, panel: *mut u8) {
    unsafe {
        let ones8 = _mm256_set1_epi8(1);
        let ones16 = _mm256_set1_epi16(1);
        let mut sums = _mm256_setzero_si256();
        for s in 0..steps {
            let x = _mm256_loadu_si256(panel.add(MR * STEP * s).cast());
            // Pairs of values fit an i16
            let pairs = _mm256_maddubs_epi16(ones8, x);
            sums = _mm256_add_epi32(sums, _mm256_madd_epi16(pairs, ones16));
        }
        let corrections = _mm256_sub_epi32(_mm256_setzero_si256(), _mm256_slli_epi32(sums, 7));
        _mm256_storeu_si256(panel.add(MR * STEP * steps).cast(), corrections);
    }
}

/// See [`Micro::tile`]: 8 rows by 32 columns over `steps` steps, the rows signed and broadcast,
/// the columns unsigned, with i32 accumulators. If `CORRECT`, they start from the corrections
/// after the row panel.
#[target_feature(enable = "avx512f,avx512vnni")]
#[inline]
unsafe fn tile<const CORRECT: bool>(
    steps: usize,
    a: *const u8,
    b: *const u8,
    c: *mut i32,
    ldc: usize,
    acc: bool,
) {
    // SAFETY: per step `MR` rows and `NR` columns of `STEP` bytes, within the `steps`, and the
    // corrections after the row panel, from the caller. `MR` rows of `NR` values of `c`.
    unsafe {
        let mut sums = [[_mm512_setzero_si512(); 2]; MR];
        if CORRECT {
            let corrections = a.add(MR * STEP * steps).cast::<i32>();
            for (r, sums) in sums.iter_mut().enumerate() {
                *sums = [_mm512_set1_epi32(corrections.add(r).read_unaligned()); 2];
            }
        }
        for s in 0..steps {
            let b0 = _mm512_loadu_si512(b.add(NR * STEP * s).cast());
            let b1 = _mm512_loadu_si512(b.add(NR * STEP * s + 64).cast());
            for (r, sums) in sums.iter_mut().enumerate() {
                let row = a
                    .add(MR * STEP * s + STEP * r)
                    .cast::<i32>()
                    .read_unaligned();
                let row = _mm512_set1_epi32(row);
                sums[0] = _mm512_dpbusd_epi32(sums[0], b0, row);
                sums[1] = _mm512_dpbusd_epi32(sums[1], b1, row);
            }
        }
        for (r, sums) in sums.iter().enumerate() {
            for (h, &sum) in sums.iter().enumerate() {
                let c = c.add(r * ldc + 16 * h);
                let sum = if acc {
                    _mm512_add_epi32(_mm512_loadu_si512(c.cast()), sum)
                } else {
                    sum
                };
                _mm512_storeu_si512(c.cast(), sum);
            }
        }
    }
}

/// [`VnniTile`] with its register tile in scalar code, so CPUs without AVX-512 test everything
/// else: the layouts, unpacking, and corrections.
#[cfg(test)]
pub(super) struct EmulatedTile<U>(std::marker::PhantomData<U>);

#[cfg(test)]
impl<U: Vnni> Micro for EmulatedTile<U> {
    type E = U;
    type Acc = i32;
    type Operand = u8;
    const UNPACK: usize = VnniTile::<U>::UNPACK;
    const UNPACK_ROWS: Unpacking = VnniTile::<U>::UNPACK_ROWS;
    const UNPACK_COLS: Unpacking = VnniTile::<U>::UNPACK_COLS;
    const ROW_EXTRA: usize = VnniTile::<U>::ROW_EXTRA;
    const MR: usize = MR;
    const NR: usize = NR;
    const KU: usize = VnniTile::<U>::KU;
    const KC: usize = VnniTile::<U>::KC;
    const MC: usize = VnniTile::<U>::MC;
    const NC: usize = VnniTile::<U>::NC;

    fn unpacked_lane(kc: usize) -> usize {
        VnniTile::<U>::unpacked_lane(kc)
    }

    unsafe fn unpack(kc: usize, panel: *const U, rows: bool, dst: *mut u8, param: U::Param<'_>) {
        unsafe { VnniTile::<U>::unpack(kc, panel, rows, dst, param) }
    }

    /// `vpdpbusd` one lane at a time
    unsafe fn tile(
        kc: usize,
        a: *const u8,
        b: *const u8,
        c: *mut i32,
        ldc: usize,
        acc: bool,
        _: U::Param<'_>,
        _: usize,
    ) {
        let steps = steps::<U>(kc);
        unsafe {
            for r in 0..MR {
                let start = if U::UNPACK > 0 {
                    a.add(MR * STEP * steps)
                        .cast::<i32>()
                        .add(r)
                        .read_unaligned()
                } else {
                    0
                };
                for j in 0..NR {
                    let mut sum = start;
                    for s in 0..steps {
                        for k in 0..STEP {
                            let row = i32::from(*a.add((s * MR + r) * STEP + k) as i8);
                            let col = i32::from(*b.add((s * NR + j) * STEP + k));
                            sum = sum.wrapping_add(row * col);
                        }
                    }
                    let c = c.add(r * ldc + j);
                    *c = if acc { (*c).wrapping_add(sum) } else { sum };
                }
            }
        }
    }

    unsafe fn transpose8x8(src: *const u32, ss: usize, dst: *mut u32, ds: usize) {
        unsafe { VnniTile::<U>::transpose8x8(src, ss, dst, ds) }
    }

    unsafe fn transpose_narrow(src: *const u32, ss: usize, w: usize, dst: *mut u32, last: bool) {
        unsafe { VnniTile::<U>::transpose_narrow(src, ss, w, dst, last) }
    }
}
