//! The register tile for the code types: 4 rows by 16 columns of i16 values, multiplied with
//! `vpmaddwd`, which adds two depth codes into each i32 lane: one pair step. Exact.
//!
//! The tile reads the codes unpacked (see [`Micro::unpack`]): a row block's panels once per
//! depth slice and a column panel once per row block, as pairs `[step][lane][2]`, so per step
//! it only loads two vectors of columns and broadcasts each row's pair from memory. Unpacking
//! maps codebook codes to their values by `vpshufb` lookups and subtracts 128 from the 8-bit
//! grid. A packed column panel holds one byte per column and element (two for [`Codes8`]).
//!
//! Values are at most 128 in magnitude, so a product at most 2¹⁴: a lane gains at most 2¹⁵
//! per step, and i32 accumulators hold 2¹⁵ steps (2¹⁶ codes, TurboQuant's largest dimension)
//! with sums of at most 2³⁰.

use std::arch::x86_64::*;

use crate::blocked::Micro;
use crate::codes::{Codebook, Codes2, Codes4, Codes8};
use crate::operands::KernelElem;

/// Codes the i32 accumulators hold without overflow, see the module.
pub(crate) const MAX_CODES: usize = 1 << 16;

/// How a code type unpacks into i16 pairs: one pair step is two depth codes.
pub(crate) trait Unpack: KernelElem + crate::scratch::Plain {
    /// Elements per group of two steps.
    const G: usize;
    /// Codes per element.
    const CODES: usize;
    /// Elements per cell of the packed layout.
    const KU: usize;
    /// What the lookups need, in registers.
    type Tables: Copy;

    /// # Safety
    ///
    /// The CPU must support AVX2.
    unsafe fn tables(param: Self::Param<'_>) -> Self::Tables;

    /// The pairs of 4 rows for the two steps of a group at `a`, step `s` in 128-bit lane `s`,
    /// or only of the first step unless `both`.
    ///
    /// # Safety
    ///
    /// `a` must be valid for the group's `4 · G` bytes, or half of them unless `both`. The CPU
    /// must support AVX2.
    unsafe fn rows(t: Self::Tables, a: *const u8, both: bool) -> __m256i;

    /// The pairs of 16 columns for the two steps of a group at `b`, `[step][columns 0..8,
    /// 8..16]`, or only of the first step unless `both`.
    ///
    /// # Safety
    ///
    /// As [`Self::rows`], for `16 · G` bytes.
    unsafe fn cols(t: Self::Tables, b: *const u8, both: bool) -> [[__m256i; 2]; 2];
}

/// Loads `N` bytes (2, 4, 8, or 16) into the low bytes of a vector.
///
/// # Safety
///
/// `p` must be valid for `N` bytes. The CPU must support AVX2.
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn load<const N: usize>(p: *const u8) -> __m128i {
    unsafe {
        match N {
            2 => _mm_cvtsi32_si128(i32::from(p.cast::<u16>().read_unaligned())),
            4 => _mm_cvtsi32_si128(p.cast::<i32>().read_unaligned()),
            8 => _mm_loadl_epi64(p.cast()),
            _ => _mm_loadu_si128(p.cast()),
        }
    }
}

/// The values of 16 nibble pairs: byte `j`'s low and high nibble is pair `j`, bytes 0..8 in
/// the first vector, 8..16 in the second.
#[target_feature(enable = "avx2")]
#[inline]
fn nibble_pairs(x: __m128i, values: __m128i) -> [__m256i; 2] {
    let mask = _mm_set1_epi8(0x0F);
    let lo = _mm_shuffle_epi8(values, _mm_and_si128(x, mask));
    let hi = _mm_shuffle_epi8(values, _mm_and_si128(_mm_srli_epi16(x, 4), mask));
    [
        _mm256_cvtepi8_epi16(_mm_unpacklo_epi8(lo, hi)),
        _mm256_cvtepi8_epi16(_mm_unpackhi_epi8(lo, hi)),
    ]
}

/// `[values, values]` of 2-bit pairs: for a nibble `n` of two codes, the first's and the
/// second's value.
#[derive(Clone, Copy)]
pub(crate) struct PairTables {
    first: __m128i,
    second: __m128i,
}

/// The values of 16 two-code nibbles at once: nibble `j` (of its byte's two) is pair `j`, as
/// [`nibble_pairs`] splits them.
#[target_feature(enable = "avx2")]
#[inline]
fn crumb_pairs(n: __m128i, t: PairTables) -> [__m256i; 2] {
    let first = _mm_shuffle_epi8(t.first, n);
    let second = _mm_shuffle_epi8(t.second, n);
    [
        _mm256_cvtepi8_epi16(_mm_unpacklo_epi8(first, second)),
        _mm256_cvtepi8_epi16(_mm_unpackhi_epi8(first, second)),
    ]
}

/// The 16 bytes' codes minus 128 as i16, in order.
#[target_feature(enable = "avx2")]
#[inline]
fn grid_values(x: __m128i) -> __m256i {
    _mm256_sub_epi16(_mm256_cvtepu8_epi16(x), _mm256_set1_epi16(128))
}

impl Unpack for Codes4 {
    const G: usize = 2;
    const CODES: usize = 2;
    const KU: usize = 1;
    type Tables = __m128i;

    unsafe fn tables(codebook: Codebook) -> __m128i {
        unsafe { _mm_loadu_si128(codebook.values.as_ptr().cast()) }
    }

    /// One byte per row and step: `[step][row]`, so the low pairs come out in that order.
    #[inline(always)]
    unsafe fn rows(values: __m128i, a: *const u8, both: bool) -> __m256i {
        unsafe {
            let x = if both { load::<8>(a) } else { load::<4>(a) };
            nibble_pairs(x, values)[0]
        }
    }

    #[inline(always)]
    unsafe fn cols(values: __m128i, b: *const u8, both: bool) -> [[__m256i; 2]; 2] {
        unsafe {
            let first = nibble_pairs(load::<16>(b), values);
            let second = if both {
                nibble_pairs(load::<16>(b.add(16)), values)
            } else {
                first
            };
            [first, second]
        }
    }
}

impl Unpack for Codes2 {
    const G: usize = 1;
    const CODES: usize = 4;
    const KU: usize = 1;
    type Tables = PairTables;

    unsafe fn tables(codebook: Codebook) -> PairTables {
        unsafe {
            PairTables {
                first: _mm_loadu_si128(codebook.first.as_ptr().cast()),
                second: _mm_loadu_si128(codebook.second.as_ptr().cast()),
            }
        }
    }

    /// One byte per row with both steps: its low nibble is step 0, its high nibble step 1.
    /// The nibbles are put in `[step][row]` order before the lookup.
    #[inline(always)]
    unsafe fn rows(t: PairTables, a: *const u8, _both: bool) -> __m256i {
        unsafe {
            let mask = _mm_set1_epi8(0x0F);
            let x = load::<4>(a);
            let lo = _mm_and_si128(x, mask);
            let hi = _mm_and_si128(_mm_srli_epi16(x, 4), mask);
            crumb_pairs(_mm_unpacklo_epi32(lo, hi), t)[0]
        }
    }

    #[inline(always)]
    unsafe fn cols(t: PairTables, b: *const u8, _both: bool) -> [[__m256i; 2]; 2] {
        unsafe {
            let x = load::<16>(b);
            let mask = _mm_set1_epi8(0x0F);
            [
                crumb_pairs(_mm_and_si128(x, mask), t),
                crumb_pairs(_mm_and_si128(_mm_srli_epi16(x, 4), mask), t),
            ]
        }
    }
}

impl Unpack for Codes8 {
    const G: usize = 4;
    const CODES: usize = 1;
    const KU: usize = 2;
    type Tables = ();

    unsafe fn tables((): ()) {}

    /// Two bytes per row and step, `[step][row]`.
    #[inline(always)]
    unsafe fn rows((): (), a: *const u8, both: bool) -> __m256i {
        unsafe { grid_values(if both { load::<16>(a) } else { load::<8>(a) }) }
    }

    #[inline(always)]
    unsafe fn cols((): (), b: *const u8, both: bool) -> [[__m256i; 2]; 2] {
        unsafe {
            let first = [
                grid_values(load::<16>(b)),
                grid_values(load::<16>(b.add(16))),
            ];
            let second = if both {
                [
                    grid_values(load::<16>(b.add(32))),
                    grid_values(load::<16>(b.add(48))),
                ]
            } else {
                first
            };
            [first, second]
        }
    }
}

/// [`tile`] for one code type.
pub(super) struct CodeTile<U>(std::marker::PhantomData<U>);

/// Depth slices of about 1024 codes: panels of 2 to 4 KB per 4 rows.
const fn slice<U: Unpack>() -> usize {
    1024 / U::CODES
}

impl<U: Unpack> Micro for CodeTile<U> {
    type E = U;
    type Acc = i32;
    type Operand = i16;
    const UNPACK: usize = U::CODES;
    const MR: usize = 4;
    const NR: usize = 16;
    const KU: usize = U::KU;
    const KC: usize = slice::<U>();
    const MC: usize = 48;
    const NC: usize = 128;

    #[inline(always)]
    unsafe fn unpack(kc: usize, panel: *const U, rows: bool, dst: *mut i16, param: U::Param<'_>) {
        // SAFETY: forwarded from the caller
        unsafe { unpack::<U, 16>(kc, panel, rows, dst, param) }
    }

    #[inline(always)]
    unsafe fn tile(
        kc: usize,
        a: *const i16,
        b: *const i16,
        c: *mut i32,
        ldc: usize,
        acc: bool,
        _: U::Param<'_>,
        _: usize,
    ) {
        // SAFETY: forwarded from the caller
        unsafe { tile(kc * U::CODES / 2, a, b, c, ldc, acc) }
    }
}

/// Writes the pairs of a panel of 4 rows or `NR` (8 or 16) columns, `[step][lane][2]`, see
/// [`Micro::unpack`].
///
/// # Safety
///
/// As [`Micro::unpack`].
#[inline(always)]
unsafe fn unpack<U: Unpack, const NR: usize>(
    kc: usize,
    panel: *const U,
    rows: bool,
    dst: *mut i16,
    param: U::Param<'_>,
) {
    let lanes = if rows { 4 } else { NR };
    let (groups, tail) = (kc / U::G, !kc.is_multiple_of(U::G));
    // SAFETY: per group `lanes · G` bytes of the panel and 2 steps of `2 · lanes` values,
    // half of that for the tail: the panel's `kc` elements and `kc · CODES` values per lane,
    // from the caller
    unsafe {
        let t = U::tables(param);
        let panel = panel.cast::<u8>();
        for g in 0..groups + usize::from(tail) {
            let both = g < groups;
            let (src, dst) = (panel.add(lanes * U::G * g), dst.add(4 * lanes * g));
            if rows {
                let pairs = U::rows(t, src, both);
                // Row panels start 32-byte aligned only if `kc` is even
                if both {
                    _mm256_storeu_si256(dst.cast(), pairs);
                } else {
                    _mm_storeu_si128(dst.cast(), _mm256_castsi256_si128(pairs));
                }
            } else {
                let [first, second] = U::cols(t, src, both);
                _mm256_store_si256(dst.cast(), first[0]);
                _mm256_store_si256(dst.add(16).cast(), first[1]);
                if both {
                    _mm256_store_si256(dst.add(32).cast(), second[0]);
                    _mm256_store_si256(dst.add(48).cast(), second[1]);
                }
            }
        }
    }
}

/// See [`Micro::tile`]: 4 rows by 16 columns of unpacked pairs, `[step][lane][2]`, over
/// `steps` pair steps, with i32 accumulators.
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn tile(steps: usize, a: *const i16, b: *const i16, c: *mut i32, ldc: usize, acc: bool) {
    const MR: usize = 4;
    const NR: usize = 16;
    let mut sums = [[_mm256_setzero_si256(); 2]; MR];
    // SAFETY: per step `MR` pairs of `a` and `NR` of `b`, within the `steps`, from the
    // caller. `MR` rows of `NR` values of `c`.
    unsafe {
        for s in 0..steps {
            let cols0 = _mm256_load_si256(b.add(NR * 2 * s).cast());
            let cols1 = _mm256_load_si256(b.add(NR * 2 * s + 16).cast());
            for (r, sums) in sums.iter_mut().enumerate() {
                let row =
                    _mm256_set1_epi32(a.add(MR * 2 * s + 2 * r).cast::<i32>().read_unaligned());
                sums[0] = _mm256_add_epi32(sums[0], _mm256_madd_epi16(row, cols0));
                sums[1] = _mm256_add_epi32(sums[1], _mm256_madd_epi16(row, cols1));
            }
        }
        for (r, sums) in sums.iter().enumerate() {
            for (h, &sum) in sums.iter().enumerate() {
                let c = c.add(r * ldc + 8 * h);
                let sum = if acc {
                    _mm256_add_epi32(_mm256_loadu_si256(c.cast()), sum)
                } else {
                    sum
                };
                _mm256_storeu_si256(c.cast(), sum);
            }
        }
    }
}
