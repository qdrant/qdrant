//! The page kernels: score every token of a page for up to four queries, and reduce a page's
//! values into an attention accumulator -- AVX-512 (BW + VNNI), AVX2 and a scalar reference
//! that all agree bit-exactly on their integer accumulators. The contract is
//! `storage/PAGES.md` (sections 2, 4 and 6); the page byte layouts are `pages::k_index` /
//! `pages::v_index`, and the codec constants come from `tq4`.
//!
//! Both passes are the same arithmetic the per-key kernel does ([`crate::kernel`]) -- nibbles
//! through one `vpshufb` into int8 levels, then `vpdpbusd` against an int8 grid -- but they
//! reduce over *different* axes, and that is the whole reason a page carries its keys and its
//! values in two different orders:
//!
//! * **score** (K half) reduces over coordinates, so the 16 int32 lanes of an accumulator are
//!   16 *tokens*; no horizontal reduction happens at all, where the per-key kernel ends every
//!   key with a `_mm512_reduce_add_epi32`. A 64-byte block is 16 tokens x one coordinate quad
//!   in its low nibbles and 16 tokens x another quad in its high ones, so one load, two `and`s
//!   and two `vpshufb` feed `2 * nq` `vpdpbusd` -- the load and the shuffles are shared by
//!   every query of the group, which is why scoring four queries costs far less than four
//!   times one.
//! * **attend** (V half) reduces over tokens, so the 16 lanes are 16 *coordinates* and the u8
//!   operand is the softmax weight of a token quad rather than the query.
//!
//! Two things differ per [`Path`] and both are forced by AVX2's `vpmaddubsw`, which sums two
//! `u8 * i8` products into an **i16** and saturates there where `vpdpbusd` accumulates in i32:
//! the query grid ([`Path::q_max`], 63 instead of 127, exactly as the per-key kernel already
//! does) and the attention's weight grid ([`weight_grid`], 127 instead of 255). Both are
//! properties of the *prepared* query / weight vector, not of the kernel that consumes it, so
//! a `PageQuery` built for one path can be fed to every path and the integer accumulators come
//! out bit-identical -- which is what the tests check.
//!
//! Three things the measurements here insisted on, all of them invisible in the source:
//!
//! * **every accumulator index must be a compile-time constant.** LLVM promotes a fixed-size
//!   array to registers only then; with a runtime `nsub` the score kernel round-tripped eight
//!   zmm through the stack on every `vpdpbusd` and ran at 3x its instruction count. Hence the
//!   const generics on [`score_avx512_n`].
//! * **the f32 scaffolding needs `#[target_feature]` too.** A plain function in this crate
//!   compiles for the baseline x86-64 target -- SSE2 -- so the per-token and per-coordinate
//!   loops around the integer pass ran four f32 wide while the kernel next to them ran
//!   sixteen. Wrapping the whole body per path ([`attend_body_avx512`]) was worth more than
//!   any change to the `vpdpbusd` loops.
//! * **short loops with a runtime trip count do not vectorise usefully.** A page is one or two
//!   zmm of tokens, and LLVM's generic vector-plus-remainder shape cost more in setup than the
//!   work; the epilogue ([`score_epilogue_avx512`]) and the weight quantisation
//!   ([`attend_quant_avx512`]) are written out instead.

use crate::kernel::{self, Path};
use crate::pages::{k_index, v_index, PAGE};
use crate::tq4::{Rotation, CODEBOOK4};

/// Queries one [`score_page`] call can share a page's loads and shuffles between.
pub const MAXQ: usize = 4;
/// Largest head dimension a page can hold (`PAGE / dim >= 16` tokens), and therefore the
/// largest accumulator the V pass keeps on the stack.
pub const MAXDIM: usize = 256;

/// Round to nearest, clamp to `[-lim, lim]` -- the same quantiser as `kernel::qi8`, repeated
/// here because that one is private and the two grids must agree bit for bit.
#[inline]
fn qi8(x: f32, lim: f32) -> i8 {
    let v = x.round();
    if v > lim {
        lim as i8
    } else if v < -lim {
        -(lim as i8)
    } else {
        v as i8
    }
}

/// A query prepared for the K half of a page (PAGES.md section 6).
pub struct PageQuery {
    pub dim: usize,
    /// `q . c_k`
    pub qc: f32,
    /// `sum_i qr_i`, f32, unquantised.
    pub sq: f32,
    /// `-128 * sum_i w_i`
    pub zero: i32,
    /// `qs * ls`
    pub factor: f32,
    /// `L[v] + 128`
    pub levels_biased: [u8; 16],
    /// `(dim / 4) * 64` bytes: quad `m` = the 4 int8 query bytes of coordinates `4m..4m+4`,
    /// repeated 16 times.
    pub wq: Vec<u8>,
}

impl PageQuery {
    pub fn new(q: &[f32], cent_k: &[f32], rot: &Rotation) -> PageQuery {
        PageQuery::with_path(q, cent_k, rot, kernel::path())
    }

    /// Prepare for a named path. The path chooses the query grid only ([`Path::q_max`]): the
    /// result is a plain byte vector every path can consume, so the tests build one query for
    /// the narrowest grid and run all three kernels over it.
    pub fn with_path(q: &[f32], cent_k: &[f32], rot: &Rotation, path: Path) -> PageQuery {
        let dim = rot.dim;
        assert_eq!(q.len(), dim, "query length must be head_dim");
        assert_eq!(cent_k.len(), dim, "centroid length must be head_dim");
        assert!(dim.is_multiple_of(4), "head_dim must be a multiple of 4");
        let mut qr = vec![0.0f32; dim];
        rot.apply(q, &mut qr);

        // the codebook on the int8 grid, biased by 128 for `vpdpbusd`'s unsigned operand; the
        // `-128 * sum(w)` this introduces is cancelled once per token by `zero`.
        let cmax = CODEBOOK4.iter().fold(0.0f32, |m, &c| m.max(c.abs()));
        let ls = if cmax > 0.0 { cmax / 127.0 } else { 1.0 };
        let mut levels_biased = [0u8; 16];
        for (v, b) in levels_biased.iter_mut().enumerate() {
            *b = (qi8(CODEBOOK4[v] / ls, 127.0) as i16 + 128) as u8;
        }

        let qmax = qr.iter().fold(0.0f32, |m, &v| m.max(v.abs()));
        let lim = path.q_max();
        let qs = if qmax > 0.0 { qmax / lim } else { 1.0 };
        let inv = 1.0 / qs;
        let w: Vec<i8> = qr.iter().map(|&v| qi8(v * inv, lim)).collect();
        let wsum: i32 = w.iter().map(|&v| v as i32).sum();

        // one 64-byte entry per coordinate quad: the quad repeated 16 times, so a zmm load is
        // "this quad for all 16 tokens" and a ymm load is the same for 8 tokens.
        let mut wq = vec![0u8; (dim / 4) * 64];
        for m in 0..dim / 4 {
            for r in 0..16 {
                for b in 0..4 {
                    wq[m * 64 + r * 4 + b] = w[4 * m + b] as u8;
                }
            }
        }
        PageQuery {
            dim,
            qc: q.iter().zip(cent_k).map(|(a, b)| a * b).sum(),
            sq: qr.iter().sum(),
            zero: -128 * wsum,
            factor: qs * ls,
            levels_biased,
            wq,
        }
    }

    /// The int8 query weight of coordinate `i` (the scalar reference's view of [`Self::wq`]).
    #[inline]
    fn w(&self, i: usize) -> i8 {
        self.wq[(i / 4) * 64 + (i % 4)] as i8
    }
}

// ---------------------------------------------------------------------------------------------
// score: the K half
// ---------------------------------------------------------------------------------------------

/// Raw scores (before `score_scale`) of one page's `t` tokens for 1..=4 queries:
/// `out[qi][tok]` for `tok < valid`, `f32::NEG_INFINITY` for padding slots.
#[allow(clippy::too_many_arguments)]
pub fn score_page(
    page: &[u8],
    dim: usize,
    t: usize,
    valid: usize,
    scale_k: &[f32],
    shift_k: &[f32],
    qs: &[&PageQuery],
    out: &mut [[f32; 32]],
) {
    score_page_with_path(
        page,
        dim,
        t,
        valid,
        scale_k,
        shift_k,
        qs,
        kernel::path(),
        out,
    )
}

/// [`score_page`] on a named path (the bench and the cross-path tests).
///
/// Multiversioned for the same reason [`attend_page_with_path`] is: the epilogue that turns
/// the int32 accumulators into scores is a per-token loop, and a plain function in this crate
/// compiles for the baseline x86-64 target (SSE2).
#[allow(clippy::too_many_arguments)]
pub fn score_page_with_path(
    page: &[u8],
    dim: usize,
    t: usize,
    valid: usize,
    scale_k: &[f32],
    shift_k: &[f32],
    qs: &[&PageQuery],
    path: Path,
    out: &mut [[f32; 32]],
) {
    #[cfg(target_arch = "x86_64")]
    match path {
        Path::Avx512 => {
            return unsafe { score_body_avx512(page, dim, t, valid, scale_k, shift_k, qs, out) }
        }
        Path::Avx2 => {
            return unsafe { score_body_avx2(page, dim, t, valid, scale_k, shift_k, qs, out) }
        }
        Path::Scalar => {}
    }
    score_body(page, dim, t, valid, scale_k, shift_k, qs, path, out)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(
    enable = "avx2",
    enable = "fma",
    enable = "avx512f",
    enable = "avx512bw",
    enable = "avx512vl",
    enable = "avx512vnni"
)]
#[allow(clippy::too_many_arguments)]
unsafe fn score_body_avx512(
    page: &[u8],
    dim: usize,
    t: usize,
    valid: usize,
    scale_k: &[f32],
    shift_k: &[f32],
    qs: &[&PageQuery],
    out: &mut [[f32; 32]],
) {
    score_body(page, dim, t, valid, scale_k, shift_k, qs, Path::Avx512, out)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2", enable = "fma")]
#[allow(clippy::too_many_arguments)]
unsafe fn score_body_avx2(
    page: &[u8],
    dim: usize,
    t: usize,
    valid: usize,
    scale_k: &[f32],
    shift_k: &[f32],
    qs: &[&PageQuery],
    out: &mut [[f32; 32]],
) {
    score_body(page, dim, t, valid, scale_k, shift_k, qs, Path::Avx2, out)
}

#[allow(clippy::too_many_arguments)]
#[inline(always)]
fn score_body(
    page: &[u8],
    dim: usize,
    t: usize,
    valid: usize,
    scale_k: &[f32],
    shift_k: &[f32],
    qs: &[&PageQuery],
    path: Path,
    out: &mut [[f32; 32]],
) {
    let nq = qs.len();
    if nq == 0 {
        return;
    }
    assert!(nq <= MAXQ, "score_page takes at most {MAXQ} queries");
    assert!(out.len() >= nq, "one output row per query");
    assert!(valid <= t && scale_k.len() >= valid && shift_k.len() >= valid);
    let mut acc = [[0i32; 32]; MAXQ];
    score_page_acc(page, dim, t, qs, path, &mut acc[..nq]);
    for (qi, q) in qs.iter().enumerate() {
        let row = &mut out[qi];
        let a = &acc[qi];
        #[cfg(target_arch = "x86_64")]
        if path == Path::Avx512 && t.is_multiple_of(16) {
            // A page is one or two zmm of tokens, so the epilogue is ~10 instructions rather
            // than a 16-trip loop whose setup and remainder handling LLVM cannot amortise:
            // auto-vectorised, this cost ~25 ns per query, more than the query's share of the
            // integer pass.
            unsafe { score_epilogue_avx512(valid, t, scale_k, shift_k, q, a, row) };
            continue;
        }
        // `zero` is added in f32 rather than i32 so the loop stays entirely in float SIMD:
        // both are exact integers in f32 and so is their sum (`|acc| <= d * 255 * 127` and
        // `|zero| <= 128 * d * 127`, together under 2^24 at d = 256), so this is bit-identical
        // to `(acc + zero) as f32`. `acc + zero` undoes the +128 bias of the shuffle table and
        // `factor = qs * ls` turns the int32 back into `sum_i qr_i * CB[code_i]`
        // (PAGES.md section 2).
        let (qc, zero, factor, sq) = (q.qc, q.zero as f32, q.factor, q.sq);
        for (((o, &ai), &sk), &hk) in row[..valid]
            .iter_mut()
            .zip(a[..valid].iter())
            .zip(&scale_k[..valid])
            .zip(&shift_k[..valid])
        {
            *o = qc + sk * ((ai as f32 + zero) * factor) + hk * sq;
        }
        for slot in row.iter_mut().take(t).skip(valid) {
            *slot = f32::NEG_INFINITY;
        }
    }
}

/// The score epilogue for one query, one zmm of tokens at a time: `s = qc + scale_k * ((acc +
/// zero) * factor) + shift_k * sq`, with `-inf` in the padding lanes.
///
/// The multiplies and adds are kept separate (no `fmadd`) so that this is bit-identical to the
/// scalar epilogue, and `scale_k` / `shift_k` are read with a *masked* load so the slices need
/// only the page's `valid` entries, not a full T.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
#[allow(clippy::too_many_arguments)]
unsafe fn score_epilogue_avx512(
    valid: usize,
    t: usize,
    scale_k: &[f32],
    shift_k: &[f32],
    q: &PageQuery,
    a: &[i32; 32],
    row: &mut [f32; 32],
) {
    use std::arch::x86_64::*;
    let qc = _mm512_set1_ps(q.qc);
    let zero = _mm512_set1_ps(q.zero as f32);
    let factor = _mm512_set1_ps(q.factor);
    let sq = _mm512_set1_ps(q.sq);
    let ninf = _mm512_set1_ps(f32::NEG_INFINITY);
    for j in 0..t / 16 {
        let lo = j * 16;
        let m: __mmask16 = if valid >= lo + 16 {
            !0
        } else if valid <= lo {
            0
        } else {
            (1u16 << (valid - lo)) - 1
        };
        // `zero` is added in f32: both it and `acc` are exact integers in f32 and so is their
        // sum (under 2^24 at d = 256), so this matches `(acc + zero) as f32` bit for bit.
        let ai = _mm512_cvtepi32_ps(_mm512_loadu_si512(a.as_ptr().add(lo) as *const __m512i));
        let v = _mm512_mul_ps(_mm512_add_ps(ai, zero), factor);
        let sk = _mm512_maskz_loadu_ps(m, scale_k.as_ptr().add(lo));
        let hk = _mm512_maskz_loadu_ps(m, shift_k.as_ptr().add(lo));
        let r = _mm512_add_ps(
            _mm512_add_ps(qc, _mm512_mul_ps(sk, v)),
            _mm512_mul_ps(hk, sq),
        );
        _mm512_storeu_ps(row.as_mut_ptr().add(lo), _mm512_mask_blend_ps(m, ninf, r));
    }
}

/// The raw, still-biased int32 accumulators `sum_i (L[code_i] + 128) * w_i` per token, per
/// query. This is the value the three paths must produce bit-identically; everything after it
/// is shared f32 arithmetic.
pub fn score_page_acc(
    page: &[u8],
    dim: usize,
    t: usize,
    qs: &[&PageQuery],
    path: Path,
    out: &mut [[i32; 32]],
) {
    let nq = qs.len();
    if nq == 0 {
        return;
    }
    assert!(nq <= MAXQ && out.len() >= nq);
    assert!(dim * t == PAGE, "a page holds exactly PAGE/dim tokens");
    assert!(
        t <= 32 && dim <= MAXDIM,
        "d >= 128 (so T <= 32) in this generation"
    );
    assert!(page.len() >= PAGE, "a page is {PAGE} bytes");
    debug_assert!(
        qs.iter()
            .all(|q| q.dim == dim && q.levels_biased == qs[0].levels_biased),
        "the queries of one call must share the head's dimension and codebook grid"
    );
    #[cfg(target_arch = "x86_64")]
    {
        // The wide paths need whole registers of tokens (16 per zmm, 8 per ymm) and are
        // monomorphised on the shapes below so that every accumulator index is a compile-time
        // constant -- see [`score_avx512_n`].
        if path == Path::Avx512 {
            let done = unsafe {
                match (nq, t) {
                    (1, 16) => score_avx512_n::<1, 1, 8>(page, dim, qs, out),
                    (2, 16) => score_avx512_n::<2, 1, 4>(page, dim, qs, out),
                    (3, 16) => score_avx512_n::<3, 1, 2>(page, dim, qs, out),
                    (4, 16) => score_avx512_n::<4, 1, 2>(page, dim, qs, out),
                    (1, 32) => score_avx512_n::<1, 2, 4>(page, dim, qs, out),
                    (2, 32) => score_avx512_n::<2, 2, 2>(page, dim, qs, out),
                    (3, 32) => score_avx512_n::<3, 2, 1>(page, dim, qs, out),
                    (4, 32) => score_avx512_n::<4, 2, 1>(page, dim, qs, out),
                    _ => false,
                }
            };
            if done {
                return;
            }
        }
        if path == Path::Avx2 && t.is_multiple_of(8) {
            let done = unsafe {
                match nq {
                    1 => score_avx2_n::<1>(page, dim, t, qs, out),
                    2 => score_avx2_n::<2>(page, dim, t, qs, out),
                    3 => score_avx2_n::<3>(page, dim, t, qs, out),
                    4 => score_avx2_n::<4>(page, dim, t, qs, out),
                    _ => false,
                }
            };
            if done {
                return;
            }
        }
    }
    let _ = path;
    for row in out.iter_mut().take(nq) {
        row.fill(0);
    }
    score_scalar(page, dim, t, qs, out);
}

/// The reference: walk the K half exactly as `pages::k_index` defines it.
///
/// The loop is written over the *layout* (block, nibble half, token, byte) rather than over
/// `(tok, i)` because `k_index`'s `m / (Q/2)` is a division by a runtime value, i.e. ~20 cycles
/// per coordinate; a `debug_assert_eq!` against `k_index` on every single element keeps the two
/// honest, so every test run checks the traversal against the contract.
fn score_scalar(page: &[u8], dim: usize, t: usize, qs: &[&PageQuery], out: &mut [[i32; 32]]) {
    let half = dim / 8; // Q/2 blocks of 4*T bytes
    for (qi, q) in qs.iter().enumerate() {
        let row = &mut out[qi];
        for blk in 0..half {
            let o = blk * 4 * t;
            for tok in 0..t {
                let mut s = 0i32;
                for b in 0..4 {
                    let byte = o + tok * 4 + b;
                    let (il, ih) = (4 * blk + b, 4 * (blk + half) + b);
                    debug_assert_eq!(k_index(dim, t, tok, il), (byte, 0));
                    debug_assert_eq!(k_index(dim, t, tok, ih), (byte, 4));
                    let c = page[byte];
                    s += q.levels_biased[(c & 0x0f) as usize] as i32 * q.w(il) as i32;
                    s += q.levels_biased[(c >> 4) as usize] as i32 * q.w(ih) as i32;
                }
                row[tok] += s;
            }
        }
    }
}

/// The AVX-512 score pass, monomorphised on `NQ` queries, `NACC` zmm of tokens (1 at T = 16,
/// 2 at T = 32) and `NSUB` sub-accumulators per chain. Returns false if the geometry does not
/// fit, so the caller can fall back.
///
/// **Why three const parameters.** `vpdpbusd` has a 5-cycle latency and accumulates *into* its
/// destination, so one accumulator over `Q/2` blocks is a dependency chain `2 * Q/2` deep --
/// 320 cycles at d = 256, five times the throughput cost of the same 64 instructions.
/// Splitting the block loop over `NSUB` rotating accumulators breaks the chain, and eight live
/// accumulators is the budget (of 32 zmm registers, leaving room for the shuffle table, the
/// masks, the code and the per-query weight quads): four queries get one each, a single query
/// gets eight. The parameters must be *const* because LLVM only promotes a fixed-size array to
/// registers when every index into it is a compile-time constant -- with a runtime `nsub` the
/// first version of this kernel round-tripped all eight accumulators through the stack on every
/// instruction and measured 374 ns/page where the instruction count says ~110.
#[cfg(target_arch = "x86_64")]
#[target_feature(
    enable = "avx512f",
    enable = "avx512bw",
    enable = "avx512vl",
    enable = "avx512vnni"
)]
#[allow(clippy::needless_range_loop)]
unsafe fn score_avx512_n<const NQ: usize, const NACC: usize, const NSUB: usize>(
    page: &[u8],
    dim: usize,
    qs: &[&PageQuery],
    out: &mut [[i32; 32]],
) -> bool {
    use std::arch::x86_64::*;
    let t = NACC * 16;
    let half = dim / 8; // Q/2 blocks
    if !half.is_multiple_of(NSUB) || dim * t != PAGE {
        return false;
    }
    let blkb = 4 * t; // bytes per block: 64 at T=16, 128 at T=32
    let levels = _mm512_broadcast_i32x4(_mm_loadu_si128(
        qs[0].levels_biased.as_ptr() as *const __m128i
    ));
    let m0f = _mm512_set1_epi8(0x0f);
    let base = page.as_ptr();
    // hoisted out of the block loop: `qs[qi].wq` is two dependent loads the compiler cannot
    // sink through the stores otherwise
    let mut wptr = [std::ptr::null::<u8>(); NQ];
    for qi in 0..NQ {
        wptr[qi] = qs[qi].wq.as_ptr();
    }
    // flat `[(qi * NACC + j) * NSUB + s]`, at most 8 live and every index a constant
    let mut a = [_mm512_setzero_si512(); 8];
    let mut blk0 = 0;
    while blk0 < half {
        for s in 0..NSUB {
            let blk = blk0 + s;
            for j in 0..NACC {
                let code = _mm512_loadu_si512(base.add(blk * blkb + j * 64) as *const __m512i);
                // low nibbles: coordinate quad `blk`; high nibbles: quad `blk + Q/2`
                let lo = _mm512_shuffle_epi8(levels, _mm512_and_si512(code, m0f));
                let hi = _mm512_shuffle_epi8(
                    levels,
                    _mm512_and_si512(_mm512_srli_epi16::<4>(code), m0f),
                );
                for qi in 0..NQ {
                    let wp = wptr[qi];
                    let wlo = _mm512_loadu_si512(wp.add(blk * 64) as *const __m512i);
                    let whi = _mm512_loadu_si512(wp.add((blk + half) * 64) as *const __m512i);
                    let idx = (qi * NACC + j) * NSUB + s;
                    a[idx] = _mm512_dpbusd_epi32(a[idx], lo, wlo);
                    a[idx] = _mm512_dpbusd_epi32(a[idx], hi, whi);
                }
            }
        }
        blk0 += NSUB;
    }
    // `[i32; 32]` is 32 contiguous i32, so one row is a plain `*mut i32`
    let op = out.as_mut_ptr() as *mut i32;
    for qi in 0..NQ {
        for j in 0..NACC {
            let mut v = a[(qi * NACC + j) * NSUB];
            for s in 1..NSUB {
                v = _mm512_add_epi32(v, a[(qi * NACC + j) * NSUB + s]);
            }
            _mm512_storeu_epi32(op.add(qi * 32 + j * 16), v);
        }
    }
    true
}

/// AVX2 fallback: `vpmaddubsw` + `vpmaddwd` with a ones vector, 8 tokens per ymm.
///
/// The i16 intermediate cannot saturate because the query grid is capped at
/// [`Path::q_max`] = 63: with biased levels at most 255 a pair sums to at most
/// `2 * 255 * 63 = 32130 < 32767`. The token groups are the *outer* loop here (the block loop
/// is inner) so that only `nq` accumulators are live at a time -- AVX2 has 16 ymm registers,
/// and `nq * (T/8)` accumulators would be all of them at T = 32.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[allow(clippy::needless_range_loop)]
unsafe fn score_avx2_n<const NQ: usize>(
    page: &[u8],
    dim: usize,
    t: usize,
    qs: &[&PageQuery],
    out: &mut [[i32; 32]],
) -> bool {
    use std::arch::x86_64::*;
    let nq = NQ;
    let half = dim / 8;
    let blkb = 4 * t;
    let levels = _mm256_broadcastsi128_si256(_mm_loadu_si128(
        qs[0].levels_biased.as_ptr() as *const __m128i
    ));
    let ones = _mm256_set1_epi16(1);
    let m0f = _mm256_set1_epi8(0x0f);
    let base = page.as_ptr();
    let op = out.as_mut_ptr() as *mut i32;
    let mut wptr = [std::ptr::null::<u8>(); NQ];
    for qi in 0..nq {
        wptr[qi] = qs[qi].wq.as_ptr();
    }
    for j in 0..t / 8 {
        let mut a = [_mm256_setzero_si256(); MAXQ];
        for blk in 0..half {
            let code = _mm256_loadu_si256(base.add(blk * blkb + j * 32) as *const __m256i);
            let lo = _mm256_shuffle_epi8(levels, _mm256_and_si256(code, m0f));
            let hi =
                _mm256_shuffle_epi8(levels, _mm256_and_si256(_mm256_srli_epi16::<4>(code), m0f));
            for qi in 0..nq {
                let wp = wptr[qi];
                let wlo = _mm256_loadu_si256(wp.add(blk * 64) as *const __m256i);
                let whi = _mm256_loadu_si256(wp.add((blk + half) * 64) as *const __m256i);
                a[qi] = _mm256_add_epi32(
                    a[qi],
                    _mm256_madd_epi16(_mm256_maddubs_epi16(lo, wlo), ones),
                );
                a[qi] = _mm256_add_epi32(
                    a[qi],
                    _mm256_madd_epi16(_mm256_maddubs_epi16(hi, whi), ones),
                );
            }
        }
        for qi in 0..nq {
            _mm256_storeu_si256(op.add(qi * 32 + j * 8) as *mut __m256i, a[qi]);
        }
    }
    true
}

// ---------------------------------------------------------------------------------------------
// attend: the V half
// ---------------------------------------------------------------------------------------------

/// The codebook on the int8 grid, unbiased, for the V half's shuffle table.
pub struct VLevels {
    pub levels_i8: [i8; 16],
    /// `max_v |CB[v]| / 127`
    pub ls: f32,
}

impl VLevels {
    pub fn new() -> VLevels {
        let cmax = CODEBOOK4.iter().fold(0.0f32, |m, &c| m.max(c.abs()));
        let ls = if cmax > 0.0 { cmax / 127.0 } else { 1.0 };
        let mut levels_i8 = [0i8; 16];
        for (v, l) in levels_i8.iter_mut().enumerate() {
            *l = qi8(CODEBOOK4[v] / ls, 127.0);
        }
        VLevels { levels_i8, ls }
    }
}

impl Default for VLevels {
    fn default() -> Self {
        VLevels::new()
    }
}

/// One query's attention accumulator over pages.
pub struct VAcc {
    pub dim: usize,
    /// `sum_t w_t * scale_v_t * CB[code_t]`, in rotated space.
    pub num_rot: Vec<f32>,
    /// `sum_t w_t * shift_v_t`
    pub shift_sum: f32,
    /// `sum_t w_t`
    pub z: f64,
}

impl VAcc {
    pub fn new(dim: usize) -> VAcc {
        VAcc {
            dim,
            num_rot: vec![0.0; dim],
            shift_sum: 0.0,
            z: 0.0,
        }
    }
}

/// Precision of the per-page weight quantisation: one u8 pass, or a hi/lo u16 pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WeightBits {
    Eight,
    Sixteen,
}

/// The integer grid a page's weights are quantised to, as `(max, split shift, passes)`.
///
/// `vpdpbusd` cannot overflow, so AVX-512 and the scalar reference use the whole u8 range and,
/// for [`WeightBits::Sixteen`], a plain hi/lo byte split of a 16-bit weight. AVX2's
/// `vpmaddubsw` sums two `u8 * i8` products into an i16 and the levels reach 127, so a byte of
/// the weight must stay within `32767 / (2 * 127) = 129`: the u8 pass drops to a **7-bit
/// grid** (127, i.e. one bit of weight precision -- 0.4 % instead of 0.2 % relative, well under
/// the 1 % the int8 *levels* already cost) and the two-pass form splits a 14-bit weight at
/// **base 128** rather than 256, so both bytes stay within 127. Reducing the weights rather
/// than the levels is deliberate: the levels are the data, the weights are softmax mass whose
/// own dynamic range is already bounded by the page max.
#[inline]
pub fn weight_grid(bits: WeightBits, path: Path) -> (u32, u32, usize) {
    match (bits, path) {
        (WeightBits::Eight, Path::Avx2) => (127, 0, 1),
        (WeightBits::Eight, _) => (255, 0, 1),
        (WeightBits::Sixteen, Path::Avx2) => (127 * 128 + 127, 7, 2),
        (WeightBits::Sixteen, _) => (65535, 8, 2),
    }
}

/// One query, one page: `w[tok] = exp(s_tok - m)` for tokens in U, 0 otherwise (and for
/// padding). Adds the page's contribution to `acc` (PAGES.md section 6).
#[allow(clippy::too_many_arguments)]
pub fn attend_page(
    page: &[u8],
    dim: usize,
    t: usize,
    valid: usize,
    scale_v: &[f32],
    shift_v: &[f32],
    w: &[f32; 32],
    lv: &VLevels,
    bits: WeightBits,
    acc: &mut VAcc,
) {
    attend_page_with_path(
        page,
        dim,
        t,
        valid,
        scale_v,
        shift_v,
        w,
        lv,
        bits,
        kernel::path(),
        acc,
    )
}

/// [`attend_page`] on a named path (the bench and the cross-path tests).
///
/// The body is *multiversioned*: the f32 prologue (the page max and the two exact sums), the
/// weight quantisation and the epilogue that folds the int32 accumulators into `num_rot` are
/// per-token and per-coordinate loops, and a plain function in this crate compiles for the
/// baseline x86-64 target -- SSE2, four f32 per instruction. Only `#[target_feature]` code
/// gets AVX-512. Leaving the scaffolding outside cost more than the whole integer pass
/// (~215 ns of a 280 ns call at d = 256), so each path calls its own copy of the body.
#[allow(clippy::too_many_arguments)]
pub fn attend_page_with_path(
    page: &[u8],
    dim: usize,
    t: usize,
    valid: usize,
    scale_v: &[f32],
    shift_v: &[f32],
    w: &[f32; 32],
    lv: &VLevels,
    bits: WeightBits,
    path: Path,
    acc: &mut VAcc,
) {
    #[cfg(target_arch = "x86_64")]
    match path {
        Path::Avx512 => {
            return unsafe {
                attend_body_avx512(page, dim, t, valid, scale_v, shift_v, w, lv, bits, acc)
            }
        }
        Path::Avx2 => {
            return unsafe {
                attend_body_avx2(page, dim, t, valid, scale_v, shift_v, w, lv, bits, acc)
            }
        }
        Path::Scalar => {}
    }
    attend_body(
        page, dim, t, valid, scale_v, shift_v, w, lv, bits, path, acc,
    )
}

#[cfg(target_arch = "x86_64")]
#[target_feature(
    enable = "avx2",
    enable = "fma",
    enable = "avx512f",
    enable = "avx512bw",
    enable = "avx512vl",
    enable = "avx512vnni"
)]
#[allow(clippy::too_many_arguments)]
unsafe fn attend_body_avx512(
    page: &[u8],
    dim: usize,
    t: usize,
    valid: usize,
    scale_v: &[f32],
    shift_v: &[f32],
    w: &[f32; 32],
    lv: &VLevels,
    bits: WeightBits,
    acc: &mut VAcc,
) {
    attend_body(
        page,
        dim,
        t,
        valid,
        scale_v,
        shift_v,
        w,
        lv,
        bits,
        Path::Avx512,
        acc,
    )
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2", enable = "fma")]
#[allow(clippy::too_many_arguments)]
unsafe fn attend_body_avx2(
    page: &[u8],
    dim: usize,
    t: usize,
    valid: usize,
    scale_v: &[f32],
    shift_v: &[f32],
    w: &[f32; 32],
    lv: &VLevels,
    bits: WeightBits,
    acc: &mut VAcc,
) {
    attend_body(
        page,
        dim,
        t,
        valid,
        scale_v,
        shift_v,
        w,
        lv,
        bits,
        Path::Avx2,
        acc,
    )
}

#[allow(clippy::too_many_arguments)]
#[inline(always)]
fn attend_body(
    page: &[u8],
    dim: usize,
    t: usize,
    valid: usize,
    scale_v: &[f32],
    shift_v: &[f32],
    w: &[f32; 32],
    lv: &VLevels,
    bits: WeightBits,
    path: Path,
    acc: &mut VAcc,
) {
    assert_eq!(
        acc.dim, dim,
        "accumulator and page must share the head's dimension"
    );
    assert!(valid <= t && scale_v.len() >= valid && shift_v.len() >= valid);

    // The two exact scalars first: they are f32/f64 sums of unquantised terms and do not care
    // whether the integer pass runs at all.
    //
    // Four interleaved partials rather than one running sum, and `max(0.0)` rather than a
    // `continue`: this loop runs T times per page against ~64 `vpdpbusd` in the integer pass,
    // so a branch on the weight and a serial f64 chain (4 cycles per token, twice) cost more
    // than the SIMD it feeds -- the first measurement spent ~290 of 446 ns/page here at T = 32.
    // `max(0.0)` also maps a NaN weight to zero, since `f32::max` returns the non-NaN operand.
    let n = valid;
    let mut wp = [0.0f32; 32];
    #[allow(unused_mut)]
    let mut prep = None;
    #[cfg(target_arch = "x86_64")]
    if path == Path::Avx512 {
        prep = Some(unsafe { attend_prep_avx512(n, scale_v, shift_v, w, &mut wp) });
    }
    let (zsum, ssum, wmax) =
        prep.unwrap_or_else(|| attend_prep_scalar(n, scale_v, shift_v, w, &mut wp));
    acc.z += zsum;
    acc.shift_sum += ssum as f32;
    if wmax <= 0.0 {
        return; // no token of this page carries mass: nothing for the integer pass to add
    }

    let (gmax, shift, passes) = weight_grid(bits, path);
    let inv = gmax as f32 / wmax;
    let (mut w_hi, mut w_lo) = ([0u8; 32], [0u8; 32]);
    #[allow(unused_mut)]
    let mut quantised = false;
    #[cfg(target_arch = "x86_64")]
    if path == Path::Avx512 {
        unsafe { attend_quant_avx512(n, &wp, inv, gmax, shift, &mut w_hi, &mut w_lo) };
        quantised = true;
    }
    if !quantised {
        let mask = if shift == 0 { 0 } else { (1u32 << shift) - 1 };
        let top = gmax as f32;
        for ((h, l), &p) in w_hi[..n]
            .iter_mut()
            .zip(w_lo[..n].iter_mut())
            .zip(wp[..n].iter())
        {
            // ties to even, to match `vrndscaleps`'s default rounding control on the AVX-512
            // path; PAGES.md's `round` does not say which way ties go, and the weights come
            // out of `exp`, so a tie is a measure-zero event either way.
            let v = (p * inv).round_ties_even().clamp(0.0, top) as u32;
            *h = (v >> shift) as u8;
            *l = (v & mask) as u8;
        }
    }

    let mut ai = [0i32; MAXDIM];
    attend_page_acc(page, dim, t, &w_hi, lv, path, &mut ai[..dim]);
    // f32 throughout and over slices, so that this loop vectorises: an int32 accumulator is at
    // most `T * 255 * 127 ~ 1.0e6`, well inside f32's exactly-representable integers, and the
    // scalar f64 version of this epilogue cost more than the whole integer pass (~200 of the
    // 710 ns the first measurement showed).
    let mul = (wmax / gmax as f32) * lv.ls;
    if passes == 1 {
        for (d, &a) in acc.num_rot[..dim].iter_mut().zip(ai[..dim].iter()) {
            *d += mul * a as f32;
        }
    } else {
        let mut bi = [0i32; MAXDIM];
        attend_page_acc(page, dim, t, &w_lo, lv, path, &mut bi[..dim]);
        // the hi pass carries the top bits of the weight, so it weighs `2^shift` more; the two
        // terms are scaled separately rather than combined as one integer, which would leave
        // f32's exact range.
        let mhi = mul * (1u32 << shift) as f32;
        for ((d, &a), &b) in acc.num_rot[..dim]
            .iter_mut()
            .zip(ai[..dim].iter())
            .zip(bi[..dim].iter())
        {
            *d += mhi * a as f32 + mul * b as f32;
        }
    }
}

/// `w'[tok] = max(w, 0) * scale_v`, the page max, and the two exact f64 sums `sum w` and
/// `sum w * shift_v`.
///
/// `max(w, 0)` rather than a `continue` on a non-positive weight: the branch was
/// data-dependent and stopped this loop vectorising, and it also maps a NaN weight to zero
/// (`f32::max` returns the non-NaN operand). Four interleaved partials because one running
/// f64 sum is a 4-cycle serial chain per token, and this loop runs T times per page against
/// only ~64 `vpdpbusd` in the integer pass.
fn attend_prep_scalar(
    n: usize,
    scale_v: &[f32],
    shift_v: &[f32],
    w: &[f32; 32],
    wp: &mut [f32; 32],
) -> (f64, f64, f32) {
    for (o, (&a, &s)) in wp[..n].iter_mut().zip(w[..n].iter().zip(&scale_v[..n])) {
        *o = a.max(0.0) * s;
    }
    let (mut zp, mut sp, mut mp) = ([0.0f64; 4], [0.0f64; 4], [0.0f32; 4]);
    let mut tok = 0;
    while tok + 4 <= n {
        for k in 0..4 {
            let wt = w[tok + k].max(0.0);
            zp[k] += wt as f64;
            sp[k] += (wt * shift_v[tok + k]) as f64;
            mp[k] = mp[k].max(wp[tok + k]);
        }
        tok += 4;
    }
    while tok < n {
        let wt = w[tok].max(0.0);
        zp[0] += wt as f64;
        sp[0] += (wt * shift_v[tok]) as f64;
        mp[0] = mp[0].max(wp[tok]);
        tok += 1;
    }
    (
        (zp[0] + zp[1]) + (zp[2] + zp[3]),
        (sp[0] + sp[1]) + (sp[2] + sp[3]),
        mp[0].max(mp[1]).max(mp[2]).max(mp[3]),
    )
}

/// [`attend_prep_scalar`] in eight-token steps. Even auto-vectorised inside an AVX-512 body
/// the scalar form cost ~10 cycles per token -- about 140 ns of a 210 ns call at T = 32, more
/// than the integer pass it feeds -- because the f64 conversions and the max reduction stayed
/// scalar. `scale_v` / `shift_v` are read with masked loads, so they need only `n` entries.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f", enable = "avx512vl")]
unsafe fn attend_prep_avx512(
    n: usize,
    scale_v: &[f32],
    shift_v: &[f32],
    w: &[f32; 32],
    wp: &mut [f32; 32],
) -> (f64, f64, f32) {
    use std::arch::x86_64::*;
    let (mut zacc, mut sacc) = (_mm512_setzero_pd(), _mm512_setzero_pd());
    let mut macc = _mm256_setzero_ps();
    let z8 = _mm256_setzero_ps();
    let mut lo = 0;
    while lo < n {
        let left = n - lo;
        let m: __mmask8 = if left >= 8 { !0 } else { (1u8 << left) - 1 };
        // `vmaxps` returns its SECOND operand when either is NaN, so this is `max(w, 0)` with
        // NaN mapped to zero, the same as `f32::max(w, 0.0)`.
        let wc = _mm256_max_ps(_mm256_maskz_loadu_ps(m, w.as_ptr().add(lo)), z8);
        let sv = _mm256_maskz_loadu_ps(m, scale_v.as_ptr().add(lo));
        let hv = _mm256_maskz_loadu_ps(m, shift_v.as_ptr().add(lo));
        let p = _mm256_mul_ps(wc, sv);
        // `wp` is 32 wide and `lo` is a multiple of 8 below `n <= 32`, so this store is in
        // bounds; the masked-off lanes are zero and only raise the max by nothing.
        _mm256_storeu_ps(wp.as_mut_ptr().add(lo), p);
        macc = _mm256_max_ps(macc, p);
        zacc = _mm512_add_pd(zacc, _mm512_cvtps_pd(wc));
        sacc = _mm512_add_pd(sacc, _mm512_cvtps_pd(_mm256_mul_ps(wc, hv)));
        lo += 8;
    }
    let mut mm = [0.0f32; 8];
    _mm256_storeu_ps(mm.as_mut_ptr(), macc);
    let wmax = mm.iter().fold(0.0f32, |a, &b| a.max(b));
    (_mm512_reduce_add_pd(zacc), _mm512_reduce_add_pd(sacc), wmax)
}

/// `w8 = round(gmax * w' / wmax)` split into its hi and lo bytes, 16 tokens per pass.
///
/// `vrndscaleps` with RC = 0 rounds ties to even, which is why the scalar form above uses
/// `round_ties_even` rather than `round`: the two must agree on every input, not just almost
/// every one. After the rounding and the clamp, `vcvttps2dq`'s truncation is exact, and
/// `vpmovdb` narrows the 16 int32 to 16 bytes in one instruction.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f", enable = "avx512bw", enable = "avx512vl")]
#[allow(clippy::too_many_arguments)]
unsafe fn attend_quant_avx512(
    n: usize,
    wp: &[f32; 32],
    inv: f32,
    gmax: u32,
    shift: u32,
    w_hi: &mut [u8; 32],
    w_lo: &mut [u8; 32],
) {
    use std::arch::x86_64::*;
    let vinv = _mm512_set1_ps(inv);
    let top = _mm512_set1_ps(gmax as f32);
    let zerof = _mm512_setzero_ps();
    let maskv = _mm512_set1_epi32(if shift == 0 {
        0
    } else {
        ((1u32 << shift) - 1) as i32
    });
    let cnt = _mm_cvtsi32_si128(shift as i32);
    let mut lo = 0;
    while lo < n {
        let left = n - lo;
        let m: __mmask16 = if left >= 16 { !0 } else { (1u16 << left) - 1 };
        let v =
            _mm512_roundscale_ps::<0>(_mm512_mul_ps(_mm512_loadu_ps(wp.as_ptr().add(lo)), vinv));
        let i = _mm512_cvttps_epi32(_mm512_min_ps(_mm512_max_ps(v, zerof), top));
        _mm_mask_storeu_epi8(
            w_hi.as_mut_ptr().add(lo) as *mut i8,
            m,
            _mm512_cvtepi32_epi8(_mm512_srl_epi32(i, cnt)),
        );
        _mm_mask_storeu_epi8(
            w_lo.as_mut_ptr().add(lo) as *mut i8,
            m,
            _mm512_cvtepi32_epi8(_mm512_and_si512(i, maskv)),
        );
        lo += 16;
    }
}

/// The raw int32 accumulators `sum_t w8[tok] * L[code_{tok,i}]` per coordinate, overwriting
/// `out[..dim]`. The three paths must agree on this bit for bit.
pub fn attend_page_acc(
    page: &[u8],
    dim: usize,
    t: usize,
    w8: &[u8; 32],
    lv: &VLevels,
    path: Path,
    out: &mut [i32],
) {
    assert!(dim * t == PAGE, "a page holds exactly PAGE/dim tokens");
    assert!(t <= 32 && dim <= MAXDIM);
    assert!(page.len() >= PAGE && out.len() >= dim);
    #[cfg(target_arch = "x86_64")]
    {
        // a 64-byte block is 16 coordinates x one token quad per nibble half; the SIMD paths
        // write every lane, so they need no zero fill
        if dim.is_multiple_of(16 * CBU) && t.is_multiple_of(8) && path != Path::Scalar {
            let wrep = repeat_quads(w8);
            if path == Path::Avx512 {
                unsafe { attend_avx512(page, dim, t, &wrep, lv, out) };
                return;
            }
            unsafe { attend_avx2(page, dim, t, &wrep, lv, out) };
            return;
        }
    }
    let _ = path;
    out[..dim].fill(0);
    attend_scalar(page, dim, t, w8, lv, out);
}

/// `wrep[tq * 64 + r * 4 + tb] = w8[tq * 4 + tb]`: one token quad's four weights repeated 16
/// times, which is the u8 operand a `vpdpbusd` over "16 coordinates x 4 tokens" wants.
///
/// All eight quads are written unconditionally (`w8` is zero past `valid`, and past `T` the
/// kernels never read them) so that both loop bounds are constants: LLVM then turns the inner
/// one into a broadcast and a handful of wide stores instead of 64 bounds-checked 4-byte ones.
#[inline]
fn repeat_quads(w8: &[u8; 32]) -> [u32; 8 * 16] {
    let mut out = [0u32; 8 * 16];
    for tq in 0..8 {
        let q = u32::from_le_bytes([w8[4 * tq], w8[4 * tq + 1], w8[4 * tq + 2], w8[4 * tq + 3]]);
        for r in 0..16 {
            out[tq * 16 + r] = q;
        }
    }
    out
}

/// The reference: walk the V half through `pages::v_index`.
///
/// Unlike `k_index` this one has no division by a runtime value (`i / 16`, `tok / 4` and the
/// rest are all by constants), so the contract's own index function is the loop body.
fn attend_scalar(page: &[u8], dim: usize, t: usize, w8: &[u8; 32], lv: &VLevels, out: &mut [i32]) {
    let v = &page[PAGE / 2..];
    for tok in 0..t {
        let w = w8[tok] as i32;
        if w == 0 {
            continue;
        }
        for i in 0..dim {
            let (byte, sh) = v_index(dim, t, tok, i);
            out[i] += w * lv.levels_i8[((v[byte] >> sh) & 0x0f) as usize] as i32;
        }
    }
}

/// Coordinate runs processed together, so that four `vpdpbusd` chains are in flight: the
/// accumulator of one run is only `2 * (T/8)` deep, which at T = 16 is four dependent 5-cycle
/// instructions -- the same latency problem [`nsub_for`] solves on the score side.
#[cfg(target_arch = "x86_64")]
const CBU: usize = 4;

#[cfg(target_arch = "x86_64")]
#[target_feature(
    enable = "avx512f",
    enable = "avx512bw",
    enable = "avx512vl",
    enable = "avx512vnni"
)]
#[allow(clippy::needless_range_loop)]
unsafe fn attend_avx512(
    page: &[u8],
    dim: usize,
    t: usize,
    wrep: &[u32; 8 * 16],
    lv: &VLevels,
    out: &mut [i32],
) {
    use std::arch::x86_64::*;
    let pairs = t / 8; // TQ/2 blocks per coordinate run
    let runs = dim / 16; // 16 coordinates per accumulator
    let table = _mm512_broadcast_i32x4(_mm_loadu_si128(lv.levels_i8.as_ptr() as *const __m128i));
    let m0f = _mm512_set1_epi8(0x0f);
    let vbase = page.as_ptr().add(PAGE / 2);
    let wp = wrep.as_ptr() as *const u8;
    let op = out.as_mut_ptr();
    let mut r = 0;
    while r < runs {
        // `CBU` is const so these loops unroll and every `a[k]` / `b[k]` index is a constant,
        // which is what keeps the eight accumulators in registers instead of on the stack.
        let mut a = [_mm512_setzero_si512(); CBU];
        let mut b = [_mm512_setzero_si512(); CBU];
        for pair in 0..pairs {
            // low nibbles are token quad 2*pair, high nibbles token quad 2*pair + 1
            let wlo = _mm512_loadu_si512(wp.add(2 * pair * 64) as *const __m512i);
            let whi = _mm512_loadu_si512(wp.add((2 * pair + 1) * 64) as *const __m512i);
            for k in 0..CBU {
                let code =
                    _mm512_loadu_si512(vbase.add(((r + k) * pairs + pair) * 64) as *const __m512i);
                let lo = _mm512_shuffle_epi8(table, _mm512_and_si512(code, m0f));
                let hi =
                    _mm512_shuffle_epi8(table, _mm512_and_si512(_mm512_srli_epi16::<4>(code), m0f));
                a[k] = _mm512_dpbusd_epi32(a[k], wlo, lo);
                b[k] = _mm512_dpbusd_epi32(b[k], whi, hi);
            }
        }
        for k in 0..CBU {
            _mm512_storeu_epi32(op.add((r + k) * 16), _mm512_add_epi32(a[k], b[k]));
        }
        r += CBU;
    }
}

/// AVX2: 8 coordinates per ymm, so a 64-byte block is two halves.
///
/// `vpmaddubsw`'s i16 is why the weights are on a 7-bit grid here ([`weight_grid`]): with
/// `|level| <= 127` a pair sums to at most `2 * 127 * 127 = 32258 < 32767`. The accumulation
/// chain is only the `vpaddd`, so unlike the AVX-512 path this one needs no unrolling.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[allow(clippy::needless_range_loop)]
unsafe fn attend_avx2(
    page: &[u8],
    dim: usize,
    t: usize,
    wrep: &[u32; 8 * 16],
    lv: &VLevels,
    out: &mut [i32],
) {
    use std::arch::x86_64::*;
    let pairs = t / 8;
    let runs = dim / 16;
    let table =
        _mm256_broadcastsi128_si256(_mm_loadu_si128(lv.levels_i8.as_ptr() as *const __m128i));
    let ones = _mm256_set1_epi16(1);
    let m0f = _mm256_set1_epi8(0x0f);
    let vbase = page.as_ptr().add(PAGE / 2);
    let wp = wrep.as_ptr() as *const u8;
    let op = out.as_mut_ptr();
    for r in 0..runs {
        let mut a0 = _mm256_setzero_si256();
        let mut a1 = _mm256_setzero_si256();
        for pair in 0..pairs {
            let wlo = _mm256_loadu_si256(wp.add(2 * pair * 64) as *const __m256i);
            let whi = _mm256_loadu_si256(wp.add((2 * pair + 1) * 64) as *const __m256i);
            let blk = vbase.add((r * pairs + pair) * 64);
            for h in 0..2 {
                let code = _mm256_loadu_si256(blk.add(h * 32) as *const __m256i);
                let lo = _mm256_shuffle_epi8(table, _mm256_and_si256(code, m0f));
                let hi =
                    _mm256_shuffle_epi8(table, _mm256_and_si256(_mm256_srli_epi16::<4>(code), m0f));
                let s = _mm256_add_epi32(
                    _mm256_madd_epi16(_mm256_maddubs_epi16(wlo, lo), ones),
                    _mm256_madd_epi16(_mm256_maddubs_epi16(whi, hi), ones),
                );
                if h == 0 {
                    a0 = _mm256_add_epi32(a0, s);
                } else {
                    a1 = _mm256_add_epi32(a1, s);
                }
            }
        }
        _mm256_storeu_si256(op.add(r * 16) as *mut __m256i, a0);
        _mm256_storeu_si256(op.add(r * 16 + 8) as *mut __m256i, a1);
    }
}

// ---------------------------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::SplitMix64;
    use crate::kernel::available_paths;
    use crate::pages::code_at;
    use crate::tq4::rotation_seed;

    /// A page of random nibbles, written through the contract's own index functions, plus the
    /// `[tok][i]` code matrices a reference can read directly. Padding slots stay zero.
    fn random_page(
        rng: &mut SplitMix64,
        dim: usize,
        t: usize,
        valid: usize,
    ) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let mut page = vec![0u8; PAGE];
        let mut kc = vec![0u8; t * dim];
        let mut vc = vec![0u8; t * dim];
        for tok in 0..valid {
            for i in 0..dim {
                let (a, b) = ((rng.next_u64() & 0xf) as u8, (rng.next_u64() & 0xf) as u8);
                kc[tok * dim + i] = a;
                vc[tok * dim + i] = b;
                let (byte, sh) = k_index(dim, t, tok, i);
                page[byte] |= a << sh;
                let (byte, sh) = v_index(dim, t, tok, i);
                page[PAGE / 2 + byte] |= b << sh;
            }
        }
        // the page really is what `code_at` says it is
        for tok in 0..valid {
            for i in 0..dim {
                assert_eq!(code_at(&page, true, dim, t, tok, i), kc[tok * dim + i]);
                assert_eq!(code_at(&page, false, dim, t, tok, i), vc[tok * dim + i]);
            }
        }
        (page, kc, vc)
    }

    fn rand_vec(rng: &mut SplitMix64, n: usize) -> Vec<f32> {
        (0..n)
            .map(|_| (rng.next_f64() as f32 - 0.5) * 4.0)
            .collect()
    }

    /// Random pages and random queries: every available path returns the SAME int32
    /// accumulators. The query is prepared for the narrowest grid of the paths under test, so
    /// that the AVX2 kernel's i16 intermediate cannot saturate (see [`weight_grid`]).
    #[test]
    fn score_paths_agree_bit_exactly() {
        for (dim, t) in [(256usize, 16usize), (128, 32)] {
            let rot = Rotation::new(rotation_seed("pagekern"), dim);
            let mut rng = SplitMix64::new(3 + dim as u64);
            let (page, _, _) = random_page(&mut rng, dim, t, t);
            let cent = rand_vec(&mut rng, dim);
            let paths = available_paths();
            for nq in [1usize, 4] {
                // one grid per path, every kernel run over it
                for grid in &paths {
                    let queries: Vec<PageQuery> = (0..nq)
                        .map(|_| {
                            let q = rand_vec(&mut rng, dim);
                            PageQuery::with_path(&q, &cent, &rot, *grid)
                        })
                        .collect();
                    let refs: Vec<&PageQuery> = queries.iter().collect();
                    let mut want = [[0i32; 32]; MAXQ];
                    score_page_acc(&page, dim, t, &refs, Path::Scalar, &mut want[..nq]);
                    for p in &paths {
                        // a wider grid than this path can take would saturate its i16s
                        if p.q_max() < grid.q_max() {
                            continue;
                        }
                        let mut got = [[0i32; 32]; MAXQ];
                        score_page_acc(&page, dim, t, &refs, *p, &mut got[..nq]);
                        for qi in 0..nq {
                            assert_eq!(
                                got[qi][..t],
                                want[qi][..t],
                                "d {dim} T {t} nq {nq} grid {} path {}",
                                grid.name(),
                                p.name()
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn attend_paths_agree_bit_exactly() {
        let lv = VLevels::new();
        for (dim, t) in [(256usize, 16usize), (128, 32)] {
            let mut rng = SplitMix64::new(101 + dim as u64);
            let (page, _, _) = random_page(&mut rng, dim, t, t);
            let paths = available_paths();
            for bits in [WeightBits::Eight, WeightBits::Sixteen] {
                for grid in &paths {
                    let (gmax, shift, _) = weight_grid(bits, *grid);
                    let mut w8 = [0u8; 32];
                    for tok in 0..t {
                        let v = (rng.next_f64() * gmax as f64) as u32;
                        w8[tok] = (v >> shift) as u8;
                    }
                    let mut want = vec![0i32; dim];
                    attend_page_acc(&page, dim, t, &w8, &lv, Path::Scalar, &mut want);
                    for p in &paths {
                        let (pmax, _, _) = weight_grid(bits, *p);
                        if pmax < gmax {
                            continue; // this path's i16 could saturate on the wider grid
                        }
                        let mut got = vec![0i32; dim];
                        attend_page_acc(&page, dim, t, &w8, &lv, *p, &mut got);
                        assert_eq!(
                            got,
                            want,
                            "d {dim} T {t} {bits:?} grid {} path {}",
                            grid.name(),
                            p.name()
                        );
                    }
                }
            }
        }
    }

    /// `score_page` against the unquantised f32 score of PAGES.md section 2, on every path.
    #[test]
    fn score_page_tracks_the_f32_reference() {
        for (dim, t) in [(256usize, 16usize), (128, 32)] {
            let rot = Rotation::new(rotation_seed("pagescore"), dim);
            let mut rng = SplitMix64::new(17 + dim as u64);
            let valid = t - 3;
            let (page, kc, _) = random_page(&mut rng, dim, t, valid);
            let cent = rand_vec(&mut rng, dim);
            let scale_k: Vec<f32> = (0..t).map(|_| 0.5 + rng.next_f64() as f32).collect();
            let shift_k: Vec<f32> = (0..t).map(|_| rng.next_f64() as f32 - 0.5).collect();
            for nq in [1usize, 4] {
                let qv: Vec<Vec<f32>> = (0..nq).map(|_| rand_vec(&mut rng, dim)).collect();
                for p in available_paths() {
                    let queries: Vec<PageQuery> = qv
                        .iter()
                        .map(|q| PageQuery::with_path(q, &cent, &rot, p))
                        .collect();
                    let refs: Vec<&PageQuery> = queries.iter().collect();
                    let mut out = [[0.0f32; 32]; MAXQ];
                    score_page_with_path(
                        &page,
                        dim,
                        t,
                        valid,
                        &scale_k,
                        &shift_k,
                        &refs,
                        p,
                        &mut out[..nq],
                    );
                    for (qi, q) in qv.iter().enumerate() {
                        let mut qr = vec![0.0f32; dim];
                        rot.apply(q, &mut qr);
                        let qc: f32 = q.iter().zip(&cent).map(|(a, b)| a * b).sum();
                        let sq: f32 = qr.iter().sum();
                        // The error is relative to the ONLY approximated term, `scale * sum_i
                        // qr_i CB[code_i]` -- `qc` and `shift * sq` are exact f32. Both the
                        // error and the magnitude are RMS over the page's tokens, so the
                        // number below is a plain relative error rather than a worst case
                        // divided by a typical scale.
                        let (mut se, mut sm, mut worst) = (0.0f64, 0.0f64, 0.0f64);
                        for tok in 0..valid {
                            let resid: f64 = (0..dim)
                                .map(|i| {
                                    qr[i] as f64 * CODEBOOK4[kc[tok * dim + i] as usize] as f64
                                })
                                .sum();
                            let term = scale_k[tok] as f64 * resid;
                            let want = qc as f64 + term + shift_k[tok] as f64 * sq as f64;
                            let e = out[qi][tok] as f64 - want;
                            se += e * e;
                            sm += term * term;
                            worst = worst.max(e.abs());
                        }
                        let (rel, mag) = ((se / sm).sqrt(), (sm / valid as f64).sqrt());
                        println!(
                            "score d {dim} T {t} {} nq {nq} q {qi}: rel rms {rel:.2e}, \
                             worst/rms {:.2e}",
                            p.name(),
                            worst / mag
                        );
                        // the int8 query grid is the whole error budget: 1/127 on AVX-512 and
                        // AVX-512's grid, 1/63 on AVX2 (`Path::q_max`), on top of the 1/127
                        // codebook grid both share
                        assert!(
                            rel < 3e-2,
                            "d {dim} path {} q {qi}: rel rms {rel:.3e}",
                            p.name()
                        );
                        assert!(
                            worst / mag < 8e-2,
                            "d {dim} path {} q {qi}: worst {worst:.4} vs rms magnitude {mag:.4}",
                            p.name()
                        );
                        for tok in valid..t {
                            assert_eq!(out[qi][tok], f32::NEG_INFINITY, "padding slot {tok}");
                        }
                    }
                }
            }
        }
    }

    /// `attend_page` against `sum_t w_t * scale_v_t * CB[code]`, plus the two exact scalars.
    ///
    /// Two references: the unquantised codebook (which the int8 LEVELS alone cost ~1 % of) and
    /// the same sum with the quantised levels, which isolates the weight grid and is where
    /// `Sixteen` visibly beats `Eight`.
    #[test]
    fn attend_page_tracks_the_f32_reference() {
        let lv = VLevels::new();
        for (dim, t) in [(256usize, 16usize), (128, 32)] {
            let mut rng = SplitMix64::new(29 + dim as u64);
            let valid = t - 3;
            let (page, _, vc) = random_page(&mut rng, dim, t, valid);
            let scale_v: Vec<f32> = (0..t).map(|_| 0.5 + rng.next_f64() as f32).collect();
            let shift_v: Vec<f32> = (0..t).map(|_| rng.next_f64() as f32 - 0.5).collect();
            let mut w = [0.0f32; 32];
            for tok in 0..valid {
                // a softmax-shaped spread: most of the mass on a few tokens
                w[tok] = (-4.0 * rng.next_f64() as f32).exp();
            }
            let mut exact = vec![0.0f64; dim];
            let mut onlevels = vec![0.0f64; dim];
            let (mut zw, mut sw) = (0.0f64, 0.0f64);
            for tok in 0..valid {
                let wp = (w[tok] * scale_v[tok]) as f64;
                zw += w[tok] as f64;
                sw += (w[tok] as f64) * (shift_v[tok] as f64);
                for i in 0..dim {
                    let c = vc[tok * dim + i] as usize;
                    exact[i] += wp * CODEBOOK4[c] as f64;
                    onlevels[i] += wp * (lv.levels_i8[c] as f64 * lv.ls as f64);
                }
            }
            let norm = |v: &[f64]| v.iter().map(|x| x * x).sum::<f64>().sqrt();
            let (ne, nl) = (norm(&exact), norm(&onlevels));
            for p in available_paths() {
                for bits in [WeightBits::Eight, WeightBits::Sixteen] {
                    let mut acc = VAcc::new(dim);
                    attend_page_with_path(
                        &page, dim, t, valid, &scale_v, &shift_v, &w, &lv, bits, p, &mut acc,
                    );
                    let de: f64 = (0..dim)
                        .map(|i| (acc.num_rot[i] as f64 - exact[i]).powi(2))
                        .sum::<f64>()
                        .sqrt();
                    let dl: f64 = (0..dim)
                        .map(|i| (acc.num_rot[i] as f64 - onlevels[i]).powi(2))
                        .sum::<f64>()
                        .sqrt();
                    println!(
                        "attend d {dim} T {t} {} {bits:?}: rel vs CB {:.2e}, vs int8 levels \
                         {:.2e}",
                        p.name(),
                        de / ne,
                        dl / nl
                    );
                    assert!(
                        de / ne < 3e-2,
                        "{} {bits:?}: rel err vs CB {:.3e}",
                        p.name(),
                        de / ne
                    );
                    let want_l = match bits {
                        WeightBits::Eight => 1e-2,
                        WeightBits::Sixteen => 1e-4,
                    };
                    assert!(
                        dl / nl < want_l,
                        "{} {bits:?}: rel err vs the int8 levels {:.3e} (want < {want_l:.0e})",
                        p.name(),
                        dl / nl
                    );
                    assert!(
                        (acc.z - zw).abs() <= 1e-12 * zw.abs().max(1e-12),
                        "z {} vs {zw}",
                        acc.z
                    );
                    assert!(
                        (acc.shift_sum as f64 - sw).abs() <= 1e-6 * sw.abs().max(1e-6),
                        "shift_sum {} vs {sw}",
                        acc.shift_sum
                    );
                }
            }
        }
    }

    /// The f32 epilogues are hand-written per path too, so check that the *scores* -- not just
    /// the integer accumulators -- come out bit-identical: the AVX-512 epilogue deliberately
    /// avoids `fmadd` and adds `zero` in f32 so that it matches the scalar one exactly.
    #[test]
    fn score_epilogue_is_bit_identical_across_paths() {
        for (dim, t) in [(256usize, 16usize), (128, 32)] {
            let rot = Rotation::new(rotation_seed("pageepi"), dim);
            let mut rng = SplitMix64::new(5 + dim as u64);
            for valid in [t, t - 3, 1] {
                let (page, _, _) = random_page(&mut rng, dim, t, valid);
                let cent = rand_vec(&mut rng, dim);
                let scale_k: Vec<f32> = (0..valid).map(|_| 0.5 + rng.next_f64() as f32).collect();
                let shift_k: Vec<f32> = (0..valid).map(|_| rng.next_f64() as f32 - 0.5).collect();
                let qv: Vec<Vec<f32>> = (0..MAXQ).map(|_| rand_vec(&mut rng, dim)).collect();
                // one grid (the narrowest) so every path may run it
                let queries: Vec<PageQuery> = qv
                    .iter()
                    .map(|q| PageQuery::with_path(q, &cent, &rot, Path::Avx2))
                    .collect();
                let refs: Vec<&PageQuery> = queries.iter().collect();
                let mut want = [[0.0f32; 32]; MAXQ];
                score_page_with_path(
                    &page,
                    dim,
                    t,
                    valid,
                    &scale_k,
                    &shift_k,
                    &refs,
                    Path::Scalar,
                    &mut want,
                );
                for p in available_paths() {
                    let mut got = [[0.0f32; 32]; MAXQ];
                    score_page_with_path(
                        &page, dim, t, valid, &scale_k, &shift_k, &refs, p, &mut got,
                    );
                    for qi in 0..MAXQ {
                        for tok in 0..t {
                            assert_eq!(
                                got[qi][tok].to_bits(),
                                want[qi][tok].to_bits(),
                                "d {dim} valid {valid} path {} q {qi} token {tok}: {} vs {}",
                                p.name(),
                                got[qi][tok],
                                want[qi][tok]
                            );
                        }
                    }
                }
            }
        }
    }

    /// The weight grid is derived per path, so the AVX-512 prologue and the scalar one must
    /// produce the SAME `w8` bytes (this is what `round_ties_even` is for) and the same sums.
    #[test]
    #[cfg(target_arch = "x86_64")]
    fn attend_prologue_agrees_with_the_scalar_one() {
        if !available_paths().contains(&Path::Avx512) {
            return;
        }
        let mut rng = SplitMix64::new(4242);
        for t in [16usize, 32] {
            for valid in [t, t - 3, 5] {
                let scale_v: Vec<f32> = (0..valid).map(|_| 0.5 + rng.next_f64() as f32).collect();
                let shift_v: Vec<f32> = (0..valid).map(|_| rng.next_f64() as f32 - 0.5).collect();
                let mut w = [0.0f32; 32];
                for x in w.iter_mut().take(valid) {
                    // include exact ties on the weight grid, which is where the two rounding
                    // modes could disagree
                    *x = if rng.next_u64() & 7 == 0 {
                        (2 * (rng.next_u64() % 128) + 1) as f32 / 510.0
                    } else {
                        (-4.0 * rng.next_f64() as f32).exp()
                    };
                }
                let (mut wa, mut wb) = ([0.0f32; 32], [0.0f32; 32]);
                let (za, sa, ma) = attend_prep_scalar(valid, &scale_v, &shift_v, &w, &mut wa);
                let (zb, sb, mb) =
                    unsafe { attend_prep_avx512(valid, &scale_v, &shift_v, &w, &mut wb) };
                assert_eq!(ma.to_bits(), mb.to_bits(), "T {t} valid {valid}: page max");
                assert!(
                    (za - zb).abs() <= 1e-14 * za.abs().max(1e-14),
                    "z {za} vs {zb}"
                );
                assert!(
                    (sa - sb).abs() <= 1e-14 * sa.abs().max(1e-14),
                    "s {sa} vs {sb}"
                );
                assert_eq!(wa[..valid], wb[..valid], "T {t} valid {valid}: w'");
                for bits in [WeightBits::Eight, WeightBits::Sixteen] {
                    let (gmax, shift, _) = weight_grid(bits, Path::Avx512);
                    let inv = gmax as f32 / ma;
                    let (mut ha, mut la) = ([0u8; 32], [0u8; 32]);
                    let mask = if shift == 0 { 0 } else { (1u32 << shift) - 1 };
                    for tok in 0..valid {
                        let v = (wa[tok] * inv).round_ties_even().clamp(0.0, gmax as f32) as u32;
                        ha[tok] = (v >> shift) as u8;
                        la[tok] = (v & mask) as u8;
                    }
                    let (mut hb, mut lb) = ([0u8; 32], [0u8; 32]);
                    unsafe { attend_quant_avx512(valid, &wb, inv, gmax, shift, &mut hb, &mut lb) };
                    assert_eq!(ha, hb, "T {t} valid {valid} {bits:?}: hi bytes");
                    assert_eq!(la, lb, "T {t} valid {valid} {bits:?}: lo bytes");
                }
            }
        }
    }

    /// Weights that are all zero (a page none of whose tokens made it into U) add nothing, and
    /// tokens beyond `valid` never contribute.
    #[test]
    fn attend_ignores_empty_and_padding_slots() {
        let lv = VLevels::new();
        let (dim, t) = (256usize, 16usize);
        let mut rng = SplitMix64::new(77);
        let valid = t - 5;
        let (page, _, _) = random_page(&mut rng, dim, t, valid);
        let scale_v: Vec<f32> = (0..t).map(|_| 0.5 + rng.next_f64() as f32).collect();
        let shift_v: Vec<f32> = (0..t).map(|_| rng.next_f64() as f32).collect();
        for p in available_paths() {
            let mut acc = VAcc::new(dim);
            let w = [0.0f32; 32];
            attend_page_with_path(
                &page,
                dim,
                t,
                valid,
                &scale_v,
                &shift_v,
                &w,
                &lv,
                WeightBits::Eight,
                p,
                &mut acc,
            );
            assert_eq!(acc.z, 0.0);
            assert_eq!(acc.shift_sum, 0.0);
            assert!(acc.num_rot.iter().all(|&v| v == 0.0), "{}", p.name());

            // one live token; the padding slots' garbage must not leak in
            let mut w = [0.0f32; 32];
            w[1] = 1.0;
            let mut acc = VAcc::new(dim);
            attend_page_with_path(
                &page,
                dim,
                t,
                valid,
                &scale_v,
                &shift_v,
                &w,
                &lv,
                WeightBits::Eight,
                p,
                &mut acc,
            );
            assert!((acc.z - 1.0).abs() < 1e-12);
            assert!((acc.shift_sum - shift_v[1]).abs() < 1e-6);
        }
    }
}
