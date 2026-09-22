use std::sync::OnceLock;

use crate::builder::search::bf16_to_f32;
use crate::builder::tq4::{Centroid, Cents, Rotation, CODEBOOK4};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Codec {
    #[default]
    Tq4,
}

impl Codec {
    pub const fn bits(self) -> usize {
        4
    }
    pub const fn levels(self) -> usize {
        1 << 4
    }
    pub const fn per_byte(self) -> usize {
        2
    }
    pub const fn code_bytes(self, n: usize) -> usize {
        n / 2
    }
    pub fn codebook(self) -> &'static [f32] {
        &CODEBOOK4
    }
    pub fn thresholds(self) -> Vec<f32> {
        let cb = self.codebook();
        (0..cb.len() - 1)
            .map(|i| (cb[i] + cb[i + 1]) / 2.0)
            .collect()
    }
    #[inline]
    pub fn quantise(self, z: f32) -> u8 {
        crate::builder::tq4::quantise(z)
    }
    pub const fn default_rescore(self) -> usize {
        256
    }
    pub const fn name(self) -> &'static str {
        "tq4"
    }
    pub fn parse(s: &str) -> Option<Codec> {
        match s {
            "tq4" => Some(Codec::Tq4),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// runtime dispatch
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Path {
    Scalar,
    Avx2,
    Avx512,
}

impl Path {
    pub const fn q_max(self) -> f32 {
        match self {
            Path::Avx2 => 63.0,
            _ => 127.0,
        }
    }
    pub const fn name(self) -> &'static str {
        match self {
            Path::Scalar => "scalar",
            Path::Avx2 => "avx2",
            Path::Avx512 => "avx512-vnni",
        }
    }
}

fn detect_path() -> Path {
    let forced = std::env::var("KVSTORE_KERNEL").unwrap_or_default();
    let have512 = cfg!(target_arch = "x86_64")
        && is_x86_feature_detected!("avx512f")
        && is_x86_feature_detected!("avx512bw")
        && is_x86_feature_detected!("avx512vl")
        && is_x86_feature_detected!("avx512vnni")
        && is_x86_feature_detected!("f16c");
    let have2 = cfg!(target_arch = "x86_64")
        && is_x86_feature_detected!("avx2")
        && is_x86_feature_detected!("fma")
        && is_x86_feature_detected!("f16c");
    match forced.as_str() {
        "scalar" => Path::Scalar,
        "avx2" if have2 => Path::Avx2,
        "avx512" if have512 => Path::Avx512,
        _ if have512 => Path::Avx512,
        _ if have2 => Path::Avx2,
        _ => Path::Scalar,
    }
}

pub fn path() -> Path {
    static P: OnceLock<Path> = OnceLock::new();
    *P.get_or_init(detect_path)
}

pub fn available_paths() -> Vec<Path> {
    let mut v = vec![Path::Scalar];
    let saved = std::env::var("KVSTORE_KERNEL").ok();
    // detection is cheap and side-effect free; probe with the env var out of the way
    std::env::remove_var("KVSTORE_KERNEL");
    let best = detect_path();
    if let Some(s) = saved {
        std::env::set_var("KVSTORE_KERNEL", s);
    }
    if best == Path::Avx512 {
        v.push(Path::Avx2);
        v.push(Path::Avx512);
    } else if best == Path::Avx2 {
        v.push(Path::Avx2);
    }
    v
}

// ---------------------------------------------------------------------------------------------
// per-query state
// ---------------------------------------------------------------------------------------------

pub struct KeyQuery {
    pub codec: Codec,
    pub dim: usize,
    pub path: Path,
    pub levels: [u8; 16],
    pub w_lo: Vec<i8>,
    pub w_hi: Vec<i8>,
    pub w_nat: Vec<i8>,
    pub zero: i32,
    pub scale: f32,
    pub q: Vec<f32>,
}

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

impl KeyQuery {
    pub fn new(q: &[f32], rot: &Rotation, codec: Codec) -> KeyQuery {
        KeyQuery::with_path(q, rot, codec, path())
    }

    pub fn with_path(q: &[f32], rot: &Rotation, codec: Codec, path: Path) -> KeyQuery {
        let dim = rot.dim;
        assert_eq!(q.len(), dim, "query length must be head_dim");
        let mut rq = vec![0.0f32; dim];
        rot.apply(q, &mut rq);
        KeyQuery::from_rotated(q, &rq, codec, path, dim)
    }

    pub fn from_rotated(q: &[f32], rq: &[f32], codec: Codec, path: Path, dim: usize) -> KeyQuery {
        let lim = path.q_max();
        let cb = codec.codebook();
        let cmax = cb.iter().fold(0.0f32, |m, &c| m.max(c.abs()));
        let level_scale = if cmax > 0.0 { cmax / 127.0 } else { 1.0 };
        let mut levels = [0u8; 16];
        for v in 0..16 {
            let l = qi8(cb[v % cb.len()] / level_scale, 127.0);
            levels[v] = (l as i16 + 128) as u8;
        }
        let qmax = rq.iter().fold(0.0f32, |m, &v| m.max(v.abs()));
        let query_scale = if qmax > 0.0 { qmax / lim } else { 1.0 };
        let inv = 1.0 / query_scale;
        let w: Vec<i8> = rq.iter().map(|&v| qi8(v * inv, lim)).collect();
        let wsum: i32 = w.iter().map(|&v| v as i32).sum();

        let pad = |v: &mut Vec<i8>| {
            let want = v.len().next_multiple_of(64);
            v.resize(want, 0);
        };
        let (mut w_lo, mut w_hi) = (Vec::with_capacity(dim / 2), Vec::with_capacity(dim / 2));
        for g in 0..dim / 2 {
            w_lo.push(w[2 * g]);
            w_hi.push(w[2 * g + 1]);
        }
        pad(&mut w_lo);
        pad(&mut w_hi);
        let mut w_nat = w;
        pad(&mut w_nat);
        KeyQuery {
            codec,
            dim,
            path,
            levels,
            w_lo,
            w_hi,
            w_nat,
            zero: -128 * wsum,
            scale: level_scale * query_scale / (dim as f32).sqrt(),
            q: q.to_vec(),
        }
    }

    pub fn levels_i8(&self) -> [i8; 16] {
        let mut o = [0i8; 16];
        for (i, v) in o.iter_mut().enumerate() {
            *v = (self.levels[i] as i16 - 128) as i8;
        }
        o
    }
}

// ---------------------------------------------------------------------------------------------
// per-key kernel
// ---------------------------------------------------------------------------------------------

#[allow(clippy::needless_range_loop)]
pub fn dot_code_scalar(code: &[u8], kq: &KeyQuery) -> i32 {
    let lv = kq.levels_i8();
    let mut acc = 0i32;
    for (g, &b) in code.iter().enumerate() {
        acc += lv[(b & 0x0f) as usize] as i32 * kq.w_lo[g] as i32;
        acc += lv[(b >> 4) as usize] as i32 * kq.w_hi[g] as i32;
    }
    acc
}

#[allow(clippy::too_many_arguments)]
pub fn score_keys(
    codes: &[u8],
    stride: usize,
    norms: &[f32],
    block: usize,
    mean_dots: &[f32],
    kq: &KeyQuery,
    ids: &[u32],
    out: &mut [f32],
) {
    debug_assert_eq!(ids.len(), out.len());
    let shift = block.trailing_zeros();
    let pow2 = block.is_power_of_two();
    let mut accbuf = [0i32; MAX_BATCH];
    let mut done = 0;
    while done < ids.len() {
        let n = (ids.len() - done).min(MAX_BATCH);
        let ids = &ids[done..done + n];
        let acc = &mut accbuf[..n];
        match kq.path {
            #[cfg(target_arch = "x86_64")]
            Path::Avx512 => unsafe { dots_avx512(codes, stride, kq, ids, acc) },
            #[cfg(target_arch = "x86_64")]
            Path::Avx2 => unsafe { dots_avx2(codes, stride, kq, ids, acc) },
            _ => {
                let code_len = kq.codec.code_bytes(kq.dim);
                for (j, &id) in ids.iter().enumerate() {
                    let o = id as usize * stride;
                    acc[j] = dot_code_scalar(&codes[o..o + code_len], kq);
                }
            }
        }
        for (j, &id) in ids.iter().enumerate() {
            let id = id as usize;
            let b = if pow2 { id >> shift } else { id / block };
            let norm = unsafe { *norms.get_unchecked(id) };
            let md = unsafe { *mean_dots.get_unchecked(b) };
            out[done + j] = md + norm * ((acc[j] + kq.zero) as f32 * kq.scale);
        }
        done += n;
    }
}

pub fn dot_offsets(codes: &[u8], kq: &KeyQuery, offsets: &[u32], out: &mut [i32]) {
    assert_eq!(offsets.len(), out.len());
    assert!(offsets.iter().all(|&o| (o as usize)
        .checked_add(kq.dim / 2)
        .is_some_and(|end| end <= codes.len())));
    for (ids, dst) in offsets.chunks(MAX_BATCH).zip(out.chunks_mut(MAX_BATCH)) {
        match kq.path {
            #[cfg(target_arch = "x86_64")]
            Path::Avx512 => unsafe { dots_avx512(codes, 1, kq, ids, dst) },
            #[cfg(target_arch = "x86_64")]
            Path::Avx2 => unsafe { dots_avx2(codes, 1, kq, ids, dst) },
            _ => {
                for (&off, v) in ids.iter().zip(dst) {
                    *v = dot_code_scalar(&codes[off as usize..off as usize + kq.dim / 2], kq);
                }
            }
        }
    }
}

pub const MAX_BATCH: usize = 128;
const G: usize = 8;

#[inline(always)]
pub fn f16_to_f32(bits: u16) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if path() != Path::Scalar {
            // f16c is implied by both SIMD paths
            unsafe {
                use std::arch::x86_64::*;
                let v = _mm_cvtph_ps(_mm_cvtsi32_si128(bits as i32));
                return _mm_cvtss_f32(v);
            }
        }
    }
    half::f16::from_bits(bits).to_f32()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(
    enable = "avx512f",
    enable = "avx512bw",
    enable = "avx512vl",
    enable = "avx512vnni"
)]
#[allow(clippy::needless_range_loop)]
unsafe fn dots_avx512(codes: &[u8], stride: usize, kq: &KeyQuery, ids: &[u32], out: &mut [i32]) {
    use std::arch::x86_64::*;
    let levels = _mm512_broadcast_i32x4(_mm_loadu_si128(kq.levels.as_ptr() as *const __m128i));
    let m0f = _mm512_set1_epi8(0x0f);
    let base = codes.as_ptr();
    // `stride` is the record PITCH; the code itself is `code_len` bytes (a record may be padded
    // or carry other per-key fields, see the stride sweep in `kernel-bench`).
    let code_len = kq.codec.code_bytes(kq.dim);
    let chunks = code_len.div_ceil(64);
    let tail = code_len - (chunks - 1) * 64;
    let tail_mask: __mmask64 = if tail >= 64 { !0 } else { (1u64 << tail) - 1 };
    let n = ids.len();
    let mut i = 0;
    while i < n {
        let g = (n - i).min(G);
        let mut acc = [_mm512_setzero_si512(); G];
        for c in 0..chunks {
            let wl = _mm512_loadu_si512(kq.w_lo.as_ptr().add(c * 64) as *const __m512i);
            let wh = _mm512_loadu_si512(kq.w_hi.as_ptr().add(c * 64) as *const __m512i);
            for j in 0..g {
                let p = base.add(*ids.get_unchecked(i + j) as usize * stride + c * 64);
                let code = if c + 1 == chunks {
                    _mm512_maskz_loadu_epi8(tail_mask, p as *const i8)
                } else {
                    _mm512_loadu_si512(p as *const __m512i)
                };
                let lo = _mm512_shuffle_epi8(levels, _mm512_and_si512(code, m0f));
                let hi = _mm512_shuffle_epi8(
                    levels,
                    _mm512_and_si512(_mm512_srli_epi16::<4>(code), m0f),
                );
                acc[j] = _mm512_dpbusd_epi32(acc[j], lo, wl);
                acc[j] = _mm512_dpbusd_epi32(acc[j], hi, wh);
            }
        }
        for j in 0..g {
            *out.get_unchecked_mut(i + j) = _mm512_reduce_add_epi32(acc[j]);
        }
        i += g;
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2", enable = "fma")]
#[allow(clippy::needless_range_loop)]
unsafe fn dots_avx2(codes: &[u8], stride: usize, kq: &KeyQuery, ids: &[u32], out: &mut [i32]) {
    use std::arch::x86_64::*;
    let levels = _mm256_broadcastsi128_si256(_mm_loadu_si128(kq.levels.as_ptr() as *const __m128i));
    let ones = _mm256_set1_epi16(1);
    let base = codes.as_ptr();
    let code_len = kq.codec.code_bytes(kq.dim);
    let full = code_len / 32;
    let rest = code_len % 32;
    let n = ids.len();
    for (j, &id) in ids.iter().enumerate() {
        let _ = n;
        let off = id as usize * stride;
        let mut acc = _mm256_setzero_si256();
        let m0f = _mm256_set1_epi8(0x0f);
        for c in 0..full {
            let code = _mm256_loadu_si256(base.add(off + c * 32) as *const __m256i);
            let wl = _mm256_loadu_si256(kq.w_lo.as_ptr().add(c * 32) as *const __m256i);
            let wh = _mm256_loadu_si256(kq.w_hi.as_ptr().add(c * 32) as *const __m256i);
            let lo = _mm256_shuffle_epi8(levels, _mm256_and_si256(code, m0f));
            let hi =
                _mm256_shuffle_epi8(levels, _mm256_and_si256(_mm256_srli_epi16::<4>(code), m0f));
            acc = _mm256_add_epi32(acc, _mm256_madd_epi16(_mm256_maddubs_epi16(lo, wl), ones));
            acc = _mm256_add_epi32(acc, _mm256_madd_epi16(_mm256_maddubs_epi16(hi, wh), ones));
        }
        let mut t = [0i32; 8];
        _mm256_storeu_si256(t.as_mut_ptr() as *mut __m256i, acc);
        let mut s: i32 = t.iter().sum();
        // the tail (a stride that is not a multiple of 32 bytes) uses the BIASED levels too, so
        // that the returned accumulator is the same `sum (level + 128) * w` the AVX-512 path
        // returns and `score_keys` can cancel the bias once with `kq.zero`.
        for g in full * 32..full * 32 + rest {
            let b = *base.add(off + g);
            s += kq.levels[(b & 0x0f) as usize] as i32 * kq.w_lo[g] as i32;
            s += kq.levels[(b >> 4) as usize] as i32 * kq.w_hi[g] as i32;
        }
        out[j] = s;
    }
}

// ---------------------------------------------------------------------------------------------
// the block-mean term
// ---------------------------------------------------------------------------------------------

pub fn mean_dots(q: &[f32], c: &Cents) -> Vec<f32> {
    let mut out = vec![0.0f32; c.n_blocks];
    mean_dots_into(q, c, &mut out);
    out
}

pub fn mean_dots_into(q: &[f32], c: &Cents, out: &mut [f32]) {
    let (n, dim) = (c.n_blocks, c.dim);
    debug_assert!(out.len() >= n);
    debug_assert_eq!(c.bytes.len(), n * c.kind.bytes_per_block(dim));
    match c.kind {
        Centroid::F16 => {
            #[cfg(target_arch = "x86_64")]
            if path() != Path::Scalar && dim.is_multiple_of(8) {
                unsafe { return mean_dots_avx(q, c.bytes, dim, &mut out[..n]) };
            }
            for b in 0..n {
                let m = &c.bytes[b * dim * 2..(b + 1) * dim * 2];
                out[b] = (0..dim)
                    .map(|i| f16_to_f32(u16::from_le_bytes([m[2 * i], m[2 * i + 1]])) * q[i])
                    .sum();
            }
        }
        Centroid::I8 => cent_dots_i8(q, c.bytes, dim, &mut out[..n]),
    }
}

fn cent_dots_i8(q: &[f32], table: &[u8], dim: usize, out: &mut [f32]) {
    let qmax = q[..dim].iter().fold(0.0f32, |a, &v| a.max(v.abs()));
    if qmax <= 0.0 {
        out.fill(0.0);
        return;
    }
    let qs = qmax / 127.0;
    let inv = 1.0f32 / qs;
    // padded to a multiple of 64 so the wide paths can load the tail unmasked
    let mut w = vec![0i8; dim.next_multiple_of(64)];
    for i in 0..dim {
        w[i] = qi8(q[i] * inv, 127.0);
    }
    let wsum: i32 = w[..dim].iter().map(|&v| v as i32).sum();
    let row = dim + 2;
    // FOUR blocks per pass where the machine has VNNI. The arithmetic is unchanged -- the same
    // `vpdpbusd` chain per block, in the same order -- and what changes is the *reduction*: one
    // `_mm512_reduce_add_epi32` per block is a 5-instruction shuffle tree feeding a scalar, and
    // at `dim = 128` a block is only TWO `vpdpbusd`, so the pass spent more of itself folding
    // accumulators than multiplying. Night 6 R2 measured the int8 pass at 1.25x the fp16 one
    // where halving the bytes predicted 2x and named this as the reason ("what it pays for is
    // the per-block horizontal reduction, which halving the bytes does not touch"). Four
    // accumulators fold in one tree ([`hsum4_i32`]) and the four `vpdpbusd` chains are
    // independent, so they pipeline.
    #[cfg(target_arch = "x86_64")]
    if path() == Path::Avx512 && dim.is_multiple_of(64) && mean_dots4() {
        unsafe { cent_dots_i8_vnni4(table, dim, row, &w, wsum, qs, out) };
        return;
    }
    for (b, o) in out.iter_mut().enumerate() {
        let r = &table[b * row..b * row + row];
        let acc = dot_i8(&r[..dim], &w, wsum);
        let s = f16_to_f32(u16::from_le_bytes([r[dim], r[dim + 1]]));
        *o = acc as f32 * qs * s;
    }
}

pub fn mean_dots4() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("KVSTORE_NO_MEAN_DOTS4").is_err())
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,avx512f,avx512bw,avx512vnni")]
#[allow(clippy::too_many_arguments)]
unsafe fn cent_dots_i8_vnni4(
    table: &[u8],
    dim: usize,
    row: usize,
    w: &[i8],
    wsum: i32,
    qs: f32,
    out: &mut [f32],
) {
    use std::arch::x86_64::*;
    let bias = _mm512_set1_epi8(-128);
    let n = out.len();
    let base = table.as_ptr();
    let wp = w.as_ptr();
    let scale = |b: usize| -> f32 {
        let r = base.add(b * row + dim);
        f16_to_f32(u16::from_le_bytes([*r, *r.add(1)]))
    };
    let mut b = 0;
    while b + 4 <= n {
        let (mut a0, mut a1, mut a2, mut a3) = (
            _mm512_setzero_si512(),
            _mm512_setzero_si512(),
            _mm512_setzero_si512(),
            _mm512_setzero_si512(),
        );
        let (p0, p1, p2, p3) = (
            base.add(b * row),
            base.add((b + 1) * row),
            base.add((b + 2) * row),
            base.add((b + 3) * row),
        );
        let mut d = 0;
        while d + 64 <= dim {
            let x = _mm512_loadu_si512(wp.add(d) as *const __m512i);
            let ld = |p: *const u8| {
                _mm512_xor_si512(_mm512_loadu_si512(p.add(d) as *const __m512i), bias)
            };
            a0 = _mm512_dpbusd_epi32(a0, ld(p0), x);
            a1 = _mm512_dpbusd_epi32(a1, ld(p1), x);
            a2 = _mm512_dpbusd_epi32(a2, ld(p2), x);
            a3 = _mm512_dpbusd_epi32(a3, ld(p3), x);
            d += 64;
        }
        let sums = hsum4_i32(a0, a1, a2, a3);
        for (j, &s) in sums.iter().enumerate() {
            *out.get_unchecked_mut(b + j) = (s - 128 * wsum) as f32 * qs * scale(b + j);
        }
        b += 4;
    }
    while b < n {
        let r = &table[b * row..b * row + row];
        out[b] = dot_i8(&r[..dim], w, wsum) as f32 * qs * scale(b);
        b += 1;
    }
}

#[cfg(target_arch = "x86_64")]
#[inline]
#[target_feature(enable = "avx2,avx512f,avx512bw,avx512vnni")]
unsafe fn hsum4_i32(
    a0: std::arch::x86_64::__m512i,
    a1: std::arch::x86_64::__m512i,
    a2: std::arch::x86_64::__m512i,
    a3: std::arch::x86_64::__m512i,
) -> [i32; 4] {
    use std::arch::x86_64::*;
    // 512 -> 256 by folding the halves, then two rounds of `vphaddd`: after the first the low
    // lane holds pairs of a0 then pairs of a1, after the second it holds (sum a0, sum a1, sum a2,
    // sum a3) over the low 4 lanes of each 128-bit half, so one final add finishes all four.
    let fold =
        |a: __m512i| _mm256_add_epi32(_mm512_castsi512_si256(a), _mm512_extracti64x4_epi64::<1>(a));
    let h01 = _mm256_hadd_epi32(fold(a0), fold(a1));
    let h23 = _mm256_hadd_epi32(fold(a2), fold(a3));
    let h = _mm256_hadd_epi32(h01, h23);
    let r = _mm_add_epi32(_mm256_castsi256_si128(h), _mm256_extracti128_si256::<1>(h));
    let mut o = [0i32; 4];
    _mm_storeu_si128(o.as_mut_ptr() as *mut __m128i, r);
    o
}

#[inline]
fn dot_i8(cent: &[u8], w: &[i8], wsum: i32) -> i32 {
    #[cfg(target_arch = "x86_64")]
    if path() == Path::Avx512 && cent.len().is_multiple_of(64) {
        return unsafe { dot_i8_vnni(cent, w) } - 128 * wsum;
    }
    #[cfg(target_arch = "x86_64")]
    if path() == Path::Avx2 && cent.len().is_multiple_of(16) {
        return unsafe { dot_i8_avx2(cent, w) };
    }
    cent.iter()
        .zip(w)
        .map(|(&c, &x)| (c as i8) as i32 * x as i32)
        .sum()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(
    enable = "avx512f",
    enable = "avx512bw",
    enable = "avx512vl",
    enable = "avx512vnni"
)]
unsafe fn dot_i8_vnni(cent: &[u8], w: &[i8]) -> i32 {
    use std::arch::x86_64::*;
    let bias = _mm512_set1_epi8(-128); // 0x80 in every lane: XOR turns i8 into biased u8
    let mut acc = _mm512_setzero_si512();
    let mut d = 0;
    while d + 64 <= cent.len() {
        let c = _mm512_xor_si512(
            _mm512_loadu_si512(cent.as_ptr().add(d) as *const __m512i),
            bias,
        );
        let x = _mm512_loadu_si512(w.as_ptr().add(d) as *const __m512i);
        acc = _mm512_dpbusd_epi32(acc, c, x);
        d += 64;
    }
    _mm512_reduce_add_epi32(acc)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn dot_i8_avx2(cent: &[u8], w: &[i8]) -> i32 {
    use std::arch::x86_64::*;
    let mut acc = _mm256_setzero_si256();
    let mut d = 0;
    while d + 16 <= cent.len() {
        let c = _mm256_cvtepi8_epi16(_mm_loadu_si128(cent.as_ptr().add(d) as *const __m128i));
        let x = _mm256_cvtepi8_epi16(_mm_loadu_si128(w.as_ptr().add(d) as *const __m128i));
        acc = _mm256_add_epi32(acc, _mm256_madd_epi16(c, x));
        d += 16;
    }
    let lo = _mm256_castsi256_si128(acc);
    let hi = _mm256_extracti128_si256(acc, 1);
    let s = _mm_add_epi32(lo, hi);
    let s = _mm_hadd_epi32(s, s);
    let s = _mm_hadd_epi32(s, s);
    _mm_cvtsi128_si32(s)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2", enable = "fma", enable = "f16c")]
#[allow(clippy::needless_range_loop)]
unsafe fn mean_dots_avx(q: &[f32], means: &[u8], dim: usize, out: &mut [f32]) {
    use std::arch::x86_64::*;
    let mp = means.as_ptr();
    for (b, o) in out.iter_mut().enumerate() {
        let row = mp.add(b * dim * 2);
        let mut acc = _mm256_setzero_ps();
        let mut d = 0;
        while d + 8 <= dim {
            let h = _mm_loadu_si128(row.add(d * 2) as *const __m128i);
            acc = _mm256_fmadd_ps(_mm256_cvtph_ps(h), _mm256_loadu_ps(q.as_ptr().add(d)), acc);
            d += 8;
        }
        let hi = _mm256_extractf128_ps(acc, 1);
        let lo = _mm256_castps256_ps128(acc);
        let s = _mm_add_ps(lo, hi);
        let s = _mm_hadd_ps(s, s);
        let s = _mm_hadd_ps(s, s);
        let mut v = _mm_cvtss_f32(s);
        for dd in d..dim {
            v += f16_to_f32(*(row.add(dd * 2) as *const u16)) * q[dd];
        }
        *o = v;
    }
}

// ---------------------------------------------------------------------------------------------
// exact bf16 dot (the EXACT traversal and the rescoring pass)
// ---------------------------------------------------------------------------------------------

pub fn dot_bf16(a: &[u8], b: &[u8]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    match path() {
        Path::Avx512 => return unsafe { dot_bf16_avx512(a, b) },
        Path::Avx2 => return unsafe { dot_bf16_avx2(a, b) },
        Path::Scalar => {}
    }
    crate::builder::search::dot_bf16_bytes(a, b)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f", enable = "avx512bw")]
unsafe fn dot_bf16_avx512(a: &[u8], b: &[u8]) -> f32 {
    use std::arch::x86_64::*;
    let n = a.len() / 2;
    let (mut acc0, mut acc1) = (_mm512_setzero_ps(), _mm512_setzero_ps());
    let mut i = 0;
    while i + 32 <= n {
        for (k, acc) in [&mut acc0, &mut acc1].into_iter().enumerate() {
            let o = (i + k * 16) * 2;
            let av = _mm512_slli_epi32(
                _mm512_cvtepu16_epi32(_mm256_loadu_si256(a.as_ptr().add(o) as *const __m256i)),
                16,
            );
            let bv = _mm512_slli_epi32(
                _mm512_cvtepu16_epi32(_mm256_loadu_si256(b.as_ptr().add(o) as *const __m256i)),
                16,
            );
            *acc = _mm512_fmadd_ps(_mm512_castsi512_ps(av), _mm512_castsi512_ps(bv), *acc);
        }
        i += 32;
    }
    while i + 16 <= n {
        let o = i * 2;
        let av = _mm512_slli_epi32::<16>(_mm512_cvtepu16_epi32(_mm256_loadu_si256(
            a.as_ptr().add(o) as *const __m256i,
        )));
        let bv = _mm512_slli_epi32::<16>(_mm512_cvtepu16_epi32(_mm256_loadu_si256(
            b.as_ptr().add(o) as *const __m256i,
        )));
        acc0 = _mm512_fmadd_ps(_mm512_castsi512_ps(av), _mm512_castsi512_ps(bv), acc0);
        i += 16;
    }
    let mut s = _mm512_reduce_add_ps(_mm512_add_ps(acc0, acc1));
    for j in i..n {
        let x = bf16_to_f32(u16::from_le_bytes([a[2 * j], a[2 * j + 1]]));
        let y = bf16_to_f32(u16::from_le_bytes([b[2 * j], b[2 * j + 1]]));
        s += x * y;
    }
    s
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2", enable = "fma")]
unsafe fn dot_bf16_avx2(a: &[u8], b: &[u8]) -> f32 {
    use std::arch::x86_64::*;
    let n = a.len() / 2;
    let mut acc = _mm256_setzero_ps();
    let mut i = 0;
    while i + 8 <= n {
        let o = i * 2;
        let av = _mm256_slli_epi32::<16>(_mm256_cvtepu16_epi32(_mm_loadu_si128(
            a.as_ptr().add(o) as *const __m128i
        )));
        let bv = _mm256_slli_epi32::<16>(_mm256_cvtepu16_epi32(_mm_loadu_si128(
            b.as_ptr().add(o) as *const __m128i
        )));
        acc = _mm256_fmadd_ps(_mm256_castsi256_ps(av), _mm256_castsi256_ps(bv), acc);
        i += 8;
    }
    let hi = _mm256_extractf128_ps(acc, 1);
    let lo = _mm256_castps256_ps128(acc);
    let s4 = _mm_add_ps(lo, hi);
    let s4 = _mm_hadd_ps(s4, s4);
    let s4 = _mm_hadd_ps(s4, s4);
    let mut s = _mm_cvtss_f32(s4);
    for j in i..n {
        let x = bf16_to_f32(u16::from_le_bytes([a[2 * j], a[2 * j + 1]]));
        let y = bf16_to_f32(u16::from_le_bytes([b[2 * j], b[2 * j + 1]]));
        s += x * y;
    }
    s
}

#[inline(always)]
pub unsafe fn prefetch(p: *const u8, bytes: usize) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
        let mut o = 0;
        while o < bytes {
            _mm_prefetch(p.add(o) as *const i8, _MM_HINT_T0);
            o += 64;
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = (p, bytes);
}

// ---------------------------------------------------------------------------------------------
// block (FastScan-style) kernel
// ---------------------------------------------------------------------------------------------

pub const BLOCK: usize = 32;

pub fn to_block_layout(records: &[u8], stride: usize, first: usize, n: usize, out: &mut [u8]) {
    assert!(
        stride.is_multiple_of(4),
        "code stride must be a multiple of 4 bytes"
    );
    assert!(out.len() >= BLOCK * stride, "block buffer too small");
    let quads = stride / 4;
    let avail = n.saturating_sub(first).min(BLOCK);
    if avail < BLOCK {
        // only a short last block needs the padding slots zeroed
        out[..BLOCK * stride].fill(0);
    }
    if avail == 0 {
        return;
    }
    assert!(
        (first + avail) * stride <= records.len(),
        "records end before key {}",
        first + avail - 1
    );
    // SAFETY: `avail` keys starting at `first` are inside `records` (asserted), and every write
    // lands inside `out[..BLOCK * stride]` (asserted): `a < quads` and `t < BLOCK`, so the
    // largest offset is `(quads - 1) * BLOCK * 4 + (BLOCK - 1) * 4 + 4 = BLOCK * stride`.
    unsafe {
        let dst = out.as_mut_ptr();
        for t in 0..avail {
            let src = records.as_ptr().add((first + t) * stride);
            for a in 0..quads {
                let v = (src.add(a * 4) as *const u32).read_unaligned();
                (dst.add(a * (BLOCK * 4) + t * 4) as *mut u32).write_unaligned(v);
            }
        }
    }
}

pub fn block_scan(
    block: &[u8],
    stride: usize,
    norms: &[u16],
    mean_dot: f32,
    kq: &KeyQuery,
    out: &mut [f32],
) {
    debug_assert!(out.len() >= BLOCK);
    #[cfg(target_arch = "x86_64")]
    if kq.path == Path::Avx512 && stride.is_multiple_of(4) {
        unsafe {
            block_scan_avx512(block, stride, norms, mean_dot, kq, out);
        }
        return;
    }
    // portable reference: the per-key kernel over the same (permuted) bytes
    let quads = stride / 4;
    let mut rec = vec![0u8; stride];
    for t in 0..BLOCK {
        for a in 0..quads {
            rec[a * 4..a * 4 + 4]
                .copy_from_slice(&block[a * (BLOCK * 4) + t * 4..a * (BLOCK * 4) + t * 4 + 4]);
        }
        let acc = dot_code_scalar(&rec, kq);
        out[t] = mean_dot + f16_to_f32(norms[t]) * ((acc + kq.zero) as f32 * kq.scale);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(
    enable = "avx512f",
    enable = "avx512bw",
    enable = "avx512vl",
    enable = "avx512vnni"
)]
#[allow(clippy::needless_range_loop)]
unsafe fn block_scan_avx512(
    block: &[u8],
    stride: usize,
    norms: &[u16],
    mean_dot: f32,
    kq: &KeyQuery,
    out: &mut [f32],
) {
    use std::arch::x86_64::*;
    let levels = _mm512_broadcast_i32x4(_mm_loadu_si128(kq.levels.as_ptr() as *const __m128i));
    let m0f = _mm512_set1_epi8(0x0f);
    let quads = stride / 4;
    let p = block.as_ptr();
    let mut a0 = _mm512_setzero_si512();
    let mut a1 = _mm512_setzero_si512();
    for a in 0..quads {
        // the four code bytes of quad `a` carry coordinates 8a .. 8a+7:
        // byte 4a+e -> low nibble = coord 2(4a+e), high nibble = coord 2(4a+e)+1
        let wl = _mm512_set1_epi32((kq.w_lo.as_ptr().add(a * 4) as *const i32).read_unaligned());
        let wh = _mm512_set1_epi32((kq.w_hi.as_ptr().add(a * 4) as *const i32).read_unaligned());
        let c0 = _mm512_loadu_si512(p.add(a * 128) as *const __m512i);
        let c1 = _mm512_loadu_si512(p.add(a * 128 + 64) as *const __m512i);
        a0 = _mm512_dpbusd_epi32(
            a0,
            _mm512_shuffle_epi8(levels, _mm512_and_si512(c0, m0f)),
            wl,
        );
        a0 = _mm512_dpbusd_epi32(
            a0,
            _mm512_shuffle_epi8(levels, _mm512_and_si512(_mm512_srli_epi16::<4>(c0), m0f)),
            wh,
        );
        a1 = _mm512_dpbusd_epi32(
            a1,
            _mm512_shuffle_epi8(levels, _mm512_and_si512(c1, m0f)),
            wl,
        );
        a1 = _mm512_dpbusd_epi32(
            a1,
            _mm512_shuffle_epi8(levels, _mm512_and_si512(_mm512_srli_epi16::<4>(c1), m0f)),
            wh,
        );
    }
    let zero = _mm512_set1_epi32(kq.zero);
    let vs = _mm512_set1_ps(kq.scale);
    let vo = _mm512_set1_ps(mean_dot);
    let n0 = _mm512_cvtph_ps(_mm256_loadu_si256(norms.as_ptr() as *const __m256i));
    let n1 = _mm512_cvtph_ps(_mm256_loadu_si256(norms.as_ptr().add(16) as *const __m256i));
    let f0 = _mm512_mul_ps(_mm512_cvtepi32_ps(_mm512_add_epi32(a0, zero)), vs);
    let f1 = _mm512_mul_ps(_mm512_cvtepi32_ps(_mm512_add_epi32(a1, zero)), vs);
    _mm512_storeu_ps(out.as_mut_ptr(), _mm512_fmadd_ps(f0, n0, vo));
    _mm512_storeu_ps(out.as_mut_ptr().add(16), _mm512_fmadd_ps(f1, n1, vo));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::index::SplitMix64;
    use crate::builder::tq4::rotation_seed;

    fn rand_vec(rng: &mut SplitMix64, n: usize) -> Vec<f32> {
        (0..n)
            .map(|_| (rng.next_f64() as f32 - 0.5) * 4.0)
            .collect()
    }

    #[test]
    fn every_kernel_path_matches_the_scalar_reference() {
        for codec in [Codec::Tq4] {
            for dim in [128usize, 64, 256] {
                let stride = codec.code_bytes(dim);
                let mut rng = SplitMix64::new(7 + dim as u64);
                let nk = 37usize;
                let codes: Vec<u8> = (0..nk * stride)
                    .map(|_| (rng.next_u64() & 0xff) as u8)
                    .collect();
                let ids: Vec<u32> = (0..nk as u32).collect();
                let rot = Rotation::new(rotation_seed("kern"), dim);
                let q = rand_vec(&mut rng, dim);
                for p in available_paths() {
                    let kq = KeyQuery::with_path(&q, &rot, codec, p);
                    let mut got = vec![0i32; nk];
                    match p {
                        #[cfg(target_arch = "x86_64")]
                        Path::Avx512 => unsafe { dots_avx512(&codes, stride, &kq, &ids, &mut got) },
                        #[cfg(target_arch = "x86_64")]
                        Path::Avx2 => unsafe { dots_avx2(&codes, stride, &kq, &ids, &mut got) },
                        _ => {}
                    }
                    if p == Path::Scalar {
                        continue;
                    }
                    for k in 0..nk {
                        let want = dot_code_scalar(&codes[k * stride..(k + 1) * stride], &kq);
                        // the kernels return the +128-biased accumulator; `score_keys` cancels
                        // the bias once per key with `kq.zero`
                        assert_eq!(
                            got[k] + kq.zero,
                            want,
                            "{codec:?} dim {dim} path {} key {k}",
                            p.name()
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn score_keys_matches_the_dequantised_dot() {
        // build a real head, then compare the kernel against tq4::Lut / dequantise
        let (dim, block) = (128usize, 32usize);
        let rot = Rotation::new(rotation_seed("score"), dim);
        let mut rng = SplitMix64::new(11);
        let n = 200usize;
        let keys: Vec<f32> = (0..n * dim)
            .map(|i| (rng.next_f64() as f32 - 0.5) + (i / (dim * block)) as f32)
            .collect();
        let values = rand_vec(&mut rng, n * dim);
        for codec in [Codec::Tq4] {
            let head = crate::builder::tq4::TqHead::encode(&keys, &values, dim, block, &rot, codec);
            let recs = crate::builder::nodes::NodeRecords::from_head(&head);
            let q = rand_vec(&mut rng, dim);
            let lut = crate::builder::tq4::Lut::new(&q, &rot, codec);
            let md = mean_dots(&q, &head.cents());
            let ids: Vec<u32> = (0..n as u32).collect();
            let mut got = vec![0.0f32; n];
            recs.score(
                &md,
                &crate::builder::kernel::KeyQuery::new(&q, &rot, codec),
                &ids,
                &mut got,
            );
            let mut deq = vec![0.0f32; dim];
            let mut worst = 0.0f32;
            let mut worst_lut = 0.0f32;
            for pos in 0..n as u32 {
                head.dequantise(true, pos, &rot, &mut deq);
                let want: f32 = q.iter().zip(&deq).map(|(a, b)| a * b).sum();
                let lut_score = head.score_key(pos, &lut, md[pos as usize / block]);
                worst = worst.max((got[pos as usize] - want).abs());
                worst_lut = worst_lut.max((got[pos as usize] - lut_score).abs());
            }
            let mag: f32 = q.iter().map(|v| v * v).sum::<f32>().sqrt() * 30.0;
            assert!(
                worst < 0.01 * mag,
                "{codec:?}: kernel vs dequantised dot max abs err {worst} (scale {mag})"
            );
            assert!(
                worst_lut < 0.01 * mag,
                "{codec:?}: kernel vs f32 LUT score max abs err {worst_lut}"
            );
        }
    }

    #[test]
    fn int8_mean_dots_agree_across_paths_and_track_the_exact_dot() {
        let (dim, n) = (128usize, 300usize);
        let rot =
            crate::builder::tq4::Rotation::new(crate::builder::tq4::rotation_seed("i8md"), dim);
        let mut rng = crate::builder::index::SplitMix64::new(11);
        let keys: Vec<f32> = (0..n * dim)
            .map(|i| (i / dim) as f32 * 0.1 + (rng.next_f64() as f32 - 0.5) * 4.0)
            .collect();
        let values: Vec<f32> = (0..n * dim).map(|_| rng.next_f64() as f32 - 0.5).collect();
        let head = crate::builder::tq4::TqHead::encode_spec(
            &keys,
            &values,
            dim,
            32,
            &rot,
            Codec::Tq4,
            crate::builder::tq4::CentroidSpec {
                kind: Centroid::I8,
                iters: 3,
            },
        );
        let q = rand_vec(&mut rng, dim);
        let c = head.cents();
        // exact reference: widen the int8 table and dot in f32
        let mut cb = vec![0.0f32; dim];
        let exact: Vec<f32> = (0..c.n_blocks)
            .map(|b| {
                c.into_f32(b, &mut cb);
                q.iter().zip(&cb).map(|(a, v)| a * v).sum()
            })
            .collect();
        let got = mean_dots(&q, &c);
        // the only difference is the int8 query, whose step is `max|q| / 127`: the error of one
        // block's dot is bounded by half a step times the block's L1 norm, and that bound is
        // what is asserted (a relative bound would be wrong -- the dot itself cancels)
        let qs = q.iter().fold(0.0f32, |a, &v| a.max(v.abs())) / 127.0;
        for b in 0..c.n_blocks {
            c.into_f32(b, &mut cb);
            let l1: f32 = cb.iter().map(|v| v.abs()).sum();
            let bound = 0.5 * qs * l1 + 1e-3;
            assert!(
                (got[b] - exact[b]).abs() <= bound,
                "block {b}: int8 pass {} vs exact {} (bound {bound})",
                got[b],
                exact[b]
            );
        }
        // and the approximation is small against the scores themselves
        let rms = (exact.iter().map(|v| (v * v) as f64).sum::<f64>() / exact.len() as f64).sqrt();
        let err = (got
            .iter()
            .zip(&exact)
            .map(|(a, b)| ((a - b) * (a - b)) as f64)
            .sum::<f64>()
            / exact.len() as f64)
            .sqrt();
        assert!(err < 0.01 * rms, "int8 query costs {err} rms against {rms}");
        // and the three inner products do not disagree by a single integer: the accumulation is
        // exact in i32, so a session's scores do not depend on which machine served them
        let mut w = vec![0i8; dim.next_multiple_of(64)];
        for (i, &v) in q.iter().enumerate() {
            w[i] = qi8(v * 100.0, 127.0);
        }
        let wsum: i32 = w[..dim].iter().map(|&v| v as i32).sum();
        for b in 0..c.n_blocks {
            let row = &c.row(b)[..dim];
            let want: i32 = row
                .iter()
                .zip(&w)
                .map(|(&x, &y)| (x as i8) as i32 * y as i32)
                .sum();
            #[cfg(target_arch = "x86_64")]
            {
                if available_paths().contains(&Path::Avx512) {
                    assert_eq!(
                        unsafe { dot_i8_vnni(row, &w) } - 128 * wsum,
                        want,
                        "vnni {b}"
                    );
                }
                if available_paths().contains(&Path::Avx2) {
                    assert_eq!(unsafe { dot_i8_avx2(row, &w) }, want, "avx2 {b}");
                }
            }
            assert_eq!(dot_i8(row, &w, wsum), want, "dispatch {b}");
        }
    }

    #[test]
    #[cfg(target_arch = "x86_64")]
    fn int8_mean_dots_are_bit_identical_four_blocks_at_a_time() {
        if !(is_x86_feature_detected!("avx512f")
            && is_x86_feature_detected!("avx512bw")
            && is_x86_feature_detected!("avx512vnni"))
        {
            return;
        }
        let dim = 128usize;
        let mut rng = SplitMix64::new(4242);
        for nb in [1usize, 3, 4, 7, 16, 33] {
            let row = dim + 2;
            let table: Vec<u8> = (0..nb * row)
                .map(|i| {
                    if i % row >= dim {
                        // the fp16 per-block scale, kept in a sane range
                        half::f16::from_f32(0.5 + rng.next_f64() as f32).to_le_bytes()
                            [i % row - dim]
                    } else {
                        (rng.next_f64() * 255.0) as u8
                    }
                })
                .collect();
            let q = rand_vec(&mut rng, dim);
            let qmax = q.iter().fold(0.0f32, |a, &v| a.max(v.abs()));
            let qs = qmax / 127.0;
            let mut w = vec![0i8; dim.next_multiple_of(64)];
            for i in 0..dim {
                w[i] = qi8(q[i] / qs, 127.0);
            }
            let wsum: i32 = w[..dim].iter().map(|&v| v as i32).sum();
            // the reference: the per-block loop this replaced
            let want: Vec<f32> = (0..nb)
                .map(|b| {
                    let r = &table[b * row..b * row + row];
                    let acc: i32 = r[..dim]
                        .iter()
                        .zip(&w)
                        .map(|(&c, &x)| (c as i8) as i32 * x as i32)
                        .sum();
                    let sc = f16_to_f32(u16::from_le_bytes([r[dim], r[dim + 1]]));
                    acc as f32 * qs * sc
                })
                .collect();
            let mut got = vec![0.0f32; nb];
            unsafe { cent_dots_i8_vnni4(&table, dim, row, &w, wsum, qs, &mut got) };
            assert_eq!(
                got.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
                want.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
                "nb = {nb}"
            );
        }
    }

    #[test]
    fn mean_dots_match_the_scalar_pass() {
        let dim = 128usize;
        let mut rng = SplitMix64::new(3);
        let nb = 17usize;
        let means: Vec<u16> = (0..nb * dim)
            .map(|_| half::f16::from_f32((rng.next_f64() as f32 - 0.5) * 3.0).to_bits())
            .collect();
        let q = rand_vec(&mut rng, dim);
        let bytes: Vec<u8> = means.iter().flat_map(|v| v.to_le_bytes()).collect();
        let cents = Cents {
            kind: Centroid::F16,
            dim,
            n_blocks: nb,
            bytes: &bytes,
        };
        let got = mean_dots(&q, &cents);
        for b in 0..nb {
            let want: f32 = (0..dim)
                .map(|i| half::f16::from_bits(means[b * dim + i]).to_f32() * q[i])
                .sum();
            assert!(
                (got[b] - want).abs() < 1e-3 * want.abs().max(1.0),
                "block {b}"
            );
        }
    }

    #[test]
    fn dot_bf16_matches_the_portable_reference() {
        let mut rng = SplitMix64::new(5);
        for n in [128usize, 64, 37] {
            let a: Vec<u8> = rand_vec(&mut rng, n)
                .iter()
                .flat_map(|&v| crate::builder::search::f32_to_bf16(v).to_le_bytes())
                .collect();
            let b: Vec<u8> = rand_vec(&mut rng, n)
                .iter()
                .flat_map(|&v| crate::builder::search::f32_to_bf16(v).to_le_bytes())
                .collect();
            let want = crate::builder::search::dot_bf16_bytes(&a, &b);
            let got = dot_bf16(&a, &b);
            assert!(
                (got - want).abs() < 1e-3 * want.abs().max(1.0),
                "n {n}: {got} vs {want}"
            );
        }
    }

    #[test]
    fn block_scan_matches_the_per_key_kernel() {
        let (dim, block) = (128usize, BLOCK);
        let rot = Rotation::new(rotation_seed("blk"), dim);
        let mut rng = SplitMix64::new(21);
        let n = 70usize; // two full blocks + a short one
        let keys = rand_vec(&mut rng, n * dim);
        let values = rand_vec(&mut rng, n * dim);
        for codec in [Codec::Tq4] {
            let head = crate::builder::tq4::TqHead::encode(&keys, &values, dim, block, &rot, codec);
            let recs = crate::builder::nodes::NodeRecords::from_head(&head);
            let q = rand_vec(&mut rng, dim);
            let kq = KeyQuery::new(&q, &rot, codec);
            let md = mean_dots(&q, &head.cents());
            let stride = recs.stride;
            let mut buf = vec![0u8; BLOCK * stride];
            let mut per_block = vec![0.0f32; BLOCK];
            for b in 0..head.n_blocks as usize {
                to_block_layout(recs.codes(), stride, b * BLOCK, n, &mut buf);
                let norms = &head.key_norms[b * BLOCK..(b + 1) * BLOCK];
                block_scan(&buf, stride, norms, md[b], &kq, &mut per_block);
                let ids: Vec<u32> = (0..BLOCK as u32)
                    .map(|t| (b * BLOCK) as u32 + t)
                    .filter(|&p| (p as usize) < n)
                    .collect();
                let mut want = vec![0.0f32; ids.len()];
                recs.score(&md, &kq, &ids, &mut want);
                for (j, &w) in want.iter().enumerate() {
                    assert!(
                        (per_block[j] - w).abs() < 1e-3 * w.abs().max(1.0),
                        "{codec:?} block {b} slot {j}: {} vs {w}",
                        per_block[j]
                    );
                }
            }
        }
    }
}
