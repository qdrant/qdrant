//! SIMD kernels for encoding a rotated vector onto the 8- or 16-bit grid, see
//! `TurboQuantizer::finish_grid_simd`. They produce the same codes, centroid
//! norm and scratch buffer, bit for bit, as the scalar steps of
//! `TurboQuantizer::finish_quantize`:
//!
//! * the largest magnitude is exact in any order;
//! * scaling is one multiplication per value, as in the scalar loop;
//! * rounding is half away from zero, as `f64::round`: NEON has it as one
//!   instruction, AVX2 builds it from a truncation and a comparison of the
//!   exact remainder with 0.5;
//! * the squared codes are whole numbers up to 32767², so their sum is exact
//!   in any order below 2^53, which holds for fewer than 8 million dimensions.
//!
//! Backends: AVX2 on x86_64 and NEON on aarch64, as the Hadamard transform of
//! the same encoding path. Elsewhere the scalar steps run.

use crate::turboquant::TQBits;
use crate::turboquant::simd::query8bit::{CODE_OFFSET, signed_code};

/// The grid widths: 8-bit codes (`signed_code` of the value, one byte) and
/// 16-bit codes (the value as little-endian i16).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GridWidth {
    Bits8,
    Bits16,
}

impl GridWidth {
    pub(crate) fn of(bits: TQBits) -> Option<Self> {
        match bits {
            TQBits::Bits8 => Some(GridWidth::Bits8),
            TQBits::Bits16 => Some(GridWidth::Bits16),
            TQBits::Bits4 | TQBits::Bits2 | TQBits::Bits1_5 | TQBits::Bits1 => None,
        }
    }

    /// `TQBits::grid_max` of the width.
    pub(crate) fn grid_max(self) -> f64 {
        match self {
            GridWidth::Bits8 => 127.0,
            GridWidth::Bits16 => 32767.0,
        }
    }

    pub(crate) fn code_bytes(self) -> usize {
        match self {
            GridWidth::Bits8 => 1,
            GridWidth::Bits16 => 2,
        }
    }

    /// The scalar encoding of one scaled value, as `pack_vector` writes it,
    /// and its clamped value for the centroid norm.
    fn encode_scalar(self, x: f64, out: &mut [u8]) -> f64 {
        let g = self.grid_max();
        let c = x.round().clamp(-g, g);
        match self {
            GridWidth::Bits8 => out[0] = signed_code(c as i32),
            GridWidth::Bits16 => out.copy_from_slice(&(c as i16).to_le_bytes()),
        }
        c
    }
}

/// Whether [`GridSimd::scale_encode`] writes the scaled values back to the
/// buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteBack {
    Yes,
    No,
}

/// A SIMD backend the host supports, found once by [`GridSimd::detect`]; its
/// methods are safe because holding one proves the CPU features.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GridSimd {
    #[cfg(target_arch = "x86_64")]
    Avx2,
    #[cfg(target_arch = "aarch64")]
    Neon,
}

impl GridSimd {
    pub(crate) fn detect() -> Option<Self> {
        #[cfg(target_arch = "x86_64")]
        if std::is_x86_feature_detected!("avx2") {
            return Some(GridSimd::Avx2);
        }
        #[cfg(target_arch = "aarch64")]
        if std::arch::is_aarch64_feature_detected!("neon") {
            return Some(GridSimd::Neon);
        }
        None
    }

    /// Largest magnitude of `buf`, as `buf.iter().fold(0.0, |m, &x|
    /// m.max(x.abs()))` computes it (NaN values are skipped), and whether
    /// `buf` holds a NaN.
    pub(crate) fn abs_max(self, buf: &[f64]) -> (f64, bool) {
        match self {
            // SAFETY: `Avx2` exists only if AVX2 was detected.
            #[cfg(target_arch = "x86_64")]
            GridSimd::Avx2 => unsafe { x86::abs_max(buf) },
            // SAFETY: `Neon` exists only if NEON was detected.
            #[cfg(target_arch = "aarch64")]
            GridSimd::Neon => unsafe { arm::abs_max(buf) },
        }
    }

    /// Encodes `buf` onto the grid of `width`: multiplies each value by
    /// `scale` (written back to `buf` if `write_back`), rounds it half away
    /// from zero, clamps it to the grid, and stores its code in `out`
    /// (`width.code_bytes()` per value). A NaN value gets the code of 0, as
    /// `NaN as i32` gives 0.
    ///
    /// Returns the sum of the squared clamped values, NaN if any scaled value
    /// is NaN, as the scalar `compute_centroid_norm` sums them.
    pub(crate) fn scale_encode(
        self,
        buf: &mut [f64],
        scale: f64,
        width: GridWidth,
        write_back: WriteBack,
        out: &mut [u8],
    ) -> f64 {
        assert_eq!(out.len(), buf.len() * width.code_bytes());
        // Below 2^53 / 32767², every partial sum of squares stays exact
        debug_assert!(buf.len() < 8_000_000);
        let (sq_sum, any_nan, done) = match self {
            // SAFETY: `Avx2` exists only if AVX2 was detected; lengths checked above.
            #[cfg(target_arch = "x86_64")]
            GridSimd::Avx2 => unsafe { x86::scale_encode(buf, scale, width, write_back, out) },
            // SAFETY: `Neon` exists only if NEON was detected; lengths checked above.
            #[cfg(target_arch = "aarch64")]
            GridSimd::Neon => unsafe { arm::scale_encode(buf, scale, width, write_back, out) },
        };
        let mut sq_sum = sq_sum;
        let bytes = width.code_bytes();
        for (i, v) in buf.iter_mut().enumerate().skip(done) {
            let x = *v * scale;
            if write_back == WriteBack::Yes {
                *v = x;
            }
            let c = width.encode_scalar(x, &mut out[i * bytes..(i + 1) * bytes]);
            sq_sum += c.powi(2);
        }
        if any_nan { f64::NAN } else { sq_sum }
    }
}

/// The blocks of 16 values both backends encode at a time.
const BLOCK: usize = 16;

#[cfg(target_arch = "x86_64")]
mod x86 {
    use core::arch::x86_64::*;

    use super::{BLOCK, CODE_OFFSET, GridWidth, WriteBack};

    /// # Safety
    /// The CPU must support AVX and AVX2.
    #[target_feature(enable = "avx,avx2")]
    pub(super) unsafe fn abs_max(buf: &[f64]) -> (f64, bool) {
        let (chunks, tail) = buf.as_chunks::<4>();
        let sign = _mm256_set1_pd(-0.0);
        let mut max = _mm256_setzero_pd();
        let mut nan = _mm256_setzero_pd();
        for chunk in chunks {
            // SAFETY: `chunk` holds 4 values.
            let x = unsafe { _mm256_loadu_pd(chunk.as_ptr()) };
            // `max_pd` returns its second operand if either is NaN, so a NaN
            // keeps the running maximum, as `f64::max` does.
            max = _mm256_max_pd(_mm256_andnot_pd(sign, x), max);
            nan = _mm256_or_pd(nan, _mm256_cmp_pd::<_CMP_UNORD_Q>(x, x));
        }
        let mut lanes = [0.0_f64; 4];
        // SAFETY: `lanes` holds 4 values.
        unsafe { _mm256_storeu_pd(lanes.as_mut_ptr(), max) };
        let mut abs_max = lanes.iter().fold(0.0_f64, |m, &x| m.max(x));
        let mut has_nan = _mm256_movemask_pd(nan) != 0;
        for &x in tail {
            abs_max = abs_max.max(x.abs());
            has_nan |= x.is_nan();
        }
        (abs_max, has_nan)
    }

    /// Encodes the whole blocks of 16 values; returns the sum of their squared
    /// clamped values, whether any was NaN, and how many values it encoded.
    ///
    /// # Safety
    /// The CPU must support AVX and AVX2; `out` must hold
    /// `buf.len() * width.code_bytes()` bytes.
    #[target_feature(enable = "avx,avx2")]
    pub(super) unsafe fn scale_encode(
        buf: &mut [f64],
        scale: f64,
        width: GridWidth,
        write_back: WriteBack,
        out: &mut [u8],
    ) -> (f64, bool, usize) {
        let c = Consts::new(scale, width.grid_max());
        let mut sq = _mm256_setzero_pd();
        let mut nan = _mm256_setzero_pd();
        let blocks = buf.len() / BLOCK;
        for b in 0..blocks {
            let at = b * BLOCK;
            // SAFETY: values `at .. at + 16` exist in `buf`, and their codes in
            // `out` (the caller's length guarantee); AVX2 is the caller's.
            unsafe {
                let p = buf.as_mut_ptr().add(at);
                let q0 = quad(p, &c, write_back, &mut sq, &mut nan);
                let q1 = quad(p.add(4), &c, write_back, &mut sq, &mut nan);
                let q2 = quad(p.add(8), &c, write_back, &mut sq, &mut nan);
                let q3 = quad(p.add(12), &c, write_back, &mut sq, &mut nan);
                // The values are within ±32767: the packs don't saturate
                let lo = _mm_packs_epi32(q0, q1);
                let hi = _mm_packs_epi32(q2, q3);
                match width {
                    GridWidth::Bits8 => {
                        // 1 ..= 255, or `CODE_OFFSET` for NaN
                        let offset = _mm_set1_epi16(i16::from(CODE_OFFSET));
                        let bytes =
                            _mm_packus_epi16(_mm_add_epi16(lo, offset), _mm_add_epi16(hi, offset));
                        _mm_storeu_si128(out.as_mut_ptr().add(at).cast::<__m128i>(), bytes);
                    }
                    GridWidth::Bits16 => {
                        // Little endian, as `i16::to_le_bytes`
                        let o = out.as_mut_ptr().add(2 * at);
                        _mm_storeu_si128(o.cast::<__m128i>(), lo);
                        _mm_storeu_si128(o.add(16).cast::<__m128i>(), hi);
                    }
                }
            }
        }
        let mut lanes = [0.0_f64; 4];
        // SAFETY: `lanes` holds 4 values.
        unsafe { _mm256_storeu_pd(lanes.as_mut_ptr(), sq) };
        (
            lanes.iter().sum(),
            _mm256_movemask_pd(nan) != 0,
            blocks * BLOCK,
        )
    }

    struct Consts {
        scale: __m256d,
        half: __m256d,
        one: __m256d,
        sign: __m256d,
        lo: __m256d,
        hi: __m256d,
    }

    impl Consts {
        #[target_feature(enable = "avx,avx2")]
        fn new(scale: f64, grid_max: f64) -> Self {
            Consts {
                scale: _mm256_set1_pd(scale),
                half: _mm256_set1_pd(0.5),
                one: _mm256_set1_pd(1.0),
                sign: _mm256_set1_pd(-0.0),
                lo: _mm256_set1_pd(-grid_max),
                hi: _mm256_set1_pd(grid_max),
            }
        }
    }

    /// Four values at `p`: scaled, rounded, clamped; returns them as i32.
    ///
    /// # Safety
    /// `p .. p + 4` must be valid for reads and writes; the CPU must support
    /// AVX and AVX2.
    #[target_feature(enable = "avx,avx2")]
    #[inline]
    unsafe fn quad(
        p: *mut f64,
        c: &Consts,
        write_back: WriteBack,
        sq: &mut __m256d,
        nan: &mut __m256d,
    ) -> __m128i {
        // SAFETY: the caller guarantees 4 values at `p`.
        let x = _mm256_mul_pd(unsafe { _mm256_loadu_pd(p) }, c.scale);
        if write_back == WriteBack::Yes {
            // SAFETY: as above.
            unsafe { _mm256_storeu_pd(p, x) };
        }
        // Half away from zero: truncate, then step away from zero where the
        // remainder (exact: |x| < 2^52 or x is already whole) is at least
        // 0.5. NaN and infinities get a NaN remainder, which compares false.
        let t = _mm256_round_pd::<{ _MM_FROUND_TO_ZERO | _MM_FROUND_NO_EXC }>(x);
        let rem = _mm256_andnot_pd(c.sign, _mm256_sub_pd(x, t));
        let away = _mm256_cmp_pd::<_CMP_GE_OQ>(rem, c.half);
        let step = _mm256_and_pd(away, _mm256_or_pd(c.one, _mm256_and_pd(x, c.sign)));
        let rounded = _mm256_add_pd(t, step);
        let is_nan = _mm256_cmp_pd::<_CMP_UNORD_Q>(x, x);
        // Clamp, then NaN to 0 (`max_pd` would turn NaN into the lower limit)
        let clamped = _mm256_min_pd(_mm256_max_pd(rounded, c.lo), c.hi);
        let clamped = _mm256_andnot_pd(is_nan, clamped);
        *sq = _mm256_add_pd(*sq, _mm256_mul_pd(clamped, clamped));
        *nan = _mm256_or_pd(*nan, is_nan);
        // Whole numbers: the conversion is exact in any rounding mode
        _mm256_cvtpd_epi32(clamped)
    }
}

#[cfg(target_arch = "aarch64")]
mod arm {
    use core::arch::aarch64::*;

    use super::{BLOCK, CODE_OFFSET, GridWidth, WriteBack};

    /// # Safety
    /// The CPU must support NEON.
    #[target_feature(enable = "neon")]
    pub(super) unsafe fn abs_max(buf: &[f64]) -> (f64, bool) {
        let (chunks, tail) = buf.as_chunks::<2>();
        let mut max = vdupq_n_f64(0.0);
        let mut not_nan = vdupq_n_u64(u64::MAX);
        for chunk in chunks {
            // SAFETY: `chunk` holds 2 values.
            let x = unsafe { vld1q_f64(chunk.as_ptr()) };
            // NaN lanes become +0.0 before the maximum, so they are skipped as
            // `f64::max` skips them: FMAXNM alone would turn a signaling NaN
            // into a quiet NaN and lose the running maximum. All magnitudes
            // are at least +0.0, the start value, so the mask changes nothing
            // else.
            let ok = vceqq_f64(x, x);
            let magnitude =
                vreinterpretq_f64_u64(vandq_u64(vreinterpretq_u64_f64(vabsq_f64(x)), ok));
            max = vmaxnmq_f64(max, magnitude);
            not_nan = vandq_u64(not_nan, ok);
        }
        let mut abs_max = vgetq_lane_f64::<0>(max).max(vgetq_lane_f64::<1>(max));
        let mut has_nan = (vgetq_lane_u64::<0>(not_nan) & vgetq_lane_u64::<1>(not_nan)) == 0;
        for &x in tail {
            abs_max = abs_max.max(x.abs());
            has_nan |= x.is_nan();
        }
        (abs_max, has_nan)
    }

    /// Encodes the whole blocks of 16 values; returns the sum of their squared
    /// clamped values, whether any was NaN, and how many values it encoded.
    ///
    /// # Safety
    /// The CPU must support NEON; `out` must hold
    /// `buf.len() * width.code_bytes()` bytes.
    #[target_feature(enable = "neon")]
    pub(super) unsafe fn scale_encode(
        buf: &mut [f64],
        scale: f64,
        width: GridWidth,
        write_back: WriteBack,
        out: &mut [u8],
    ) -> (f64, bool, usize) {
        let c = Consts {
            scale: vdupq_n_f64(scale),
            lo: vdupq_n_f64(-width.grid_max()),
            hi: vdupq_n_f64(width.grid_max()),
        };
        let mut sq = vdupq_n_f64(0.0);
        let mut not_nan = vdupq_n_u64(u64::MAX);
        let blocks = buf.len() / BLOCK;
        for b in 0..blocks {
            let at = b * BLOCK;
            // SAFETY: values `at .. at + 16` exist in `buf`, and their codes in
            // `out` (the caller's length guarantee); NEON is the caller's.
            unsafe {
                let p = buf.as_mut_ptr().add(at);
                let mut i16s = [vdupq_n_s16(0); 2];
                for (h, half) in i16s.iter_mut().enumerate() {
                    let q = p.add(8 * h);
                    let mut i32s = [vdupq_n_s32(0); 2];
                    for (k, quad) in i32s.iter_mut().enumerate() {
                        let a = pair(q.add(4 * k), &c, write_back, &mut sq, &mut not_nan);
                        let b = pair(q.add(4 * k + 2), &c, write_back, &mut sq, &mut not_nan);
                        // Within ±32767: narrowing keeps the values
                        *quad = vcombine_s32(vmovn_s64(a), vmovn_s64(b));
                    }
                    *half = vcombine_s16(vmovn_s32(i32s[0]), vmovn_s32(i32s[1]));
                }
                match width {
                    GridWidth::Bits8 => {
                        // 1 ..= 255, or `CODE_OFFSET` for NaN
                        let offset = vdupq_n_s16(i16::from(CODE_OFFSET));
                        let bytes = vcombine_u8(
                            vqmovun_s16(vaddq_s16(i16s[0], offset)),
                            vqmovun_s16(vaddq_s16(i16s[1], offset)),
                        );
                        vst1q_u8(out.as_mut_ptr().add(at), bytes);
                    }
                    GridWidth::Bits16 => {
                        // Little endian, as `i16::to_le_bytes`
                        let o = out.as_mut_ptr().add(2 * at);
                        vst1q_u8(o, vreinterpretq_u8_s16(i16s[0]));
                        vst1q_u8(o.add(16), vreinterpretq_u8_s16(i16s[1]));
                    }
                }
            }
        }
        let sum = vgetq_lane_f64::<0>(sq) + vgetq_lane_f64::<1>(sq);
        let any_nan = (vgetq_lane_u64::<0>(not_nan) & vgetq_lane_u64::<1>(not_nan)) == 0;
        (sum, any_nan, blocks * BLOCK)
    }

    struct Consts {
        scale: float64x2_t,
        lo: float64x2_t,
        hi: float64x2_t,
    }

    /// Two values at `p`: scaled, rounded, clamped; returns them as i64.
    ///
    /// # Safety
    /// `p .. p + 2` must be valid for reads and writes; the CPU must support
    /// NEON.
    #[target_feature(enable = "neon")]
    #[inline]
    unsafe fn pair(
        p: *mut f64,
        c: &Consts,
        write_back: WriteBack,
        sq: &mut float64x2_t,
        not_nan: &mut uint64x2_t,
    ) -> int64x2_t {
        // SAFETY: the caller guarantees 2 values at `p`.
        let x = vmulq_f64(unsafe { vld1q_f64(p) }, c.scale);
        if write_back == WriteBack::Yes {
            // SAFETY: as above.
            unsafe { vst1q_f64(p, x) };
        }
        // FRINTA: to nearest, ties away from zero, as `f64::round`
        let rounded = vrndaq_f64(x);
        let ok = vceqq_f64(x, x);
        // FMAX/FMIN keep NaN; the mask then makes it +0.0, as `NaN as i32` is 0
        let clamped = vminq_f64(vmaxq_f64(rounded, c.lo), c.hi);
        let clamped = vreinterpretq_f64_u64(vandq_u64(vreinterpretq_u64_f64(clamped), ok));
        *sq = vaddq_f64(*sq, vmulq_f64(clamped, clamped));
        *not_nan = vandq_u64(*not_nan, ok);
        // Whole numbers: the conversion is exact
        vcvtq_s64_f64(clamped)
    }
}
