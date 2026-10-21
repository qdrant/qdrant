//! TurboQuant vectors as matrix operands for the `pairwise` kernels, which then compute what
//! [`TurboQuantizer::score_symmetric`] computes for every pair. The kernels compute the inner
//! products of the codes, and [`TurboCombine`] finishes them with the same steps as
//! `score_symmetric`.
//!
//! The codes stay in the width they're stored in, except TQ+ 2 and 4 bits, whose codes are
//! folded with their coordinates' `D'` into the i8 values `score_symmetric` multiplies (see
//! [`query4bit::fold_codes`]) and scored as 8-bit codes. The scores are exact, except for 16
//! bits, decoded to f32 and summed by the f32 kernels, about 1e-7 of the inputs' magnitude
//! off, as no integer kernel was faster. On aarch64, which has no code tile, the Normal 2, 4,
//! and 8 bits and the folded TQ+ values are decoded to f32 as well, see [`CODES_IN_F32`].

use std::marker::PhantomData;

use common::types::PointOffsetType;
use pairwise::{
    Aux, Bits, Codebook, Codes2, Codes4, Codes8, Cols, Combine, CombineDispatch, CombineFn, Kernel,
    KernelElem, MatrixOperands, TopK,
};

use crate::DistanceType;
use crate::encoded_storage::EncodedStorage;
use crate::turboquant::TQBits;
use crate::turboquant::quantization::{
    SymmetricCorrection, SymmetricValues, TurboQuantizer, symmetric_score,
};
use crate::turboquant::simd::{query1bit, query2bit, query4bit};

/// Whether [`TurboMatrixOperands::new`] decodes the Normal 2, 4, and 8 bits and the folded TQ+
/// 2 and 4 bits to f32 for the f32 kernels instead of keeping them as codes: on aarch64, where
/// the code types have only the scalar kernel, far slower than NEON scoring per pair.
///
/// The products are integers of at most 128², so the f32 sums are exact as long as they stay
/// within 2²⁴: always for up to 1024 positions. Beyond, they can round, about 1e-7 of the
/// inputs' magnitude (at most 1536 · 128² ≈ 2.5e7 at dim 1536), though sums of real vectors
/// stay far lower.
#[doc(hidden)]
pub const CODES_IN_F32: bool = cfg!(target_arch = "aarch64");

/// Encoded TurboQuant vectors by point, read in batches.
pub trait TurboSource: Sync {
    type Error;

    /// Calls `f` with the position in `ids` and the encoded vector of each point.
    fn for_each_batch(
        &self,
        ids: &[PointOffsetType],
        f: impl FnMut(usize, &[u8]),
    ) -> Result<(), Self::Error>;
}

/// The vectors of an [`EncodedStorage`].
pub struct StorageSource<'a, T>(pub &'a T);

impl<T: EncodedStorage + Sync> TurboSource for StorageSource<'_, T> {
    type Error = std::io::Error;

    fn for_each_batch(
        &self,
        ids: &[PointOffsetType],
        mut f: impl FnMut(usize, &[u8]),
    ) -> std::io::Result<()> {
        self.0.for_each_batch(ids, |i, vector| f(i, &vector))
    }
}

/// TurboQuant vectors as operands of element type `E`, with their [`SymmetricValues`] as `aux`.
pub struct TurboOperands<'a, S, E: KernelElem> {
    quantizer: &'a TurboQuantizer,
    source: S,
    /// Elements per vector.
    depth: usize,
    combine: TurboCombine,
    param: E::Param<'a>,
    decode: Decode,
    _elem: PhantomData<fn() -> E>,
}

/// Combines as [`TurboQuantizer::score_symmetric`], negated if `invert`: the inner product
/// becomes the centroid dot as `centroid_dot` says, then the score as `symmetric_score` says.
#[derive(Clone, Copy, Debug)]
pub struct TurboCombine {
    centroid_dot: CentroidDot,
    l2: bool,
    invert: bool,
}

/// How the kernels' inner product of two vectors becomes their centroid dot, by width.
#[derive(Clone, Copy, Debug)]
enum CentroidDot {
    /// 1 and 1.5 bits
    Signs(Signs),
    /// Normal 2 bits
    Codebook2(Codebook2),
    /// Normal 4 bits
    Codebook4(Codebook4),
    /// 8 and 16 bits
    Grid(Grid),
    /// TQ+ 2 and 4 bits: the sum of the folded values, which carries `D'²`.
    Corrected(SymmetricCorrection),
}

impl CombineDispatch for TurboCombine {
    #[inline(always)]
    fn dispatch<F: CombineFn>(self, f: F) -> F::Output {
        match self.centroid_dot {
            CentroidDot::Signs(step) => f.call(self.with(step)),
            CentroidDot::Codebook2(step) => f.call(self.with(step)),
            CentroidDot::Codebook4(step) => f.call(self.with(step)),
            CentroidDot::Grid(step) => f.call(self.with(step)),
            CentroidDot::Corrected(step) => f.call(self.with(step)),
        }
    }
}

impl TurboCombine {
    #[inline(always)]
    fn with<D: ToCentroidDot>(self, centroid_dot: D) -> FixedTurboCombine<D> {
        FixedTurboCombine {
            centroid_dot,
            l2: self.l2,
            invert: self.invert,
        }
    }
}

/// [`TurboCombine`] with its way to the centroid dot chosen.
#[derive(Clone, Copy)]
struct FixedTurboCombine<D> {
    centroid_dot: D,
    l2: bool,
    invert: bool,
}

impl<D: ToCentroidDot> Combine for FixedTurboCombine<D> {
    #[inline(always)]
    fn score(self, dot: f32, aux_a: &Aux, aux_b: &Aux) -> f32 {
        let (values_a, values_b) = (from_aux(aux_a), from_aux(aux_b));
        let raw = self.centroid_dot.to_centroid_dot(dot, &values_a, &values_b);
        let score = symmetric_score(self.l2, raw, &values_a, &values_b);
        if self.invert { -score } else { score }
    }
}

/// One way of [`CentroidDot`], through what the per-pair scorer of the width uses.
trait ToCentroidDot: Copy + Send + Sync {
    fn to_centroid_dot(
        self,
        dot: f32,
        values_a: &SymmetricValues,
        values_b: &SymmetricValues,
    ) -> f32;
}

/// The number of differing sign bits, out of `bits`.
#[derive(Clone, Copy, Debug)]
struct Signs {
    bits: f32,
}

impl ToCentroidDot for Signs {
    #[inline(always)]
    fn to_centroid_dot(self, dot: f32, _: &SymmetricValues, _: &SymmetricValues) -> f32 {
        query1bit::differing_to_score(self.bits, dot)
    }
}

/// The sum of the codes' products, as signed codebook values of 2 bits.
#[derive(Clone, Copy, Debug)]
struct Codebook2;

impl ToCentroidDot for Codebook2 {
    #[inline(always)]
    fn to_centroid_dot(self, dot: f32, _: &SymmetricValues, _: &SymmetricValues) -> f32 {
        query2bit::centroid_dot(dot)
    }
}

/// The sum of the codes' products, as signed codebook values of 4 bits.
#[derive(Clone, Copy, Debug)]
struct Codebook4;

impl ToCentroidDot for Codebook4 {
    #[inline(always)]
    fn to_centroid_dot(self, dot: f32, _: &SymmetricValues, _: &SymmetricValues) -> f32 {
        query4bit::centroid_dot(dot)
    }
}

/// The sum of the grid values' products, which is the centroid dot.
#[derive(Clone, Copy, Debug)]
struct Grid;

impl ToCentroidDot for Grid {
    #[inline(always)]
    fn to_centroid_dot(self, dot: f32, _: &SymmetricValues, _: &SymmetricValues) -> f32 {
        dot
    }
}

impl ToCentroidDot for SymmetricCorrection {
    #[inline(always)]
    fn to_centroid_dot(
        self,
        dot: f32,
        values_a: &SymmetricValues,
        values_b: &SymmetricValues,
    ) -> f32 {
        self.centroid_dot(dot, values_a, values_b)
    }
}

#[inline(always)]
fn to_aux(values: SymmetricValues) -> Aux {
    [values.scale, values.l2_sq, values.correction]
}

#[inline(always)]
fn from_aux(&[scale, l2_sq, correction]: &Aux) -> SymmetricValues {
    SymmetricValues {
        scale,
        l2_sq,
        correction,
    }
}

/// How the codes become elements.
enum Decode {
    /// The stored bytes as they are.
    Stored,
    /// 8-bit grid values, stored as `value + 128`.
    Grid8,
    /// Little-endian `i16` grid values, exact in f32.
    Grid16,
    /// Normal codes of `bits` (2 or 4) as f32: the code's value in `codebook` times `roots`
    /// of the position, all 1, which keeps the value exact.
    Weighted {
        bits: u32,
        codebook: [f32; 16],
        roots: Vec<f32>,
    },
    /// TQ+ codes of `bits` (2 or 4) folded with the Q15 `factors` of their positions, the i8
    /// values that `score_symmetric` multiplies: as 8-bit codes, or as f32 if
    /// [`CODES_IN_F32`].
    Folded { bits: u32, factors: Vec<i16> },
}

/// The operands of one width: an enum, as each width has its own element type.
pub enum TurboMatrixOperands<'a, S> {
    /// 1 and 1.5 bits: the stored sign bits.
    Bits(TurboOperands<'a, S, Bits>),
    /// 2 bits, Normal, unless [`CODES_IN_F32`].
    Codes2(TurboOperands<'a, S, Codes2>),
    /// 4 bits, Normal, unless [`CODES_IN_F32`].
    Codes4(TurboOperands<'a, S, Codes4>),
    /// 8 bits and the folded TQ+ 2 and 4 bits, unless [`CODES_IN_F32`].
    Codes8(TurboOperands<'a, S, Codes8>),
    /// 16 bits, and with [`CODES_IN_F32`] the Normal 2, 4, and 8 bits and the folded TQ+ 2
    /// and 4 bits: decoded to f32, see the module.
    F32(TurboOperands<'a, S, f32>),
}

impl<'a, S: TurboSource> TurboMatrixOperands<'a, S> {
    /// Operands that score as `score_symmetric`, negated if `invert`. `None` for L1, which
    /// has no matrix form.
    pub fn new(quantizer: &'a TurboQuantizer, source: S, invert: bool) -> Option<Self> {
        Self::new_with(quantizer, source, invert, CODES_IN_F32)
    }

    /// [`Self::new`], with the Normal 2, 4, and 8 bits decoded to f32 if `codes_in_f32`: to
    /// test and benchmark the aarch64 operands elsewhere.
    #[doc(hidden)]
    pub fn new_with(
        quantizer: &'a TurboQuantizer,
        source: S,
        invert: bool,
        codes_in_f32: bool,
    ) -> Option<Self> {
        let l2 = match quantizer.distance {
            DistanceType::Dot | DistanceType::Cosine => false,
            DistanceType::L2 => true,
            DistanceType::L1 => return None,
        };
        let data_bytes = quantizer.padded_dim * usize::from(quantizer.bits.bit_size()) / 8;
        let combine = |centroid_dot| TurboCombine {
            centroid_dot,
            l2,
            invert,
        };
        let operands = match quantizer.bits {
            // TQ+ doesn't weight 1-bit codes either
            TQBits::Bits1 | TQBits::Bits1_5 => Self::Bits(TurboOperands::new(
                quantizer,
                source,
                data_bytes,
                combine(CentroidDot::Signs(Signs {
                    bits: (data_bytes * 8) as f32,
                })),
                (),
            )),
            TQBits::Bits2 => Self::codebook(
                quantizer,
                source,
                combine,
                &query2bit::signed_codebook(),
                CentroidDot::Codebook2(Codebook2),
                codes_in_f32,
                Self::Codes2,
            ),
            TQBits::Bits4 => Self::codebook(
                quantizer,
                source,
                combine,
                &query4bit::signed_codebook(),
                CentroidDot::Codebook4(Codebook4),
                codes_in_f32,
                Self::Codes4,
            ),
            TQBits::Bits8 if codes_in_f32 => Self::decoded(
                quantizer,
                source,
                combine(CentroidDot::Grid(Grid)),
                Decode::Grid8,
            ),
            TQBits::Bits8 => Self::Codes8(TurboOperands::new(
                quantizer,
                source,
                data_bytes,
                combine(CentroidDot::Grid(Grid)),
                (),
            )),
            TQBits::Bits16 => Self::decoded(
                quantizer,
                source,
                combine(CentroidDot::Grid(Grid)),
                Decode::Grid16,
            ),
        };
        Some(operands)
    }

    /// A codebook width, with the codebook `values` and the Normal `centroid_dot`: Normal on
    /// its stored codes, or decoded to f32 if `codes_in_f32`; TQ+ folded and corrected as
    /// `score_symmetric_ec`, as 8-bit codes, or decoded to f32 if `codes_in_f32`.
    fn codebook<E>(
        quantizer: &'a TurboQuantizer,
        source: S,
        combine: impl Fn(CentroidDot) -> TurboCombine,
        values: &[i8],
        centroid_dot: CentroidDot,
        codes_in_f32: bool,
        stored: impl FnOnce(TurboOperands<'a, S, E>) -> Self,
    ) -> Self
    where
        E: KernelElem<Param<'a> = Codebook>,
    {
        debug_assert!(values.len().is_power_of_two());
        let bits = values.len().trailing_zeros();
        match &quantizer.error_correction {
            None if !codes_in_f32 => {
                let bytes = quantizer.padded_dim * usize::from(quantizer.bits.bit_size()) / 8;
                return stored(TurboOperands::new(
                    quantizer,
                    source,
                    bytes,
                    combine(centroid_dot),
                    Codebook::new(values),
                ));
            }
            None => {}
            Some(ec) => {
                let combine = combine(CentroidDot::Corrected(ec.symmetric(quantizer.bits)));
                let decode = Decode::Folded {
                    bits,
                    factors: ec.d_prime_q15.clone(),
                };
                if codes_in_f32 {
                    return Self::decoded(quantizer, source, combine, decode);
                }
                let mut operands =
                    TurboOperands::new(quantizer, source, quantizer.padded_dim, combine, ());
                operands.decode = decode;
                return Self::Codes8(operands);
            }
        }
        let mut codebook = [0.0; 16];
        for (c, &value) in codebook.iter_mut().zip(values) {
            *c = f32::from(value);
        }
        let decode = Decode::Weighted {
            bits,
            codebook,
            roots: vec![1.0; quantizer.padded_dim],
        };
        Self::decoded(quantizer, source, combine(centroid_dot), decode)
    }

    /// Codes decoded to f32 by `decode`, one element per position.
    fn decoded(
        quantizer: &'a TurboQuantizer,
        source: S,
        combine: TurboCombine,
        decode: Decode,
    ) -> Self {
        let mut operands = TurboOperands::new(quantizer, source, quantizer.padded_dim, combine, ());
        operands.decode = decode;
        Self::F32(operands)
    }

    /// [`pairwise::top_k`] with these operands.
    pub fn top_k(
        &self,
        rows: &[PointOffsetType],
        cols: Cols<'_>,
        out: &mut TopK,
    ) -> Result<(), S::Error> {
        self.top_k_with(Kernel::best(), rows, cols, out)
    }

    /// [`pairwise::top_k_with`] with these operands, for tests and benchmarks.
    #[doc(hidden)]
    pub fn top_k_with(
        &self,
        kernel: Kernel,
        rows: &[PointOffsetType],
        cols: Cols<'_>,
        out: &mut TopK,
    ) -> Result<(), S::Error> {
        match self {
            Self::Bits(operands) => pairwise::top_k_with(kernel, operands, rows, cols, out),
            Self::Codes2(operands) => pairwise::top_k_with(kernel, operands, rows, cols, out),
            Self::Codes4(operands) => pairwise::top_k_with(kernel, operands, rows, cols, out),
            Self::Codes8(operands) => pairwise::top_k_with(kernel, operands, rows, cols, out),
            Self::F32(operands) => pairwise::top_k_with(kernel, operands, rows, cols, out),
        }
    }
}

impl<'a, S: TurboSource, E: KernelElem> TurboOperands<'a, S, E> {
    fn new(
        quantizer: &'a TurboQuantizer,
        source: S,
        depth: usize,
        combine: TurboCombine,
        param: E::Param<'a>,
    ) -> Self {
        Self {
            quantizer,
            source,
            depth,
            combine,
            param,
            decode: Decode::Stored,
            _elem: PhantomData,
        }
    }

    /// Decodes `ids` with `decode(codes, out)` per vector, `depth` elements each.
    fn decode_with(
        &self,
        ids: &[PointOffsetType],
        out: &mut [E],
        aux: &mut [Aux],
        mut decode: impl FnMut(&[u8], &mut [E]),
    ) -> Result<(), S::Error> {
        let depth = self.depth;
        self.source.for_each_batch(ids, |i, vector| {
            let (codes, extras) = self.quantizer.split_vector(vector);
            decode(codes, &mut out[i * depth..(i + 1) * depth]);
            aux[i] = to_aux(self.quantizer.symmetric_values(&extras));
        })
    }
}

impl<S: TurboSource> MatrixOperands for TurboOperands<'_, S, Bits> {
    type Elem = Bits;
    type Combine = TurboCombine;
    type Error = S::Error;

    /// Bytes per vector.
    fn depth(&self) -> usize {
        self.depth
    }

    fn combine(&self) -> TurboCombine {
        self.combine
    }

    fn param(&self) {}

    fn decode_batch(
        &self,
        ids: &[PointOffsetType],
        out: &mut [Bits],
        aux: &mut [Aux],
    ) -> Result<(), S::Error> {
        self.decode_with(ids, out, aux, |codes, out| {
            for (o, &byte) in out.iter_mut().zip(codes) {
                *o = Bits(byte);
            }
        })
    }
}

impl<S: TurboSource> MatrixOperands for TurboOperands<'_, S, f32> {
    type Elem = f32;
    type Combine = TurboCombine;
    type Error = S::Error;

    fn depth(&self) -> usize {
        self.depth
    }

    fn combine(&self) -> TurboCombine {
        self.combine
    }

    fn param(&self) {}

    fn decode_batch(
        &self,
        ids: &[PointOffsetType],
        out: &mut [f32],
        aux: &mut [Aux],
    ) -> Result<(), S::Error> {
        match &self.decode {
            Decode::Stored => unreachable!("f32 operands decode their codes"),
            Decode::Grid8 => self.decode_with(ids, out, aux, |codes, out| {
                for (o, &code) in out.iter_mut().zip(codes) {
                    *o = f32::from((code ^ 0x80) as i8);
                }
            }),
            Decode::Grid16 => self.decode_with(ids, out, aux, |codes, out| {
                let (codes, _) = codes.as_chunks::<2>();
                for (o, &code) in out.iter_mut().zip(codes) {
                    *o = f32::from(i16::from_le_bytes(code));
                }
            }),
            Decode::Weighted {
                bits: 2,
                codebook,
                roots,
            } => self.decode_with(ids, out, aux, |codes, out| {
                weighted::<2>(codes, codebook, roots, out)
            }),
            Decode::Weighted {
                bits: 4,
                codebook,
                roots,
            } => self.decode_with(ids, out, aux, |codes, out| {
                weighted::<4>(codes, codebook, roots, out)
            }),
            Decode::Weighted { bits, .. } => unreachable!("codebooks of 2 and 4 bits, not {bits}"),
            Decode::Folded { bits, factors } => {
                let mut folded = vec![0; self.depth];
                self.decode_with(ids, out, aux, |codes, out| {
                    fold(*bits, codes, factors, &mut folded);
                    for (o, &value) in out.iter_mut().zip(&folded) {
                        *o = f32::from(value);
                    }
                })
            }
        }
    }
}

/// Decodes codes of `BITS` (2 or 4) each, see [`Decode::Weighted`].
#[inline]
fn weighted<const BITS: usize>(codes: &[u8], codebook: &[f32; 16], roots: &[f32], out: &mut [f32]) {
    debug_assert!(out.len() <= roots.len() && out.len() <= codes.len() * 8 / BITS);
    #[cfg(target_arch = "x86_64")]
    let done = if std::is_x86_feature_detected!("avx2") {
        // SAFETY: AVX2 checked above, the lengths from the caller
        unsafe { weighted_avx2::<BITS>(codes, codebook, roots, out) }
    } else {
        0
    };
    // No NEON decode yet: one code at a time, which took 11% of a leaf's time at dim 1536 on x86
    #[cfg(not(target_arch = "x86_64"))]
    let done = 0;
    weighted_scalar::<BITS>(done, codes, codebook, roots, out);
}

/// [`weighted`] from position `start` on, one code at a time.
#[inline]
fn weighted_scalar<const BITS: usize>(
    start: usize,
    codes: &[u8],
    codebook: &[f32; 16],
    roots: &[f32],
    out: &mut [f32],
) {
    let per_byte = 8 / BITS;
    let mask = (1 << BITS) - 1;
    for i in start..out.len() {
        let code = codes[i / per_byte] >> (BITS * (i % per_byte)) & mask;
        out[i] = codebook[usize::from(code)] * roots[i];
    }
}

/// [`weighted`] for whole groups of 32 bits of codes: 8 codes of 4 bits, 16 of 2. Returns the
/// positions done. The same products as [`weighted_scalar`].
///
/// # Safety
///
/// The CPU must support AVX2, and the slices be as [`weighted`] asserts.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn weighted_avx2<const BITS: usize>(
    codes: &[u8],
    codebook: &[f32; 16],
    roots: &[f32],
    out: &mut [f32],
) -> usize {
    use std::arch::x86_64::*;
    let per_word = 32 / BITS;
    let words = out.len() / per_word;
    // SAFETY: word `w` reads 4 bytes of `codes` at `4 · w` and writes and reads `per_word`
    // positions from `per_word · w`, all within the slices for `w < words`
    unsafe {
        let low = _mm256_loadu_ps(codebook.as_ptr());
        let high = _mm256_loadu_ps(codebook.as_ptr().add(8));
        let mask = _mm256_set1_epi32((1 << BITS) - 1);
        // Code `k` of a word sits at bit `BITS · k`
        let shifts = |half: i32| {
            let b = BITS as i32;
            let first = 8 * half * b;
            _mm256_setr_epi32(
                first,
                first + b,
                first + 2 * b,
                first + 3 * b,
                first + 4 * b,
                first + 5 * b,
                first + 6 * b,
                first + 7 * b,
            )
        };
        let shifts = [shifts(0), shifts(1)];
        for w in 0..words {
            let word = _mm256_set1_epi32(codes.as_ptr().add(4 * w).cast::<i32>().read_unaligned());
            for (h, &shift) in shifts.iter().enumerate().take(per_word / 8) {
                let index = _mm256_and_si256(_mm256_srlv_epi32(word, shift), mask);
                let value = if BITS == 4 {
                    // Codes 8 to 15 from the high table: bit 3 moved to the sign
                    let upper = _mm256_castsi256_ps(_mm256_slli_epi32::<28>(index));
                    _mm256_blendv_ps(
                        _mm256_permutevar8x32_ps(low, index),
                        _mm256_permutevar8x32_ps(high, index),
                        upper,
                    )
                } else {
                    _mm256_permutevar8x32_ps(low, index)
                };
                let p = per_word * w + 8 * h;
                let product = _mm256_mul_ps(value, _mm256_loadu_ps(roots.as_ptr().add(p)));
                _mm256_storeu_ps(out.as_mut_ptr().add(p), product);
            }
        }
    }
    words * per_word
}

/// The stored bytes as they are, for each of the code types.
macro_rules! code_operands {
    ($elem:ident, $what:literal) => {
        impl<S: TurboSource> MatrixOperands for TurboOperands<'_, S, $elem> {
            type Elem = $elem;
            type Combine = TurboCombine;
            type Error = S::Error;

            #[doc = $what]
            fn depth(&self) -> usize {
                self.depth
            }

            fn combine(&self) -> TurboCombine {
                self.combine
            }

            fn param(&self) -> <$elem as KernelElem>::Param<'_> {
                self.param
            }

            fn decode_batch(
                &self,
                ids: &[PointOffsetType],
                out: &mut [$elem],
                aux: &mut [Aux],
            ) -> Result<(), S::Error> {
                self.decode_with(ids, out, aux, |codes, out| {
                    for (o, &byte) in out.iter_mut().zip(codes) {
                        *o = $elem(byte);
                    }
                })
            }
        }
    };
}

code_operands!(Codes2, "Bytes per vector, 4 codes each.");
code_operands!(Codes4, "Bytes per vector, 2 codes each.");
impl<S: TurboSource> MatrixOperands for TurboOperands<'_, S, Codes8> {
    type Elem = Codes8;
    type Combine = TurboCombine;
    type Error = S::Error;

    /// Bytes per vector.
    fn depth(&self) -> usize {
        self.depth
    }

    fn combine(&self) -> TurboCombine {
        self.combine
    }

    fn param(&self) {}

    fn decode_batch(
        &self,
        ids: &[PointOffsetType],
        out: &mut [Codes8],
        aux: &mut [Aux],
    ) -> Result<(), S::Error> {
        match &self.decode {
            Decode::Stored => self.decode_with(ids, out, aux, |codes, out| {
                for (o, &byte) in out.iter_mut().zip(codes) {
                    *o = Codes8(byte);
                }
            }),
            // Stored as `value + 128`, as the 8-bit grid
            Decode::Folded { bits, factors } => {
                let mut folded = vec![0; self.depth];
                self.decode_with(ids, out, aux, |codes, out| {
                    fold(*bits, codes, factors, &mut folded);
                    for (o, &value) in out.iter_mut().zip(&folded) {
                        *o = Codes8(value.cast_unsigned() ^ 0x80);
                    }
                })
            }
            Decode::Grid8 | Decode::Grid16 | Decode::Weighted { .. } => {
                unreachable!("8-bit codes are stored or folded")
            }
        }
    }
}

/// Folds TQ+ codes of `bits` (2 or 4) into `out`, see [`Decode::Folded`].
fn fold(bits: u32, codes: &[u8], factors: &[i16], out: &mut [i8]) {
    match bits {
        2 => query2bit::fold_codes(codes, factors, out),
        4 => query4bit::fold_codes(codes, factors, out),
        _ => unreachable!("TQ+ folds codes of 2 and 4 bits, not {bits}"),
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;

    /// The vector decode against one code at a time, bitwise, at lengths with tails.
    fn check_weighted<const BITS: usize>() {
        let mut rng = StdRng::seed_from_u64(BITS as u64);
        let codebook: [f32; 16] = std::array::from_fn(|_| f32::from(rng.random::<i8>()));
        for positions in [1, 7, 8, 15, 16, 17, 100, 1030] {
            let codes: Vec<u8> = (0..(positions * BITS).div_ceil(8))
                .map(|_| rng.random())
                .collect();
            let roots: Vec<f32> = (0..positions)
                .map(|_| rng.random_range(0.0..200.0))
                .collect();
            let mut want = vec![0.0; positions];
            weighted_scalar::<BITS>(0, &codes, &codebook, &roots, &mut want);
            let mut got = vec![0.0; positions];
            weighted::<BITS>(&codes, &codebook, &roots, &mut got);
            let bits = |v: &[f32]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
            assert_eq!(
                bits(&got),
                bits(&want),
                "{BITS} bits, {positions} positions"
            );
        }
    }

    #[test]
    fn weighted_decode_matches_scalar() {
        check_weighted::<2>();
        check_weighted::<4>();
    }
}
