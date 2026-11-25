//! SIMD query-encoding + dot-product routines, one submodule per bit-width.
//!
//! Every `query{N}bit` submodule exposes two public entry points:
//!
//! * [`Query{N}bitSimd`](query4bit::Query4bitSimd) — a rotation-applied query
//!   precomputed for fast asymmetric scoring (original-query × PQ-vector):
//!   the shared [`query::QuerySimd`] kernels instantiated at the width, which
//!   dispatch to the best SIMD backend available on the host CPU.
//! * [`score_{N}bit_internal`](query4bit::score_4bit_internal) — dot product of
//!   two already-encoded PQ vectors (symmetric scoring), same runtime dispatch.
//!
//! The 2- and 4-bit submodules also score TQ+ symmetrically, with each code scaled by its
//! coordinate's factor: `score_{N}bit_internal_folded` in dimension order, and block-order
//! kernels (`*_blocked*`) for one vector scored against many, behind [`BlockedFactors`] and
//! [`FoldedQuery`] here.
//!
//! Available SIMD backends:
//!
//! | Path              | x86_64                                | aarch64           |
//! |-------------------|---------------------------------------|-------------------|
//! | asymmetric, 1/2/4/8 | AVX-512 VNNI, AVX2, SSE4.1+SSSE3    | NEON + SDOT, NEON |
//! | symmetric, 1      | AVX-512 VPOPCNTDQ, AVX2, SSE4.1+SSSE3 | NEON              |
//! | symmetric, 2/4    | AVX-512 VNNI, AVX2, SSE4.1+SSSE3      | NEON + SDOT, NEON |
//! | TQ+ symmetric, 2/4 | AVX2, SSE4.1+SSSE3                   | NEON              |
//! | TQ+ symmetric, 2/4, block order | AVX-512 BW + VNNI, AVX2 | NEON              |
//! | symmetric, 8      | AVX-512 VNNI, AVX2, SSE4.1            | NEON + SDOT, NEON |
//! | 16, both paths    | AVX-512BW, AVX2, SSE4.1               | NEON              |
//!
//! On any other target the scalar reference kernels take over.

pub(crate) mod grid;
pub mod hadamard;
pub mod query;
pub mod query16bit;
pub mod query1bit;
pub mod query2bit;
pub mod query4bit;
pub mod query8bit;

/// Best multiply-accumulate backend the host CPU supports, in preference
/// order AVX-512 VNNI → AVX2 → SSE → NEON + SDOT → NEON → scalar.  Shared by
/// every kernel built on `u8 × i8` / `i8 × i8` products (the asymmetric
/// paths of every width and the symmetric 2- and 4-bit paths); resolve it
/// once with [`SimdBackend::detect`] and dispatch on the value, so a scoring
/// loop doesn't re-run CPU feature detection per vector.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SimdBackend {
    #[cfg(target_arch = "x86_64")]
    Avx512Vnni,
    #[cfg(target_arch = "x86_64")]
    Avx2,
    #[cfg(target_arch = "x86_64")]
    Sse,
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    NeonSdot,
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    Neon,
    Scalar,
}

impl SimdBackend {
    pub(crate) fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            // Every wider backend's kernels also enable these, for their 128-bit helpers
            let sse =
                std::is_x86_feature_detected!("sse4.1") && std::is_x86_feature_detected!("ssse3");
            if sse
                && std::is_x86_feature_detected!("avx512f")
                && std::is_x86_feature_detected!("avx512bw")
                && std::is_x86_feature_detected!("avx512vnni")
            {
                return SimdBackend::Avx512Vnni;
            }
            if sse && std::is_x86_feature_detected!("avx2") {
                return SimdBackend::Avx2;
            }
            if sse {
                return SimdBackend::Sse;
            }
        }
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            if std::arch::is_aarch64_feature_detected!("dotprod") {
                return SimdBackend::NeonSdot;
            }
            return SimdBackend::Neon;
        }
        #[allow(unreachable_code)]
        SimdBackend::Scalar
    }
}

/// A TQ+ code's signed codebook value `value` scaled by its coordinate's Q15 factor `factor`:
/// `value · factor / 2¹⁵`, rounded half up. That's what `pmulhrsw` on x86_64 and `vqrdmulh` on
/// aarch64 compute, so the scalar and SIMD paths of the folded kernels agree bit for bit. With
/// `|value| ≤ 128` and `0 ≤ factor < 2¹⁵` the result stays an i8.
#[inline(always)]
pub(crate) fn fold_q15(value: i64, factor: i16) -> i64 {
    (value * i64::from(factor) + (1 << 14)) >> 15
}

/// The folded values of TQ+ codes of `BITS` (2 or 4) each, one per position of `out`, as
/// bytes XORed with `flip`: 0 for the values as `i8`, 0x80 for `value + 128` as the 8-bit grid
/// stores them. `value` is a code's signed codebook value. `simd` folds whole groups from the
/// start, with the slices as given here, and returns the positions done.
///
/// # Panics
///
/// If `codes` hold fewer than `out.len()` codes, or `factors` fewer factors.
#[inline]
pub(crate) fn fold_codes<const BITS: usize>(
    codes: &[u8],
    factors: &[i16],
    out: &mut [u8],
    flip: u8,
    value: fn(u8) -> i64,
    simd: impl FnOnce(&[u8], &[i16], &mut [u8], u8) -> usize,
) {
    let per_byte = 8 / BITS;
    assert!(out.len() <= per_byte * codes.len() && out.len() <= factors.len());
    let done = simd(codes, factors, out, flip);
    debug_assert!(done.is_multiple_of(per_byte));
    let mask = (1 << BITS) - 1;
    let rest = out[done..]
        .chunks_mut(per_byte)
        .zip(factors[done..].chunks(per_byte))
        .zip(&codes[done / per_byte..]);
    for ((out, factors), &byte) in rest {
        for (k, (o, &factor)) in out.iter_mut().zip(factors).enumerate() {
            let code = (byte >> (BITS * k)) & mask;
            // The low byte of the value, an i8
            *o = fold_q15(value(code), factor) as u8 ^ flip;
        }
    }
}

/// The longest vectors the folded kernels take: they sum products of at most 128² in i32,
/// which holds 2¹⁶ coordinates (Qdrant's largest dimension) with room to spare.
pub(crate) const FOLDED_MAX_DIM: usize = 1 << 16;

/// Bytes of codes per step of the block-order kernels, see [`BlockedFactors`].
pub(crate) const BLOCK_BYTES: usize = 32;

/// The code widths whose TQ+ symmetric score has block-order kernels, see [`BlockedFactors`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FoldedBits {
    Two,
    Four,
}

impl FoldedBits {
    /// Codes per byte.
    fn per_byte(self) -> usize {
        match self {
            FoldedBits::Two => 4,
            FoldedBits::Four => 2,
        }
    }
}

/// The backends of the block-order kernels of [`BlockedFactors`] and [`FoldedQuery`]. None on
/// x86_64 CPUs with neither AVX-512 nor AVX2, nor on targets without SIMD kernels: there the
/// dimension-order kernels score.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BlockedBackend {
    #[cfg(target_arch = "x86_64")]
    Avx512,
    #[cfg(target_arch = "x86_64")]
    Avx2,
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    Neon,
}

impl BlockedBackend {
    /// The best block-order backend of this CPU, `None` where only the dimension-order kernels
    /// run.
    fn detect() -> Option<Self> {
        match SimdBackend::detect() {
            #[cfg(target_arch = "x86_64")]
            SimdBackend::Avx512Vnni => Some(BlockedBackend::Avx512),
            #[cfg(target_arch = "x86_64")]
            SimdBackend::Avx2 => Some(BlockedBackend::Avx2),
            #[cfg(target_arch = "x86_64")]
            SimdBackend::Sse => None,
            #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
            SimdBackend::NeonSdot | SimdBackend::Neon => Some(BlockedBackend::Neon),
            SimdBackend::Scalar => None,
        }
    }
}

/// Where the dimensions of one vector sit in [block order](BlockedFactors#block-order).
#[derive(Clone, Copy, Debug)]
struct Layout {
    bits: FoldedBits,
    code_bytes: usize,
}

impl Layout {
    /// Whole blocks; the bytes after them keep their natural order.
    fn blocks(self) -> usize {
        self.code_bytes / BLOCK_BYTES
    }

    fn blocked_bytes(self) -> usize {
        self.blocks() * BLOCK_BYTES
    }

    fn blocked_dims(self) -> usize {
        self.blocks() * BLOCK_BYTES * self.bits.per_byte()
    }

    /// The dimension at `position` in block order.
    fn dim(self, position: usize) -> usize {
        if position >= self.blocked_dims() {
            return position;
        }
        let per_byte = self.bits.per_byte();
        let block_dims = BLOCK_BYTES * per_byte;
        let (block, within) = (position / block_dims, position % block_dims);
        let (offset, byte) = (within / BLOCK_BYTES, within % BLOCK_BYTES);
        block * block_dims + per_byte * byte + offset
    }
}

/// The TQ+ factors `D'` in Q15 (`ErrorCorrection::d_prime_q15`) of 2- or 4-bit codes in block
/// order, once per quantizer, for the block-order kernels of the TQ+ symmetric score
/// `Σ_j q(a, j) · q(b, j)`, `q(v, j) = fold_q15(c[v_j], D'_j)` ([`fold_q15`]).
///
/// [`score_2bit_internal_folded`] and [`score_4bit_internal_folded`] compute that score in
/// dimension order, unpacking and folding both vectors for every pair.
/// [`Self::score_symmetric`] computes it in block order, and a [`FoldedQuery`] folds one vector
/// once, so that a pair only unpacks and folds the other: an HNSW build scores the inserted
/// point's codes against each candidate's.
///
/// # Block order
///
/// The kernels (`query{2,4}bit`, `*_blocked_*`) read codes in blocks of [`BLOCK_BYTES`] bytes
/// and don't interleave them back into dimension order: a block lists the codes at bit offset 0
/// of each of its bytes first, then the codes at the next offset, and so on. For 4 bits that's
/// the 32 even dimensions of the block, then its 32 odd dimensions; for 2 bits the dimensions
/// `4p`, then `4p + 1`, `4p + 2` and `4p + 3` for the bytes `p` of the block. The factors and the
/// folded values of a [`FoldedQuery`] are stored in that order. The bytes after the last whole
/// block keep their natural order and are scored by the dimension-order kernels, which run SIMD
/// in smaller steps; codes without a whole block are left to those kernels entirely
/// ([`Self::new`] is `None`). A sum of integers doesn't depend on its order, so the scores equal
/// those of the dimension-order kernels exactly.
///
/// Backends: AVX-512 (BW and VNNI) and AVX2 on x86_64, NEON on aarch64. On x86_64 CPUs with
/// neither AVX-512 nor AVX2, and on other targets, [`Self::new`] is `None` and the callers score
/// in dimension order.
pub(crate) struct BlockedFactors {
    layout: Layout,
    /// One factor per dimension, in block order
    factors: Vec<i16>,
    backend: BlockedBackend,
}

impl BlockedFactors {
    /// `factors` of `bits`-bit codes, given in dimension order, or `None` where the
    /// dimension-order kernels do all the work: on CPUs without a [`BlockedBackend`], and for
    /// codes without a whole block.
    ///
    /// # Panics
    ///
    /// If `factors` don't fill whole bytes of codes, or hold more than [`FOLDED_MAX_DIM`].
    pub(crate) fn new(bits: FoldedBits, factors: &[i16]) -> Option<Self> {
        Self::with_backend(bits, factors, BlockedBackend::detect()?)
    }

    /// [`Self::new`] with `backend`, which must run on this CPU.
    fn with_backend(bits: FoldedBits, factors: &[i16], backend: BlockedBackend) -> Option<Self> {
        assert!(
            factors.len().is_multiple_of(bits.per_byte()),
            "{} factors don't fill whole bytes of {bits:?} codes",
            factors.len(),
        );
        assert!(factors.len() <= FOLDED_MAX_DIM);
        let layout = Layout {
            bits,
            code_bytes: factors.len() / bits.per_byte(),
        };
        if layout.blocks() == 0 {
            return None;
        }
        Some(Self {
            layout,
            factors: (0..factors.len())
                .map(|position| factors[layout.dim(position)])
                .collect(),
            backend,
        })
    }

    /// Heap memory in bytes.
    pub(crate) fn ram_usage(&self) -> usize {
        self.factors.capacity() * size_of::<i16>()
    }

    /// `Σ q(a, j) · q(b, j)` of two code vectors, as the dimension-order kernels compute it.
    ///
    /// # Panics
    ///
    /// If `a` or `b` don't hold one code per factor.
    pub(crate) fn score_symmetric(&self, a: &[u8], b: &[u8]) -> i64 {
        let layout = self.layout;
        assert_eq!(
            a.len(),
            layout.code_bytes,
            "codes of a don't match the factors"
        );
        assert_eq!(
            b.len(),
            layout.code_bytes,
            "codes of b don't match the factors"
        );
        // SAFETY: `a`, `b` and the factors cover `layout.blocks()` blocks
        let blocked = unsafe {
            match layout.bits {
                FoldedBits::Two => query2bit::score_2bit_internal_blocked(self, a, b),
                FoldedBits::Four => query4bit::score_4bit_internal_blocked(self, a, b),
            }
        };
        let tail_start = layout.blocked_bytes();
        blocked + self.score_tail(&a[tail_start..], &b[tail_start..])
    }

    /// `Σ q(a, j) · q(b, j)` of the bytes after the last whole block, in dimension order.
    fn score_tail(&self, a: &[u8], b: &[u8]) -> i64 {
        if a.is_empty() {
            return 0;
        }
        let factors = &self.factors[self.layout.blocked_dims()..];
        match self.layout.bits {
            FoldedBits::Two => score_2bit_internal_folded(a, b, factors),
            FoldedBits::Four => score_4bit_internal_folded(a, b, factors),
        }
    }
}

/// A code vector folded once, in [block order](BlockedFactors#block-order), to score against
/// others with the same [`BlockedFactors`].
pub(crate) struct FoldedQuery {
    /// `q(query, j)` of the dimensions in whole blocks, in block order
    values: Vec<i16>,
    /// The codes after the last whole block
    tail: Vec<u8>,
}

impl FoldedQuery {
    /// `codes` folded with `factors`.
    ///
    /// # Panics
    ///
    /// If `codes` don't hold one code per factor.
    pub(crate) fn new(codes: &[u8], factors: &BlockedFactors) -> Self {
        let layout = factors.layout;
        assert_eq!(
            codes.len(),
            layout.code_bytes,
            "codes don't match the factors"
        );
        let mut values = vec![0; layout.blocked_dims()];
        // SAFETY: `codes`, the factors and `values` cover `layout.blocks()` blocks
        unsafe {
            match layout.bits {
                FoldedBits::Two => query2bit::fold_2bit_blocked(factors, codes, &mut values),
                FoldedBits::Four => query4bit::fold_4bit_blocked(factors, codes, &mut values),
            }
        }
        Self {
            values,
            tail: codes[layout.blocked_bytes()..].to_vec(),
        }
    }

    /// `Σ q(query, j) · q(codes, j)`, as the dimension-order kernels compute it.
    ///
    /// # Panics
    ///
    /// If `codes` don't match the query, or `factors` aren't the ones it was folded with.
    pub(crate) fn score(&self, codes: &[u8], factors: &BlockedFactors) -> i64 {
        let layout = factors.layout;
        assert_eq!(
            self.values.len(),
            layout.blocked_dims(),
            "factors don't match the query"
        );
        assert_eq!(
            codes.len(),
            layout.code_bytes,
            "codes don't match the query"
        );
        // SAFETY: `codes`, the factors and the values cover `layout.blocks()` blocks
        let blocked = unsafe {
            match layout.bits {
                FoldedBits::Two => query2bit::score_2bit_blocked(&self.values, factors, codes),
                FoldedBits::Four => query4bit::score_4bit_blocked(&self.values, factors, codes),
            }
        };
        blocked + factors.score_tail(&self.tail, &codes[layout.blocked_bytes()..])
    }
}

// Re-exports below include the runtime-dispatching entry points used by the
// crate's scoring paths (`Query{N}bitSimd`, `score_{N}bit_internal`) plus
// scalar-reference and arch-specific kernels the benchmarks at
// `benches/turbo_simd.rs` target directly.  Every symbol here is consumed
// either by `turboquant::quantization` inside the crate or by benches/
// outside — narrowing them to `pub(crate)` would break the bench build.
pub use query::QuerySimd;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use query1bit::score_1bit_internal_neon;
pub use query1bit::{
    Query1bitSimd, Query1bitWideSimd, score_1bit_internal, score_1bit_internal_scalar,
};
#[cfg(target_arch = "x86_64")]
pub use query1bit::{
    score_1bit_internal_avx2, score_1bit_internal_avx512_vpopcntdq, score_1bit_internal_sse,
};
pub use query2bit::{
    CODEBOOK_SCALE_SQ as CODEBOOK_SCALE_SQ_2BIT, Query2bitSimd, score_2bit_internal,
    score_2bit_internal_folded, score_2bit_internal_folded_scalar, score_2bit_internal_scalar,
};
#[cfg(target_arch = "x86_64")]
pub use query2bit::{
    score_2bit_internal_avx2, score_2bit_internal_avx512_vnni, score_2bit_internal_sse,
};
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use query2bit::{score_2bit_internal_neon, score_2bit_internal_neon_sdot};
pub use query4bit::{
    CODEBOOK_SCALE_SQ as CODEBOOK_SCALE_SQ_4BIT, Query4bitSimd, score_4bit_internal,
    score_4bit_internal_folded, score_4bit_internal_folded_scalar, score_4bit_internal_scalar,
};
#[cfg(target_arch = "x86_64")]
pub use query4bit::{
    score_4bit_internal_avx2, score_4bit_internal_avx512_vnni, score_4bit_internal_sse,
};
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use query4bit::{score_4bit_internal_neon, score_4bit_internal_neon_sdot};
pub use query8bit::{Query8bitSimd, score_8bit_internal, score_8bit_internal_scalar};
#[cfg(target_arch = "x86_64")]
pub use query8bit::{
    score_8bit_internal_avx2, score_8bit_internal_avx512_vnni, score_8bit_internal_sse,
};
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use query8bit::{score_8bit_internal_neon, score_8bit_internal_neon_sdot};
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub use query16bit::score_16bit_internal_neon;
pub use query16bit::{Query16bitSimd, score_16bit_internal, score_16bit_internal_scalar};
#[cfg(target_arch = "x86_64")]
pub use query16bit::{
    score_16bit_internal_avx2, score_16bit_internal_avx512, score_16bit_internal_sse,
};

/// Test-only helpers shared by every `query{N}bit` submodule.
///
/// Per-bit-width specifics (`PARITY_DIMS`, `random_inputs`) still live in
/// their own module's `shared` — this holds only the genuinely
/// bit-width-agnostic pieces.
#[cfg(test)]
mod shared {
    use common::bitpacking::BitWriter;
    use rand::RngExt;
    use rand::prelude::StdRng;
    use rand_distr::{Distribution, StandardNormal};

    /// Uniformly random bytes — used directly by 1-bit tests (where bytes *are*
    /// the packed form) and indirectly via [`pack_codes`] for wider widths.
    pub fn random_bytes(rng: &mut StdRng, len: usize) -> Vec<u8> {
        (0..len).map(|_| rng.random_range(0..=u8::MAX)).collect()
    }

    /// `len` samples from N(0, 1).
    pub fn sample_normal_vec(rng: &mut StdRng, len: usize) -> Vec<f32> {
        (0..len).map(|_| StandardNormal.sample(rng)).collect()
    }

    /// Map each raw float to the index of its nearest centroid.
    pub fn encode_to_nearest_centroid(centroids: &[f32], raw: &[f32]) -> Vec<u8> {
        raw.iter()
            .map(|&v| {
                centroids
                    .iter()
                    .enumerate()
                    .min_by(|a, b| (a.1 - v).abs().partial_cmp(&(b.1 - v).abs()).unwrap())
                    .map(|(k, _)| k as u8)
                    .unwrap()
            })
            .collect()
    }

    /// Pack `indices` into bytes with `bits` bits per code, LSB-first — same
    /// layout [`crate::turboquant::quantization::TurboQuantizer::pack_vector`]
    /// produces (both go through [`BitWriter`]).  Caller guarantees every
    /// index fits in `bits` bits and `indices.len() * bits` is a multiple of 8.
    pub fn pack_codes(indices: &[u8], bits: u8) -> Vec<u8> {
        let mut out = Vec::with_capacity((indices.len() * bits as usize).div_ceil(8));
        let mut writer = BitWriter::new(&mut out);
        for &idx in indices {
            writer.write(idx, bits);
        }
        writer.finish();
        out
    }
}

/// Every backend of the block-order kernels against the dimension-order reference.
#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{RngExt as _, SeedableRng as _};

    use super::{
        BLOCK_BYTES, BlockedBackend, BlockedFactors, FoldedBits, FoldedQuery,
        score_2bit_internal_folded_scalar, score_4bit_internal_folded_scalar,
    };

    /// The backends this CPU runs.
    #[cfg(target_arch = "x86_64")]
    fn backends() -> Vec<BlockedBackend> {
        let mut backends = Vec::new();
        if std::is_x86_feature_detected!("avx2") {
            backends.push(BlockedBackend::Avx2);
        }
        if BlockedBackend::detect() == Some(BlockedBackend::Avx512) {
            backends.push(BlockedBackend::Avx512);
        }
        backends
    }

    /// The backends this CPU runs: NEON is part of the target, so [`BlockedBackend::detect`] finds it.
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    fn backends() -> Vec<BlockedBackend> {
        assert_eq!(BlockedBackend::detect(), Some(BlockedBackend::Neon));
        vec![BlockedBackend::Neon]
    }

    /// The dimension-order reference.
    fn reference(bits: FoldedBits, a: &[u8], b: &[u8], factors: &[i16]) -> i64 {
        match bits {
            FoldedBits::Two => score_2bit_internal_folded_scalar(a, b, factors),
            FoldedBits::Four => score_4bit_internal_folded_scalar(a, b, factors),
        }
    }

    /// Every backend scores a folded query and two code vectors exactly as the dimension-order
    /// kernels, with whole blocks and with bytes after them. Codes without a whole block have no
    /// [`BlockedFactors`]: the dimension-order kernels score them.
    #[rstest::rstest]
    fn folded_scores_equal_dimension_order(
        #[values(FoldedBits::Two, FoldedBits::Four)] bits: FoldedBits,
        #[values(1, 5, 31, 32, 33, 64, 100, 768)] code_bytes: usize,
    ) {
        let mut rng = StdRng::seed_from_u64(code_bytes as u64);
        let dims = code_bytes * bits.per_byte();
        let factors: Vec<i16> = (0..dims).map(|_| rng.random_range(0..=i16::MAX)).collect();
        let vectors: Vec<Vec<u8>> = (0..8)
            .map(|_| (0..code_bytes).map(|_| rng.random()).collect())
            .collect();

        for backend in backends() {
            let blocked = BlockedFactors::with_backend(bits, &factors, backend);
            if code_bytes < BLOCK_BYTES {
                assert!(blocked.is_none(), "{backend:?}, {bits:?}");
                continue;
            }
            let blocked = blocked.unwrap();
            for a in &vectors {
                let query = FoldedQuery::new(a, &blocked);
                for b in &vectors {
                    let expected = reference(bits, a, b, &factors);
                    assert_eq!(query.score(b, &blocked), expected, "{backend:?}, {bits:?}");
                    assert_eq!(
                        blocked.score_symmetric(a, b),
                        expected,
                        "{backend:?}, {bits:?}"
                    );
                }
            }
        }
    }

    /// The largest folded values and factors, where the i32 lanes sum the most.
    #[rstest::rstest]
    fn folded_scores_extremes(#[values(FoldedBits::Two, FoldedBits::Four)] bits: FoldedBits) {
        let code_bytes = 4096 / bits.per_byte();
        let factors = vec![i16::MAX; code_bytes * bits.per_byte()];
        // Code 0 is the most negative codebook value, all ones the most positive
        for (byte_a, byte_b) in [(0x00, 0x00), (0xFF, 0xFF), (0x00, 0xFF)] {
            let a = vec![byte_a; code_bytes];
            let b = vec![byte_b; code_bytes];
            let expected = reference(bits, &a, &b, &factors);
            for backend in backends() {
                let blocked = BlockedFactors::with_backend(bits, &factors, backend).unwrap();
                let query = FoldedQuery::new(&a, &blocked);
                assert_eq!(query.score(&b, &blocked), expected, "{backend:?}, {bits:?}");
                assert_eq!(blocked.score_symmetric(&a, &b), expected);
            }
        }
    }
}
