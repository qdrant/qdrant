//! TQ+ symmetric scores of 2- and 4-bit codes, for one vector scored against many: an HNSW build
//! scores the inserted point's codes against each candidate's.
//!
//! The symmetric TQ+ score of two vectors is `Σ_j q(a, j) · q(b, j)`, where
//! `q(v, j) = fold_q15(c[v_j], D'_j)` is the signed codebook value of the code scaled by the
//! coordinate's factor ([`super::fold_q15`]). [`super::score_4bit_internal_folded`] and
//! [`super::score_2bit_internal_folded`] unpack and fold both vectors for every pair.
//! [`FoldedQuery`] folds one vector once, so that a pair only unpacks and folds the other.
//!
//! # Block order
//!
//! The kernels read codes in blocks of [`BLOCK_BYTES`] bytes and don't interleave them back into
//! dimension order: a block lists the codes at bit offset 0 of each of its bytes first, then the
//! codes at the next offset, and so on. For 4 bits that's the 32 even dimensions of the block,
//! then its 32 odd dimensions; for 2 bits the dimensions `4p`, then `4p + 1`, `4p + 2` and
//! `4p + 3` for the bytes `p` of the block. The folded values of a [`FoldedQuery`] and the factors
//! of [`BlockedFactors`] are stored in that order. The bytes after the last whole block keep their
//! natural order and are scored by the dimension-order kernels, which run SIMD in smaller steps;
//! codes without a whole block are left to those kernels entirely ([`BlockedFactors::new`] is
//! `None`). A sum of integers doesn't
//! depend on its order, so the scores equal those of the dimension-order kernels exactly.
//!
//! x86_64: AVX-512 (BW and VNNI) and AVX2; aarch64: NEON. On x86_64 CPUs with neither AVX-512
//! nor AVX2, [`FoldedQuery::new`] returns `None` and the callers score in dimension order, as
//! they do on targets without this module.

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
mod arm;
#[cfg(test)]
mod tests;
#[cfg(target_arch = "x86_64")]
mod x64;

use super::{FOLDED_MAX_DIM, SimdBackend, score_2bit_internal_folded, score_4bit_internal_folded};

/// Bytes of codes per kernel step, see [Block order](self#block-order).
pub(crate) const BLOCK_BYTES: usize = 32;

/// The code widths that TQ+ folds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FoldedBits {
    Two,
    Four,
}

impl FoldedBits {
    fn bits(self) -> usize {
        match self {
            FoldedBits::Two => 2,
            FoldedBits::Four => 4,
        }
    }

    /// Codes per byte.
    fn per_byte(self) -> usize {
        8 / self.bits()
    }

    /// The signed codebook values, the ones that the dimension-order kernels fold.
    fn signed_codebook(self) -> Vec<i8> {
        match self {
            FoldedBits::Two => super::query2bit::signed_codebook().to_vec(),
            FoldedBits::Four => super::query4bit::signed_codebook().to_vec(),
        }
    }
}

/// The kernels of [`FoldedQuery`] and [`BlockedFactors::score_symmetric`] a CPU runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Backend {
    #[cfg(target_arch = "x86_64")]
    Avx512,
    #[cfg(target_arch = "x86_64")]
    Avx2,
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    Neon,
}

impl Backend {
    /// The best backend of this CPU, `None` where only the dimension-order kernels run.
    fn detect() -> Option<Self> {
        match SimdBackend::detect() {
            #[cfg(target_arch = "x86_64")]
            SimdBackend::Avx512Vnni => Some(Backend::Avx512),
            #[cfg(target_arch = "x86_64")]
            SimdBackend::Avx2 => Some(Backend::Avx2),
            #[cfg(target_arch = "x86_64")]
            SimdBackend::Sse => None,
            #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
            SimdBackend::NeonSdot | SimdBackend::Neon => Some(Backend::Neon),
            SimdBackend::Scalar => None,
        }
    }
}

/// Where the dimensions of one vector sit in [block order](self#block-order).
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

/// The TQ+ factors `D'` in Q15 (`ErrorCorrection::d_prime_q15`) in
/// [block order](self#block-order), once per quantizer.
pub(crate) struct BlockedFactors {
    layout: Layout,
    /// One factor per dimension, in block order
    factors: Vec<i16>,
    /// The signed codebook, padded with zeros, as bytes for `pshufb` and `tbl`
    codebook_bytes: [i8; 16],
    /// The same as words, for `vpermw`
    #[cfg(target_arch = "x86_64")]
    codebook_words: [i16; 32],
    backend: Backend,
}

impl BlockedFactors {
    /// `factors` of `bits`-bit codes, given in dimension order, or `None` where the
    /// dimension-order kernels do all the work: on CPUs without a [`Backend`], and for codes
    /// without a whole block.
    ///
    /// # Panics
    ///
    /// If `factors` don't fill whole bytes of codes, or hold more than [`FOLDED_MAX_DIM`].
    pub(crate) fn new(bits: FoldedBits, factors: &[i16]) -> Option<Self> {
        Self::with_backend(bits, factors, Backend::detect()?)
    }

    fn with_backend(bits: FoldedBits, factors: &[i16], backend: Backend) -> Option<Self> {
        assert!(
            factors.len().is_multiple_of(bits.per_byte()),
            "{} factors don't fill whole bytes of {bits:?} codes",
            factors.len(),
        );
        assert!(factors.len() <= FOLDED_MAX_DIM);
        let codebook = bits.signed_codebook();
        let mut codebook_bytes = [0; 16];
        codebook_bytes[..codebook.len()].copy_from_slice(&codebook);
        #[cfg(target_arch = "x86_64")]
        let codebook_words = {
            let mut words = [0; 32];
            for (word, &value) in words.iter_mut().zip(&codebook) {
                *word = i16::from(value);
            }
            words
        };
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
            codebook_bytes,
            #[cfg(target_arch = "x86_64")]
            codebook_words,
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
        // SAFETY: `detect` found the backend's features; `a`, `b` and the factors cover
        // `layout.blocks()` blocks
        let blocked = unsafe {
            match (self.backend, layout.bits) {
                #[cfg(target_arch = "x86_64")]
                (Backend::Avx512, FoldedBits::Two) => x64::score_symmetric_2bit_avx512(self, a, b),
                #[cfg(target_arch = "x86_64")]
                (Backend::Avx512, FoldedBits::Four) => x64::score_symmetric_4bit_avx512(self, a, b),
                #[cfg(target_arch = "x86_64")]
                (Backend::Avx2, FoldedBits::Two) => x64::score_symmetric_2bit_avx2(self, a, b),
                #[cfg(target_arch = "x86_64")]
                (Backend::Avx2, FoldedBits::Four) => x64::score_symmetric_4bit_avx2(self, a, b),
                #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
                (Backend::Neon, FoldedBits::Two) => arm::score_symmetric_2bit_neon(self, a, b),
                #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
                (Backend::Neon, FoldedBits::Four) => arm::score_symmetric_4bit_neon(self, a, b),
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

/// A code vector folded once, in [block order](self#block-order), to score against others with
/// the same [`BlockedFactors`].
pub(crate) struct FoldedQuery {
    /// `q(query, j)` of the dimensions in whole blocks, in block order
    values: Vec<i16>,
    /// The codes after the last whole block
    tail: Vec<u8>,
    backend: Backend,
}

impl FoldedQuery {
    /// `codes` folded with `factors`.
    ///
    /// # Panics
    ///
    /// If `codes` don't hold one code per factor.
    pub(crate) fn new(codes: &[u8], factors: &BlockedFactors) -> Self {
        let backend = factors.backend;
        let layout = factors.layout;
        assert_eq!(
            codes.len(),
            layout.code_bytes,
            "codes don't match the factors"
        );
        let mut values = vec![0; layout.blocked_dims()];
        // SAFETY: `detect` found the backend's features; `codes`, the factors and `values` cover
        // `layout.blocks()` blocks
        unsafe {
            match (backend, layout.bits) {
                #[cfg(target_arch = "x86_64")]
                (Backend::Avx512, FoldedBits::Two) => {
                    x64::fold_2bit_avx512(factors, codes, &mut values);
                }
                #[cfg(target_arch = "x86_64")]
                (Backend::Avx512, FoldedBits::Four) => {
                    x64::fold_4bit_avx512(factors, codes, &mut values);
                }
                #[cfg(target_arch = "x86_64")]
                (Backend::Avx2, FoldedBits::Two) => {
                    x64::fold_2bit_avx2(factors, codes, &mut values)
                }
                #[cfg(target_arch = "x86_64")]
                (Backend::Avx2, FoldedBits::Four) => {
                    x64::fold_4bit_avx2(factors, codes, &mut values);
                }
                #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
                (Backend::Neon, FoldedBits::Two) => {
                    arm::fold_2bit_neon(factors, codes, &mut values);
                }
                #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
                (Backend::Neon, FoldedBits::Four) => {
                    arm::fold_4bit_neon(factors, codes, &mut values);
                }
            }
        }
        Self {
            values,
            tail: codes[layout.blocked_bytes()..].to_vec(),
            backend,
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
        // SAFETY: `detect` found the backend's features; `codes`, the factors and the values
        // cover `layout.blocks()` blocks
        let blocked = unsafe {
            match (self.backend, layout.bits) {
                #[cfg(target_arch = "x86_64")]
                (Backend::Avx512, FoldedBits::Two) => {
                    x64::score_2bit_avx512(&self.values, factors, codes)
                }
                #[cfg(target_arch = "x86_64")]
                (Backend::Avx512, FoldedBits::Four) => {
                    x64::score_4bit_avx512(&self.values, factors, codes)
                }
                #[cfg(target_arch = "x86_64")]
                (Backend::Avx2, FoldedBits::Two) => {
                    x64::score_2bit_avx2(&self.values, factors, codes)
                }
                #[cfg(target_arch = "x86_64")]
                (Backend::Avx2, FoldedBits::Four) => {
                    x64::score_4bit_avx2(&self.values, factors, codes)
                }
                #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
                (Backend::Neon, FoldedBits::Two) => {
                    arm::score_2bit_neon(&self.values, factors, codes)
                }
                #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
                (Backend::Neon, FoldedBits::Four) => {
                    arm::score_4bit_neon(&self.values, factors, codes)
                }
            }
        };
        blocked + factors.score_tail(&self.tail, &codes[layout.blocked_bytes()..])
    }
}
