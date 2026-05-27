use serde::{Deserialize, Serialize};

use crate::turboquant::simd::{Query1bitSimd, Query2bitSimd, Query4bitSimd};

pub mod encoding;
pub mod lloyd_max;
pub mod math;
mod permutation;
pub mod quantization;
pub mod rotation;
pub mod simd;

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TQBits {
    Bits4,
    Bits2,
    Bits1_5,
    Bits1,
}

impl TQBits {
    #[inline]
    fn bit_size(&self) -> u8 {
        match self {
            TQBits::Bits4 => 4,
            TQBits::Bits2 => 2,
            // 1.5 bits is implemented as 1 bit with x1.5 dimension padding
            TQBits::Bits1_5 => 1,
            TQBits::Bits1 => 1,
        }
    }

    /// Number of input vectors the TQ+ pre-pass uniformly samples and
    /// streams into the per-coord P-square estimators, scaled to the
    /// extremity of the target probability for this codebook.
    ///
    /// The TQ+ anchor probability is `p_outer = Φ(c_outer)` where
    /// `c_outer` is the outermost centroid magnitude. The variance of an
    /// order-statistic estimator at quantile `p` over a sample of size
    /// `R` is `p(1-p) / (R · f(F⁻¹(p))²)` where `f` is the source PDF.
    /// For N(0, 1)-like post-rotation data this gives:
    ///
    /// | Bits | p_outer | f(F⁻¹) | R    | σ     | rel. error |
    /// |------|---------|--------|------|-------|------------|
    /// | 1    | 0.787   | 0.290  | 2048 | 0.031 | 3.9%       |
    /// | 1.5  | 0.787   | 0.290  | 2048 | 0.031 | 3.9%       |
    /// | 2    | 0.934   | 0.128  | 4096 | 0.030 | 2.0%       |
    /// | 4    | 0.997   | 0.010  | 8192 | 0.063 | 2.3%       |
    ///
    /// We pick `R` per codebook so the absolute σ stays roughly flat
    /// (~0.03–0.06 in N(0, 1) units) instead of forcing the highest-bit
    /// budget on every codebook. Bits1/Bits1_5 sit at a moderate quantile
    /// where `R = 2048` is plenty; Bits4 needs `R = 8192` because its
    /// anchor sits in the deep tail.
    ///
    /// Memory: the pre-pass holds two P-square estimators per coord plus
    /// a `BATCH_SIZE × padded_dim` rotation scratch — total ≈ 400 KB at
    /// `padded_dim = 1536`, **independent of `R`**. Wall-time scales
    /// roughly linearly in `R`.
    #[inline]
    pub(crate) fn sample_size(&self) -> usize {
        match self {
            TQBits::Bits1 | TQBits::Bits1_5 => 2_048,
            TQBits::Bits2 => 4_096,
            TQBits::Bits4 => 8_192,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TQMode {
    Normal,
    Plus,
}

/// Encoded query type for Turbo Quant.
pub struct EncodedQueryTQ {
    /// SIMD-encoded query for the asymmetric scoring path. For TQ+, the
    /// rotated query is pre-multiplied by `D' = 1/scale` per coord so the
    /// SIMD raw_dot computes `⟨Q · D', X+⟩` directly — the asymmetric score
    /// formulas then just add `ec_correction` and apply renorm's
    /// `scaling_factor`, identical to the Normal-mode path.
    data: EncodedQueryTQData,

    // Store the original query's l2 norm for Dot and L2 distances, where we can compute it once and reuse for all distance computations.
    l2_norm: Option<f32>,

    // Store the original query in pre-rotated form for L1 distance, where we need to dequantize vectors and apply inverse rotation to them.
    query: Option<Vec<f32>>,

    /// TQ+ asymmetric-scoring scalar correction `qm = ⟨Q, M⟩ = -⟨rotated_q, shift⟩`.
    /// `0.0` when EC is not configured. Added to the SIMD raw_dot so the
    /// existing score formulas stay unchanged.
    ec_correction: f32,
}

/// SIMD-ready encoded query, one variant per supported bit-width.  Each
/// variant wraps the bit-width's [`simd::Query{N}bitSimd`] precomputation
/// (rotation-applied query, quantized to the SIMD-friendly integer form);
/// on architectures without a matching SIMD instruction set the scalar
/// reference kernel inside each type takes over automatically.
///
/// `Bits1Wide` is the same kernel as `Bits1` but with 16-bit query
/// quantization instead of the default 8-bit (the kernel's max). Used in
/// TQ+ for 1-bit storage: the per-coord `D'` pre-scaling pushes some query
/// coords into the bottom of the 8-bit integer range, where rounding noise
/// is large relative to the signal — 16 bits gives the most headroom the
/// existing kernel supports.
pub enum EncodedQueryTQData {
    Bits1(Query1bitSimd),
    Bits1Wide(Query1bitSimd<16>),
    Bits2(Query2bitSimd),
    Bits4(Query4bitSimd),
}
