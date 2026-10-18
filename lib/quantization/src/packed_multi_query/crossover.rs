//! When the packed kernels beat scoring pair by pair.
//!
//! A packed block costs about as much for one query token as for a full
//! block, while scoring pair by pair costs in proportion to the tokens, so
//! small multi-vectors are faster pair by pair. Where the two meet depends on
//! the kernel, the dimension and the CPU: each [`Crossover`] is the larger of
//! the crossovers measured on AMD Milan (AVX2) and AMD Genoa (AVX-512),
//! non-decreasing in the dimension. Other CPUs keep scoring pair by pair: the
//! packed kernels have no measured SIMD variant for them.

use crate::turboquant::TQBits;

/// `(up to dim, fewest vectors)` rows by increasing dim; the last row also
/// covers any larger dim. `None`: never faster.
pub type Crossover = &'static [(usize, Option<usize>)];

/// Whether a packed multi-vector of `vectors_count` vectors of `dim` dims is
/// faster to score than its vectors one by one.
pub fn pays(crossover: Crossover, dim: usize, vectors_count: usize) -> bool {
    if !measured_cpu() {
        return false;
    }
    crossover
        .iter()
        .find(|&&(up_to, _)| dim <= up_to)
        .or(crossover.last())
        .and_then(|&(_, fewest)| fewest)
        .is_some_and(|fewest| vectors_count >= fewest)
}

fn measured_cpu() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        std::is_x86_feature_detected!("avx2")
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        false
    }
}

// Fewest vectors worth packing, from a sweep of the packed against the
// per-pair kernels on AMD Genoa (AVX-512) and AMD Milan (AVX2): 1 to 64 vectors
// against stored multi-vectors of 128, the smallest size from which every
// larger one is 1.05x faster on both.

/// Scalar int8, Dot, Cosine and L2.
pub const SCALAR_QUERY: Crossover = &[
    (128, Some(4)),
    (384, Some(6)),
    (768, Some(8)),
    (1024, Some(8)),
];
pub const SCALAR_STORED: Crossover = &[
    (128, Some(2)),
    (384, Some(4)),
    (768, Some(6)),
    (1024, Some(6)),
];
/// Scalar int8, L1.
pub const SCALAR_L1_QUERY: Crossover = &[
    (128, Some(3)),
    (384, Some(6)),
    (768, Some(12)),
    (1024, Some(12)),
];
pub const SCALAR_L1_STORED: Crossover = &[
    (128, Some(3)),
    (384, Some(4)),
    (768, Some(6)),
    (1024, Some(12)),
];
/// Binary, by storage encoding: 1, 1.5 and 2 bits.
pub const BINARY_1BIT_QUERY: Crossover = &[
    (128, Some(6)),
    (384, Some(12)),
    (768, Some(16)),
    (1024, Some(32)),
];
pub const BINARY_1BIT_STORED: Crossover = &[
    (128, Some(4)),
    (384, Some(8)),
    (768, Some(10)),
    (1024, Some(10)),
];
pub const BINARY_1_5BIT_QUERY: Crossover = &[
    (128, Some(10)),
    (384, Some(32)),
    (768, Some(32)),
    (1024, Some(32)),
];
pub const BINARY_1_5BIT_STORED: Crossover = &[
    (128, Some(8)),
    (384, Some(8)),
    (768, Some(12)),
    (1024, Some(12)),
];
pub const BINARY_2BIT_QUERY: Crossover = &[
    (128, Some(10)),
    (384, Some(16)),
    (768, Some(32)),
    (1024, Some(32)),
];
pub const BINARY_2BIT_STORED: Crossover = &[
    (128, Some(8)),
    (384, Some(10)),
    (768, Some(12)),
    (1024, Some(12)),
];
/// Binary with a 4- or 8-bit scalar query.
pub const BINARY_SCALAR_QUERY: Crossover = &[
    (128, Some(8)),
    (384, Some(16)),
    (768, Some(32)),
    (1024, Some(32)),
];
/// Product quantization; it packs queries only.
pub const PRODUCT_QUERY: Crossover = &[(128, Some(10))];

/// TurboQuant, by padded dim (1.5-bit codes pad to 1.5 values per dim). The
/// per-pair kernels of the narrow codes read several dims per byte, so the
/// packed kernel, which decodes every dim to `i16`, falls behind them as the
/// dimension grows.
pub fn turbo(bits: TQBits, stored: bool) -> Crossover {
    match (bits, stored) {
        (TQBits::Bits1, false) => &[
            (128, Some(4)),
            (384, Some(12)),
            (768, Some(32)),
            (1024, Some(48)),
        ],
        (TQBits::Bits1, true) => &[(128, Some(10)), (384, Some(32)), (768, None), (1024, None)],
        (TQBits::Bits1_5, false) => &[
            (192, Some(4)),
            (576, Some(8)),
            (1152, Some(16)),
            (1536, Some(16)),
        ],
        (TQBits::Bits1_5, true) => &[(192, Some(6)), (576, Some(32)), (1152, None), (1536, None)],
        (TQBits::Bits2, false) => &[
            (128, Some(6)),
            (384, Some(12)),
            (768, Some(32)),
            (1024, Some(32)),
        ],
        (TQBits::Bits2, true) => &[
            (128, Some(4)),
            (384, Some(6)),
            (768, Some(6)),
            (1024, Some(8)),
        ],
        (TQBits::Bits4, false) => &[
            (128, Some(6)),
            (384, Some(12)),
            (768, Some(16)),
            (1024, Some(16)),
        ],
        (TQBits::Bits4, true) => &[
            (128, Some(6)),
            (384, Some(6)),
            (768, Some(8)),
            (1024, Some(8)),
        ],
        (TQBits::Bits8, false) => &[
            (128, Some(6)),
            (384, Some(8)),
            (768, Some(16)),
            (1024, Some(16)),
        ],
        (TQBits::Bits8, true) => &[
            (128, Some(4)),
            (384, Some(6)),
            (768, Some(16)),
            (1024, Some(32)),
        ],
        (TQBits::Bits16, false) => &[(128, Some(12)), (384, Some(64)), (768, None), (1024, None)],
        (TQBits::Bits16, true) => &[(128, Some(6)), (384, Some(16)), (768, None), (1024, None)],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_by_dim() {
        let crossover: Crossover = &[(128, Some(4)), (768, Some(16)), (1024, None)];
        let pays = |dim, vectors| pays(crossover, dim, vectors);
        if !measured_cpu() {
            assert!(!pays(64, 1000));
            return;
        }
        assert!(!pays(64, 3));
        assert!(pays(64, 4));
        assert!(pays(128, 4));
        assert!(!pays(129, 15));
        assert!(pays(129, 16));
        assert!(pays(768, 16));
        assert!(!pays(769, 1000));
        assert!(!pays(4096, 1000));
    }
}
