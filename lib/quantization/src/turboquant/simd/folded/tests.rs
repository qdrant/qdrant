use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};

use super::{BLOCK_BYTES, Backend, BlockedFactors, FoldedBits, FoldedQuery};
use crate::turboquant::simd::{
    score_2bit_internal_folded_scalar, score_4bit_internal_folded_scalar,
};

/// The backends this CPU runs.
#[cfg(target_arch = "x86_64")]
fn backends() -> Vec<Backend> {
    let mut backends = Vec::new();
    if std::is_x86_feature_detected!("avx2") {
        backends.push(Backend::Avx2);
    }
    if Backend::detect() == Some(Backend::Avx512) {
        backends.push(Backend::Avx512);
    }
    backends
}

/// The backends this CPU runs: NEON is part of the target, so [`Backend::detect`] finds it.
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
fn backends() -> Vec<Backend> {
    assert_eq!(Backend::detect(), Some(Backend::Neon));
    vec![Backend::Neon]
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
