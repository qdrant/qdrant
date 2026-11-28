use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use super::*;
use crate::spaces::metric::Metric;

/// Query lengths below one block, at whole blocks, and with every kind of rest after the last
/// block: none, scalar only, whole steps of the one-vector kernels, and both.
const DIMS: [usize; 12] = [1, 31, 63, 64, 65, 80, 95, 96, 100, 160, 768, 1031];

fn random_vector(rng: &mut StdRng, dim: usize) -> Vec<VectorElementType> {
    (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect()
}

/// Asserts that each score is bit for bit `DotProductMetric::similarity` of `query` and its
/// vector.
fn assert_similarities(
    label: &str,
    query: &[VectorElementType],
    vectors: &[Vec<VectorElementType>],
    scores: &[ScoreType],
) {
    assert_eq!(vectors.len(), scores.len());
    for (i, (vector, &score)) in vectors.iter().zip(scores).enumerate() {
        let expected = DotProductMetric::similarity(query, vector);
        assert_eq!(
            score.to_bits(),
            expected.to_bits(),
            "{label}, dim {}, vector {i} of {}: {score} vs {expected}",
            query.len(),
            vectors.len(),
        );
    }
}

/// The pass kernels this CPU runs, with their names.
fn supported_kernels() -> Vec<(&'static str, PassKernel)> {
    let mut kernels: Vec<(&'static str, PassKernel)> = Vec::new();
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx") && is_x86_feature_detected!("fma") {
            kernels.push(("avx", avx::dot_similarity_blocked_avx));
            if is_x86_feature_detected!("avx512f") {
                kernels.push(("avx512", avx512::dot_similarity_blocked_avx512));
            }
        }
    }
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            kernels.push(("neon", neon::dot_similarity_blocked_neon));
        }
    }
    kernels
}

#[test]
fn pass_kernels_match_similarity() {
    let mut rng = StdRng::seed_from_u64(42);
    for (name, kernel) in supported_kernels() {
        for dim in DIMS.into_iter().filter(|&dim| dim >= BLOCK) {
            for count in 1..=MAX_PASS {
                let query = random_vector(&mut rng, dim);
                let vectors: Vec<_> = (0..count).map(|_| random_vector(&mut rng, dim)).collect();
                let pass: Vec<&[VectorElementType]> = vectors.iter().map(Vec::as_slice).collect();
                let mut scores = vec![ScoreType::NAN; count];
                // SAFETY: the kernel is supported, the query has at least one block, and the
                // pass has at most `MAX_PASS` vectors of the query's length.
                unsafe { kernel(&query, &pass, &mut scores) };
                assert_similarities(name, &query, &vectors, &scores);
            }
        }
    }
}

#[test]
fn dot_similarity_batch_matches_similarity() {
    let mut rng = StdRng::seed_from_u64(42);
    for dim in DIMS {
        // Single vectors, one pass, and several passes of equal size
        for count in [0, 1, 2, 16, 17, 33, 64] {
            let query = random_vector(&mut rng, dim);
            let vectors: Vec<_> = (0..count).map(|_| random_vector(&mut rng, dim)).collect();
            let mut scores = vec![ScoreType::NAN; count];
            dot_similarity_batch(&query, &vectors, &mut scores);
            assert_similarities("batch", &query, &vectors, &scores);
        }
    }
}
