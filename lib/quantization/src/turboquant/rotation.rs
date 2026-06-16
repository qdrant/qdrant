use crate::turboquant::permutation::Permutation;
use crate::turboquant::simd;

const N_PERMUTATIONS: usize = 3;

/// Random seeds for Permutation. The values were picked arbitrarily.
///
/// WARNING: DO NOT CHANGE THESE VALUES. They are baked into the encoding of every quantized vector.
/// Changing them would silently corrupt all existing quantized vectors.
const PERMUTATION_SEEDS: [u64; 3] = [654605292835415893, 8636605637963351413, 1775280196666917949];

/// Hadamard rotation implementation customized for TurboQuant.
pub struct HadamardRotation {
    /// Random (but shared) permutations.
    permutations: [Permutation; N_PERMUTATIONS],

    /// Original dimension.
    dim: usize,
}

impl HadamardRotation {
    pub fn new(dim: usize) -> Self {
        let permutations: [_; N_PERMUTATIONS] =
            std::array::from_fn(|index| Permutation::new_reversible(PERMUTATION_SEEDS[index], dim));

        Self { permutations, dim }
    }

    pub fn apply(&self, x: &mut [f64]) {
        debug_assert_eq!(x.len(), self.dim);
        apply_rotation_with_permutations(x, &self.permutations);
    }

    pub fn apply_inverse(&self, y: &mut [f64]) {
        debug_assert_eq!(y.len(), self.dim);
        apply_inverse_rotation_with_permutations(y, &self.permutations);
    }
}

/// In-place unnormalized Walsh-Hadamard Transform in f64.
///
/// The transform is its own inverse: calling it twice recovers the original data (up to a scale factor).
/// Normalization is applied externally in `apply`/`apply_inverse`.
///
/// Input length must be a power of 2.
pub fn in_place_walsh_hadamard_transform(x: &mut [f64]) {
    let n = x.len();
    debug_assert!(n.is_power_of_two(), "WHT requires power-of-2 length");
    let mut h = 1;
    while h < n {
        for i in (0..n).step_by(h * 2) {
            for j in i..i + h {
                let a = x[j];
                let b = x[j + h];
                x[j] = a + b;
                x[j + h] = a - b;
            }
        }
        h *= 2;
    }
}

/// Randomly rotates `x` in place. Produces bit-identical output to
/// [`HadamardRotation::apply`]; invert with [`HadamardRotation::apply_inverse`].
pub fn random_vector_rotation(x: &mut [f64]) {
    let dim = x.len();

    // Building the permutations on every call is cheap: `new_one_way` skips the LCG warm-up.
    let permutations: [_; N_PERMUTATIONS] =
        std::array::from_fn(|index| Permutation::new_one_way(PERMUTATION_SEEDS[index], dim));

    apply_rotation_with_permutations(x, &permutations);
}

/// Inverts the rotated `y` back. Produces bit-identical output to
/// [`HadamardRotation::apply_inverse`].
pub fn random_vector_rotation_inverse(y: &mut [f64]) {
    let dim = y.len();

    // `new_reversible` does an O(dim) LCG warm-up to record `end_state`, which
    // `unpermute` needs. Cost is paid once per call; no heap allocation.
    let permutations: [_; N_PERMUTATIONS] =
        std::array::from_fn(|index| Permutation::new_reversible(PERMUTATION_SEEDS[index], dim));

    apply_inverse_rotation_with_permutations(y, &permutations);
}

/// Decompose `dim` into a sequence of decreasing power-of-2 chunk sizes
/// that sum to exactly `dim`. No padding is needed.
///
/// Greedily takes the largest power-of-2 that fits the remaining length.
///
/// ```text
///  dim  | chunks
/// ------|--------
///   128 | [128]
///   300 | [256, 32, 8, 4]
///   700 | [512, 128, 32, 16, 8, 4]
///  1536 | [1024, 512]
///  4096 | [4096]
/// ```
fn compute_chunk_sizes(dim: usize) -> impl Iterator<Item = usize> {
    debug_assert!(dim > 0);

    let mut bits = dim;
    std::iter::from_fn(move || {
        if bits == 0 {
            return None;
        }

        let highest = 1 << bits.ilog2();
        bits ^= highest;
        Some(highest)
    })
}

/// Apply a Hadamard rotation to `x` using the given `permutations`.
fn apply_rotation_with_permutations(x: &mut [f64], permutations: &[Permutation; N_PERMUTATIONS]) {
    // Apply WHT + normalize to each variable-size chunk.
    wht_normalized_chunks(x);

    // Permute then WHT+normalize for each permutation.
    for permutation in permutations {
        permutation.permute(x);
        wht_normalized_chunks(x);
    }
}

/// Apply the inverse of a Hadamard rotation to `y` using the given `permutations`.
fn apply_inverse_rotation_with_permutations(
    y: &mut [f64],
    permutations: &[Permutation; N_PERMUTATIONS],
) {
    // WHT + normalize
    wht_normalized_chunks(y);

    // Apply inverse permutations backwards.
    for permutation in permutations.iter().rev() {
        permutation.unpermute(y);
        wht_normalized_chunks(y);
    }
}

/// Apply WHT + normalization to variable-size chunks.
fn wht_normalized_chunks(buf: &mut [f64]) {
    let mut offset = 0;

    for size in compute_chunk_sizes(buf.len()) {
        let chunk = &mut buf[offset..offset + size];

        simd::hadamard::wht_dispatch(chunk);

        let norm = 1.0 / (size as f64).sqrt();
        for v in chunk.iter_mut() {
            *v *= norm;
        }

        offset += size;
    }
    debug_assert_eq!(offset, buf.len());
}

#[cfg(test)]
mod test {
    use rand::prelude::StdRng;
    use rand::{Rng, RngExt, SeedableRng};

    use super::*;

    #[test]
    fn test_compute_chunk_sizes() {
        // All chunks must be powers of two.
        for dim in [5, 128, 129, 300, 700, 712, 1536, 4096] {
            let sizes = compute_chunk_sizes(dim).collect::<Vec<_>>();
            assert!(
                sizes.iter().all(|s| s.is_power_of_two()),
                "dim={dim}: not all power-of-2: {sizes:?}"
            );
            assert_eq!(
                sizes.iter().sum::<usize>(),
                dim,
                "dim={dim}: chunks don't sum to dim: {sizes:?}"
            );
            // Chunks should be in decreasing order.
            assert!(
                sizes.windows(2).all(|w| w[0] >= w[1]),
                "dim={dim}: not decreasing: {sizes:?}"
            );
        }

        // Specific cases.
        assert_eq!(compute_chunk_sizes(128).collect::<Vec<_>>(), vec![128]);
        assert_eq!(
            compute_chunk_sizes(700).collect::<Vec<_>>(),
            vec![512, 128, 32, 16, 8, 4],
        );
        assert_eq!(
            compute_chunk_sizes(1536).collect::<Vec<_>>(),
            vec![1024, 512]
        );
        assert_eq!(compute_chunk_sizes(4096).collect::<Vec<_>>(), vec![4096]);
    }

    /// Test that Hadamard rotation spreads energy across dimensions,
    /// reducing distortion in vectors where energy is concentrated.
    #[test]
    fn hadamard_reduces_distortion() {
        use crate::vector_stats::VectorStats;

        for dim in [100, 101, 300, 384, 512, 1024, 1025, 1586] {
            let n_vectors = 200;
            let rot = HadamardRotation::new(dim);

            // Generate distorted vectors: energy concentrated in first few dims.
            let mut rng = StdRng::seed_from_u64(42);
            let vectors: Vec<Vec<f64>> = (0..n_vectors)
                .map(|_| {
                    let random_vector: Vec<f64> = (0..dim)
                        .map(|d| {
                            let scale = if d < 5 { 100.0 } else { 0.01 };
                            rng.random_range(-1.0f64..1.0) * scale
                        })
                        .collect();
                    cosine_preprocess(random_vector)
                })
                .collect();

            let mut rotated = vectors.clone();
            for vector in rotated.iter_mut() {
                rot.apply(vector);
            }

            let rotated: Vec<_> = rotated
                .into_iter()
                .map(|v| v.into_iter().map(|i| i as f32).collect::<Vec<_>>())
                .collect();

            let vectors: Vec<_> = vectors
                .into_iter()
                .map(|v| v.into_iter().map(|i| i as f32).collect::<Vec<_>>())
                .collect();

            let stats_before = VectorStats::build(vectors.iter(), dim);
            let stats_after = VectorStats::build(rotated.iter(), rot.dim);

            // Measure how uniform the per-dimension stddevs are by looking at
            // the ratio between max and min stddev across dimensions.
            let stddevs_before: Vec<f32> = stats_before
                .elements_stats
                .iter()
                .map(|s| s.stddev)
                .collect();

            let stddevs_after: Vec<f32> = stats_after
                .elements_stats
                .iter()
                .map(|s| s.stddev)
                .collect();

            let ratio = |s: &[f32]| {
                let max = s.iter().copied().fold(f32::MIN, f32::max);
                let min = s.iter().copied().fold(f32::MAX, f32::min);
                max / min
            };

            let ratio_before = ratio(&stddevs_before);
            let ratio_after = ratio(&stddevs_after);

            // Allow ratio to have a ratio of max 0.2% of the original ratio.
            let allowed_ratio_ratio = 0.002;

            // Before rotation: huge spread (some dims have 100x scale).
            assert!(
                ratio_before > 1000.0,
                "expected distorted input, got ratio {ratio_before}"
            );

            // After rotation: stddevs should be much more uniform.
            assert!(
                ratio_after <= ratio_before * allowed_ratio_ratio,
                "rotation didn't spread energy enough, stddev ratio {ratio_after} ({})",
                ratio_after / ratio_before
            );
        }
    }

    /// Verify the free-function API (`random_vector_rotation` /
    /// `random_vector_rotation_inverse`) matches the struct API bit-for-bit
    /// and is its own inverse. Without this, a wrong seed index, a forgotten
    /// `.rev()`, or a swapped helper in either free function would slip past
    /// `hadamard_roundtrip` (which only exercises the struct).
    #[test]
    fn static_rotation_matches_struct_and_roundtrips() {
        for &dim in &[128, 300, 1024, 1536] {
            let mut rng = StdRng::seed_from_u64(7);
            let input: Vec<f64> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();

            let rot = HadamardRotation::new(dim);

            // Forward: free function must be bit-identical to struct.
            let mut via_static = input.clone();
            let mut via_struct = input.clone();
            random_vector_rotation(&mut via_static);
            rot.apply(&mut via_struct);
            assert_eq!(via_static, via_struct, "dim={dim}: forward mismatch");

            // Inverse: free function must be bit-identical to struct.
            let mut inv_static = via_static.clone();
            let mut inv_struct = via_struct.clone();
            random_vector_rotation_inverse(&mut inv_static);
            rot.apply_inverse(&mut inv_struct);
            assert_eq!(inv_static, inv_struct, "dim={dim}: inverse mismatch");

            // Forward-then-inverse via free functions recovers the input.
            for (orig, recovered) in input.iter().zip(&inv_static) {
                assert!(
                    (orig - recovered).abs() < 1e-5,
                    "dim={dim}: static roundtrip failed: {orig} vs {recovered}",
                );
            }
        }
    }

    #[test]
    fn hadamard_roundtrip() {
        let power_of_two_dims = [128, 512, 1024, 4096];
        let rand_dims = [50, 127, 300, 500, 1025];
        let dim_iter = power_of_two_dims.iter().chain(&rand_dims);

        for &dim in dim_iter {
            for seed in [0, 10, 42, 100] {
                let rot = HadamardRotation::new(dim);
                let mut rng = StdRng::seed_from_u64(seed);

                let input: Vec<f64> = (0..dim)
                    .map(|_| f64::from(rng.next_u32() % 1_000) / 100.0)
                    .collect();

                let mut rotated = input.clone();
                rot.apply(&mut rotated);
                rot.apply_inverse(&mut rotated);

                // Original input and recovered should be the same.
                for (orig, recovered) in input.iter().zip(rotated.iter()) {
                    assert!(
                        (orig - recovered).abs() < 1e-5,
                        "hadamard roundtrip failed: {orig} vs {recovered}",
                    );
                }
            }
        }
    }

    fn is_length_zero_or_normalized(length: f64) -> bool {
        length < f64::EPSILON || (length - 1.0).abs() <= 1.0e-6
    }

    fn cosine_preprocess(vector: Vec<f64>) -> Vec<f64> {
        let mut length: f64 = vector.iter().map(|x| x * x).sum();
        if is_length_zero_or_normalized(length) {
            return vector;
        }
        length = length.sqrt();
        vector.iter().map(|x| x / length).collect()
    }
}
