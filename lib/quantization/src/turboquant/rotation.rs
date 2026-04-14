#![allow(dead_code)]

use rand::prelude::StdRng;
use rand::{RngExt, SeedableRng};

use crate::turboquant::permutation::Permutation;

const N_PERMUTATIONS: usize = 3;

/// Hadamard rotation implementation customized for TurboQuant.
pub struct HadamardRotation {
    /// Random signs applied before the rotation
    signs1: Vec<f64>,

    /// Random signs applied after the rotation
    signs2: Vec<f64>,

    /// Random (but shared) permutations.
    permutations: [Permutation; N_PERMUTATIONS],

    /// Original dimension.
    dim: usize,

    /// Sequence of power-of-2 chunk sizes that exactly cover `dim`.
    /// Produced by greedily taking the largest power-of-2 that fits the remainder.
    chunk_sizes: Vec<usize>,
}

impl HadamardRotation {
    pub fn new(seed: u64, dim: usize) -> Self {
        let chunk_sizes = compute_chunk_sizes(dim);

        let mut rng = StdRng::seed_from_u64(seed);

        let signs1 = generate_signs(&mut rng, dim);
        let signs2 = generate_signs(&mut rng, dim);

        let permutations: [_; N_PERMUTATIONS] =
            std::array::from_fn(|_| Permutation::new(&mut rng, dim));

        Self {
            signs1,
            signs2,
            permutations,
            dim,
            chunk_sizes,
        }
    }

    pub fn apply(&self, x: &[f32]) -> Vec<f32> {
        debug_assert_eq!(x.len(), self.dim);

        let mut buf: Vec<f64> = x
            .iter()
            .zip(&self.signs1)
            .map(|(&v, &s)| f64::from(v) * s)
            .collect();

        // Apply WHT + normalize to each variable-size chunk.
        self.wht_normalized_chunks(&mut buf);

        // Temp vector for `apply_permutation`.
        let mut tmp = vec![0.0f64; buf.len()];

        // Permute then WHT+normalize for each permutation.
        for permutation in &self.permutations {
            permutation.apply(&mut buf, &mut tmp, true);
            self.wht_normalized_chunks(&mut buf);
        }

        buf.iter()
            .zip(&self.signs2)
            .map(|(&v, &s)| (v * s) as f32)
            .collect()
    }

    pub fn apply_inverse(&self, y: &[f32]) -> Vec<f32> {
        debug_assert_eq!(y.len(), self.dim);

        let mut buf: Vec<f64> = y
            .iter()
            .zip(&self.signs2)
            .map(|(&v, &s)| f64::from(v) * s)
            .collect();

        // WHT + normalize
        self.wht_normalized_chunks(&mut buf);

        // Temp vector for `apply_permutation`.
        let mut tmp = vec![0.0f64; buf.len()];

        // Apply inverse permutations backwards.
        for permutation in self.permutations.iter().rev() {
            permutation.apply(&mut buf, &mut tmp, false);
            self.wht_normalized_chunks(&mut buf);
        }

        buf[..self.dim]
            .iter()
            .zip(&self.signs1)
            .map(|(&v, &sign)| (v * sign) as f32)
            .collect()
    }

    /// Apply WHT + normalization to variable-size chunks.
    fn wht_normalized_chunks(&self, buf: &mut [f64]) {
        let mut offset = 0;
        for &size in &self.chunk_sizes {
            let chunk = &mut buf[offset..offset + size];
            in_place_walsh_hadamard_transform(chunk);
            let norm = 1.0 / (size as f64).sqrt();
            for v in chunk.iter_mut() {
                *v *= norm;
            }
            offset += size;
        }
        debug_assert_eq!(offset, buf.len());
    }
}

/// In-place unnormalized Walsh-Hadamard Transform in f64.
/// Normalization is applied externally in `apply`/`apply_inverse`.
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
pub fn compute_chunk_sizes(dim: usize) -> Vec<usize> {
    debug_assert!(dim > 0);
    let mut sizes = Vec::with_capacity(1);
    let mut remaining = dim;
    while remaining > 0 {
        let next_pow2 = remaining.next_power_of_two();
        let chunk = if next_pow2 == remaining {
            remaining
        } else {
            next_pow2 >> 1
        };
        sizes.push(chunk);
        remaining -= chunk;
    }
    sizes
}

/// Generate a random sign vector (±1.0).
fn generate_signs(rng: &mut StdRng, n: usize) -> Vec<f64> {
    (0..n)
        .map(|_| {
            if rng.random::<bool>() {
                1.0f64
            } else {
                -1.0f64
            }
        })
        .collect()
}

#[cfg(test)]
mod test {
    use rand::Rng;

    use super::*;

    #[test]
    fn test_compute_chunk_sizes() {
        // All chunks must be powers of two.
        for dim in [5, 128, 129, 300, 700, 712, 1536, 4096] {
            let sizes = compute_chunk_sizes(dim);
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
        assert_eq!(compute_chunk_sizes(128), vec![128]);
        assert_eq!(compute_chunk_sizes(700), vec![512, 128, 32, 16, 8, 4]);
        assert_eq!(compute_chunk_sizes(1536), vec![1024, 512]);
        assert_eq!(compute_chunk_sizes(4096), vec![4096]);
    }

    /// Test that Hadamard rotation spreads energy across dimensions,
    /// reducing distortion in vectors where energy is concentrated.
    #[test]
    fn hadamard_reduces_distortion() {
        use crate::VectorParameters;
        use crate::vector_stats::VectorStats;

        for dim in [100, 300, 384, 512, 1024, 1586] {
            let n_vectors = 200;
            let rot = HadamardRotation::new(42, dim);

            // Generate distorted vectors: energy concentrated in first few dims.
            let mut rng = StdRng::seed_from_u64(42);
            let vectors: Vec<Vec<f32>> = (0..n_vectors)
                .map(|_| {
                    (0..dim)
                        .map(|d| {
                            let scale = if d < 5 { 100.0 } else { 0.01 };
                            rng.random_range(-1.0f32..1.0) * scale
                        })
                        .collect()
                })
                .collect();

            let rotated: Vec<Vec<f32>> = vectors.iter().map(|v| rot.apply(v)).collect();

            let mut params = VectorParameters {
                dim,
                distance_type: crate::DistanceType::Dot,
                invert: false,
                deprecated_count: None,
            };

            let stats_before = VectorStats::build(vectors.iter(), &params);

            params.dim = rot.dim;
            let stats_after = VectorStats::build(rotated.iter(), &params);

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

    #[test]
    fn hadamard_roundtrip() {
        let power_of_two_dims = [128, 512, 1024, 4096];
        let rand_dims = [50, 127, 300, 500, 1025];

        let dim_iter = power_of_two_dims.iter().chain(&rand_dims);

        for &dim in dim_iter {
            for seed in [0, 10, 42, 100] {
                let rot = HadamardRotation::new(seed, dim);
                let mut rng = StdRng::seed_from_u64(seed);

                let input: Vec<f32> = (0..dim)
                    .map(|_| ((rng.next_u32() % 1_000) as f32) / 100.0)
                    .collect();

                let recovered = rot.apply_inverse(&rot.apply(&input));

                // Original input and recovered should be the same.
                for (orig, recovered) in input.iter().zip(recovered.iter()) {
                    assert!(
                        (orig - recovered).abs() < 1e-5,
                        "hadamard roundtrip failed: {orig} vs {recovered}",
                    );
                }
            }
        }
    }
}
