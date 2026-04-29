use crate::turboquant::permutation::Permutation;

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

    /// Sequence of power-of-2 chunk sizes that exactly cover `dim`.
    /// Produced by greedily taking the largest power-of-2 that fits the remainder.
    chunk_sizes: Vec<usize>,

    /// Precomputed normalization factors: `1.0 / sqrt(chunk_size)` for each chunk.
    chunk_norms: Vec<f64>,
}

impl HadamardRotation {
    pub fn new(dim: usize) -> Self {
        let chunk_sizes = compute_chunk_sizes(dim);

        let chunk_norms: Vec<f64> = chunk_sizes
            .iter()
            .map(|&s| 1.0 / (s as f64).sqrt())
            .collect();

        let permutations: [_; N_PERMUTATIONS] =
            std::array::from_fn(|index| Permutation::new(PERMUTATION_SEEDS[index], dim));

        Self {
            permutations,
            dim,
            chunk_sizes,
            chunk_norms,
        }
    }

    pub fn apply(&self, x: &mut [f64]) {
        debug_assert_eq!(x.len(), self.dim);

        // Apply WHT + normalize to each variable-size chunk.
        self.wht_normalized_chunks(x);

        // Permute then WHT+normalize for each permutation.
        for permutation in &self.permutations {
            permutation.permute(x);
            self.wht_normalized_chunks(x);
        }
    }

    pub fn apply_inverse(&self, y: &mut [f64]) {
        debug_assert_eq!(y.len(), self.dim);

        // WHT + normalize
        self.wht_normalized_chunks(y);

        // Apply inverse permutations backwards.
        for permutation in self.permutations.iter().rev() {
            permutation.unpermute(y);
            self.wht_normalized_chunks(y);
        }
    }

    /// Apply WHT + normalization to variable-size chunks.
    fn wht_normalized_chunks(&self, buf: &mut [f64]) {
        let mut offset = 0;
        for (&size, &norm) in self.chunk_sizes.iter().zip(&self.chunk_norms) {
            let chunk = &mut buf[offset..offset + size];
            in_place_walsh_hadamard_transform(chunk);
            for v in chunk.iter_mut() {
                *v *= norm;
            }
            offset += size;
        }
        debug_assert_eq!(offset, buf.len());
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
fn compute_chunk_sizes(dim: usize) -> Vec<usize> {
    debug_assert!(dim > 0);
    let mut sizes = Vec::with_capacity(dim.count_ones() as usize);
    let mut bits = dim;
    while bits != 0 {
        let highest = 1 << bits.ilog2();
        sizes.push(highest);
        bits ^= highest;
    }
    sizes
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
