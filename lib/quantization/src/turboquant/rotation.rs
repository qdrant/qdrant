#![allow(dead_code)]

use rand::prelude::StdRng;
use rand::{RngExt, SeedableRng};

const N_PERMUTATIONS: usize = 3;

/// Hadamard rotation implementation customized for TurboQuant.
pub struct HadamardRotation {
    signs1: Vec<f64>,
    signs2: Vec<f64>,

    permutations: [(Vec<usize>, Vec<usize>); N_PERMUTATIONS],

    /// Rotated dimension.
    padded_dim: usize,

    /// Size of each chunk depending on the original dimension.
    chunk_size: usize,
}

impl HadamardRotation {
    pub fn new(seed: u64, dim: usize) -> Self {
        let chunk_size = get_chunk_size(dim);
        let padded_dim = compute_padded_dim(dim, chunk_size);

        let mut rng = StdRng::seed_from_u64(seed);

        let signs1 = generate_signs(&mut rng, padded_dim);
        let signs2 = generate_signs(&mut rng, padded_dim);

        let permutations: [_; N_PERMUTATIONS] =
            std::array::from_fn(|_| generate_permutation(&mut rng, padded_dim));

        Self {
            signs1,
            signs2,
            permutations,
            padded_dim,
            chunk_size,
        }
    }

    pub fn padded_dim(&self) -> usize {
        self.padded_dim
    }

    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    pub fn apply(&self, x: &[f32]) -> Vec<f32> {
        debug_assert!(x.len() <= self.padded_dim);
        // TODO: Can we use f32?

        let mut buf = Vec::with_capacity(self.padded_dim);
        buf.extend(x.iter().map(|&v| v as f64));
        buf.resize(self.padded_dim, 0.0);

        // Apply random signs
        for (b, &s) in buf.iter_mut().zip(self.signs1.iter()) {
            *b *= s;
        }

        // Apply hadamard
        for chunk in buf.chunks_mut(self.chunk_size) {
            in_place_walsh_hadamard_transform(chunk);
        }

        // Temp vector for `apply_permutation`.
        let mut tmp = vec![0.0f64; buf.len()];

        // Apply hadamard for each permutation.
        for perm in &self.permutations {
            apply_permutation(&mut buf, &mut tmp, &perm.0);
            for chunk in buf.chunks_mut(self.chunk_size) {
                in_place_walsh_hadamard_transform(chunk);
            }
        }

        // Apply second random signs.
        for (b, &s) in buf.iter_mut().zip(self.signs2.iter()) {
            *b *= s;
        }

        buf.iter().map(|&v| v as f32).collect()
    }

    pub fn apply_inverse(&self, y: &[f32], original_dim: usize) -> Vec<f32> {
        debug_assert!(original_dim <= self.padded_dim);
        let mut buf = Vec::with_capacity(self.padded_dim);
        buf.extend(y.iter().map(|&v| v as f64));
        buf.resize(self.padded_dim, 0.0);

        // Apply signs 2
        for (b, &s) in buf.iter_mut().zip(self.signs2.iter()) {
            *b *= s;
        }

        // First hadamard
        for chunk in buf.chunks_mut(self.chunk_size) {
            in_place_walsh_hadamard_transform(chunk);
        }

        // Temp vector for `apply_permutation`.
        let mut tmp = vec![0.0f64; buf.len()];

        // Apply permutations (backwards)
        for perm in self.permutations.iter().rev() {
            apply_permutation(&mut buf, &mut tmp, &perm.1);

            for chunk in buf.chunks_mut(self.chunk_size) {
                in_place_walsh_hadamard_transform(chunk);
            }
        }

        // Apply signs 1
        for (b, &s) in buf.iter_mut().zip(self.signs1.iter()) {
            *b *= s;
        }

        buf[..original_dim].iter().map(|&v| v as f32).collect()
    }
}

/// In-place normalized Walsh-Hadamard Transform in f64. Self-inverse: WHT(WHT(x)) = x.
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
    let norm = (n as f64).sqrt();
    for val in x.iter_mut() {
        *val /= norm;
    }
}

/// Apply a permutation out-of-place: element at index `i` moves to `perm[i]`.
pub fn apply_permutation(buf: &mut [f64], tmp: &mut [f64], perm: &[usize]) {
    debug_assert_eq!(tmp.len(), buf.len());
    for (i, &p) in perm.iter().enumerate() {
        tmp[p] = buf[i];
    }
    buf.copy_from_slice(&tmp);
}

/// Round up to the nearest multiple of `chunk_size`.
pub fn compute_padded_dim(dim: usize, chunk_size: usize) -> usize {
    (dim + chunk_size - 1) / chunk_size * chunk_size
}

/// Pick a power-of-two chunk size for block-wise Hadamard rotation.
/// Divides dim by 4 and rounds up to the next power of two, yielding ~3-5 chunks.
///
///  dim  | chunk | padded | #chunks | overhead
/// ------|-------|--------|---------|----------
///   128 |    32 |    128 |       4 |     0.0%
///   300 |   128 |    384 |       3 |    28.0%
///   384 |   128 |    384 |       3 |     0.0%
///   768 |   256 |    768 |       3 |     0.0%
///  1024 |   256 |   1024 |       4 |     0.0%
///  1280 |   512 |   1536 |       3 |    20.0%
///  1536 |   512 |   1536 |       3 |     0.0%
///  3072 |  1024 |   3072 |       3 |     0.0%
///  4096 |  1024 |   4096 |       4 |     0.0%
pub fn get_chunk_size(dim: usize) -> usize {
    (dim >> 2).next_power_of_two()
}

/// Generate a random sign vector (±1.0).
pub fn generate_signs(rng: &mut StdRng, n: usize) -> Vec<f64> {
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

/// Generate a random permutation and its inverse using Fisher-Yates shuffle.
pub fn generate_permutation(rng: &mut StdRng, n: usize) -> (Vec<usize>, Vec<usize>) {
    let mut perm: Vec<usize> = (0..n).collect();
    for i in (1..n).rev() {
        let j = rng.random_range(0..=i);
        perm.swap(i, j);
    }
    let mut inv = vec![0usize; n];
    for (i, &p) in perm.iter().enumerate() {
        inv[p] = i;
    }
    (perm, inv)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_picking_chunk_size() {
        for i in [5, 128, 129, 712, 1536] {
            assert!(get_chunk_size(i).is_power_of_two());
        }

        assert_eq!(get_chunk_size(5), 1);
        assert_eq!(get_chunk_size(100), 32);
        assert_eq!(get_chunk_size(128), 32);
        assert_eq!(get_chunk_size(129), 32);
        assert_eq!(get_chunk_size(300), 128);
        assert_eq!(get_chunk_size(712), 256);
        assert_eq!(get_chunk_size(1536), 512);
        assert_eq!(get_chunk_size(4096), 1024);
    }

    /// Test that Hadamard rotation spreads energy across dimensions,
    /// reducing distortion in vectors where energy is concentrated.
    #[test]
    fn hadamard_reduces_distortion() {
        use crate::VectorParameters;
        use crate::vector_stats::VectorStats;

        for dim in [100, 300, 384, 1024, 1586] {
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

            params.dim = rot.padded_dim;
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
        for dim in [50, 127, 128, 500, 1024, 1025] {
            let rot = HadamardRotation::new(42, dim);
            let input: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.1).collect();
            let transformed = rot.apply(&input);
            let recovered = rot.apply_inverse(&transformed, dim);
            for (a, b) in input.iter().zip(recovered.iter()) {
                assert!(
                    (a - b).abs() < 1e-5,
                    "hadamard roundtrip failed: {} vs {}",
                    a,
                    b
                );
            }
        }
    }
}
