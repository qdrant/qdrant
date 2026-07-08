use crate::turboquant::permutation::Permutation;
use crate::turboquant::simd;

const N_PERMUTATIONS: usize = 3;

/// Random seeds for Permutation. The values were picked arbitrarily.
///
/// WARNING: DO NOT CHANGE THESE VALUES. They are baked into the encoding of every quantized vector.
/// Changing them would silently corrupt all existing quantized vectors.
const PERMUTATION_SEEDS: [u64; 3] = [654605292835415893, 8636605637963351413, 1775280196666917949];

/// Hadamard rotation implementation customized for TurboQuant.
///
/// Materializes the shared random permutations as index maps at construction
/// time, so `apply`/`apply_inverse` replace the serial Fisher-Yates swap
/// replay (one LCG step plus a 64-bit `%` per element) with a flat gather
/// pass. Output stays bit-identical to the replay-based test-only oracle
/// (`random_vector_rotation` / `random_vector_rotation_inverse`): the
/// maps move the exact same values to the exact same positions.
pub struct HadamardRotation {
    /// `forward_maps[p][k]` is the source index whose value lands at `k`
    /// when applying permutation `p` (same shuffle as [`Permutation::permute`]).
    forward_maps: [Vec<u32>; N_PERMUTATIONS],

    /// Inverse of `forward_maps`, matching [`Permutation::unpermute`].
    backward_maps: [Vec<u32>; N_PERMUTATIONS],

    /// Original dimension.
    dim: usize,
}

impl HadamardRotation {
    pub fn new(dim: usize) -> Self {
        // The index maps store u32; larger dims would silently wrap.
        assert!(
            u32::try_from(dim).is_ok(),
            "HadamardRotation dim {dim} must fit in u32",
        );

        let forward_maps: [_; N_PERMUTATIONS] = std::array::from_fn(|index| {
            // `new_one_way` is enough: the backward map is derived by
            // inverting the forward one, no reverse LCG replay needed.
            let permutation = Permutation::new_one_way(PERMUTATION_SEEDS[index], dim);
            let mut map: Vec<u32> = (0..dim as u32).collect();
            permutation.permute(&mut map);
            map
        });

        let backward_maps = std::array::from_fn(|index| {
            let mut inverse = vec![0u32; dim];
            for (position, &source) in forward_maps[index].iter().enumerate() {
                inverse[source as usize] = position as u32;
            }
            inverse
        });

        Self {
            forward_maps,
            backward_maps,
            dim,
        }
    }

    /// Number of coordinates this rotation spans.
    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn apply(&self, x: &mut [f64]) {
        debug_assert_eq!(x.len(), self.dim);
        wht_and_gather_rounds(x, self.forward_maps.iter());
    }

    pub fn apply_inverse(&self, y: &mut [f64]) {
        debug_assert_eq!(y.len(), self.dim);
        wht_and_gather_rounds(y, self.backward_maps.iter().rev());
    }
}

thread_local! {
    /// Reusable buffer for the gather ping-pong in [`wht_and_gather_rounds`],
    /// avoiding a heap allocation per `apply`/`apply_inverse` call on the
    /// encode/decode hot paths.
    static GATHER_SCRATCH: std::cell::RefCell<Vec<f64>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// WHT+normalize `x`, then for each map apply the permutation and
/// WHT+normalize again.
///
/// Each permutation is one out-of-place gather that ping-pongs the live data
/// between `x` and a scratch buffer instead of an in-place pass, avoiding a
/// full copy per permutation; after an odd number of rounds a single copy
/// moves the result from the scratch buffer back into `x`.
fn wht_and_gather_rounds<'a>(x: &mut [f64], maps: impl Iterator<Item = &'a Vec<u32>>) {
    GATHER_SCRATCH.with(|cell| {
        let mut scratch = cell.borrow_mut();
        // Grow-only: every gather fully overwrites its destination, so the
        // zero-fill only matters for capacity. Shrinking here would force a
        // re-zeroing regrow on every call when a thread alternates between
        // dims.
        if scratch.len() < x.len() {
            scratch.resize(x.len(), 0.0);
        }

        wht_normalized_chunks(x);

        let mut src: &mut [f64] = x;
        let mut dst: &mut [f64] = &mut scratch[..src.len()];
        let mut rounds = 0;
        for map in maps {
            // SAFETY: `map` comes from `HadamardRotation::new`, which builds
            // it as a permutation of `0..dim`.
            unsafe { gather_permuted(dst, src, map) };
            wht_normalized_chunks(dst);
            std::mem::swap(&mut src, &mut dst);
            rounds += 1;
        }

        // After an odd number of rounds (`src` and `dst` swapped an odd
        // number of times) the live data sits in the scratch buffer, now
        // `src`, while `dst` is `x` again.
        if rounds % 2 == 1 {
            dst.copy_from_slice(src);
        }
    });
}

/// Out-of-place gather `dst[k] = src[map[k]]`: the materialized form of one
/// permutation pass.
///
/// # Safety
/// `map` must only contain indices in `0..map.len()` (it is a permutation in
/// practice); the length asserts inside then guarantee every index is in
/// bounds for `src`.
unsafe fn gather_permuted(dst: &mut [f64], src: &[f64], map: &[u32]) {
    // Hard asserts: the unchecked gather below would otherwise turn a
    // length-mismatched call into an out-of-bounds read.
    assert_eq!(dst.len(), src.len());
    assert_eq!(dst.len(), map.len());
    for (d, &s) in dst.iter_mut().zip(map) {
        debug_assert!((s as usize) < src.len());
        // SAFETY: `map` only holds indices in `0..map.len()` (caller
        // contract), and `src.len() == map.len()` is asserted above, so every
        // index is in bounds.
        *d = unsafe { *src.get_unchecked(s as usize) };
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
///
/// Test-only: replays the Fisher-Yates shuffle instead of materializing index
/// maps, serving as the independent parity oracle that pins
/// [`HadamardRotation`]'s output to the historical replay behavior.
#[cfg(test)]
pub fn random_vector_rotation(x: &mut [f64]) {
    let dim = x.len();

    // Building the permutations on every call is cheap: `new_one_way` skips the LCG warm-up.
    let permutations: [_; N_PERMUTATIONS] =
        std::array::from_fn(|index| Permutation::new_one_way(PERMUTATION_SEEDS[index], dim));

    apply_rotation_with_permutations(x, &permutations);
}

/// Inverts the rotated `y` back. Produces bit-identical output to
/// [`HadamardRotation::apply_inverse`].
///
/// Test-only parity oracle; see [`random_vector_rotation`].
#[cfg(test)]
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
#[cfg(test)]
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
#[cfg(test)]
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

/// Apply WHT + normalization to variable-size chunks. The normalization
/// multiply is fused into the transform's final stage where a SIMD path
/// exists (bit-equal to a separate multiply pass).
fn wht_normalized_chunks(buf: &mut [f64]) {
    let mut offset = 0;

    for size in compute_chunk_sizes(buf.len()) {
        let chunk = &mut buf[offset..offset + size];

        let norm = 1.0 / (size as f64).sqrt();
        simd::hadamard::wht_dispatch_scaled(chunk, norm);

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
    /// `hadamard_roundtrip` (which only exercises the struct). Small dims
    /// (5, 50) pin the map path where the chunk split degenerates into
    /// several tiny power-of-two chunks.
    #[test]
    fn static_rotation_matches_struct_and_roundtrips() {
        for &dim in &[5, 50, 128, 300, 1024, 1536] {
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
