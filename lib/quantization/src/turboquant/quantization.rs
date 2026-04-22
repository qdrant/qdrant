use crate::DistanceType;
use crate::turboquant::rotation::HadamardRotation;
use crate::turboquant::{Metadata, TQBits, TQMode};

/// Quantize vectors using TurboQuant.
pub struct TurboQuantizer {
    pub(super) rotation: HadamardRotation,
    pub(super) bits: TQBits,
    pub(super) mode: TQMode,
    pub(super) distance: DistanceType,

    // Pre-calculated `sqrt(dim)` used in scoring.
    dim_sqrt: f32,
}

/// A query with the Hadamard rotation already applied. Built via
/// [`TurboQuantizer::precompute_query`] and consumed by
/// [`TurboQuantizer::score_precomputed`] to amortize the rotation cost across
/// many scoring calls.
pub struct Precomputed(Vec<f64>);

impl Precomputed {
    /// Borrow the rotated query components.
    #[inline]
    pub fn as_slice(&self) -> &[f64] {
        &self.0
    }
}

impl TurboQuantizer {
    /// Initialize a new TurboQuantizer.
    pub fn new(dim: usize, bits: TQBits, mode: TQMode, distance: DistanceType) -> Self {
        let rotation = HadamardRotation::new(dim);
        let dim_sqrt = (dim as f32).sqrt();
        TurboQuantizer {
            rotation,
            bits,
            mode,
            distance,
            dim_sqrt,
        }
    }

    /// Initialize a new TurboQuantizer from metadata.
    pub fn new_from_metadata(metadata: &Metadata) -> Self {
        Self::new(
            metadata.vector_parameters.dim,
            metadata.bits,
            metadata.mode,
            metadata.vector_parameters.distance_type,
        )
    }

    /// Quantize a given vector with TurboQuant.
    pub fn quantize(&self, vec: &[f32], buf: &mut [f64]) -> Vec<u8> {
        Self::assert_supported_distance(self.distance);

        debug_assert_eq!(vec.len(), buf.len());

        // Convert to f64
        for (i, &component) in vec.iter().enumerate() {
            buf[i] = f64::from(component);
        }

        // Rotate the vector.
        self.rotation.apply(buf);

        // Calculate data that needs to be stored additionally.
        let extras = self.calculate_extras(buf);

        // Rescale so per-coordinate variance is ~1 — matching the Lloyd-Max
        // N(0, 1) centroid grid.
        let length = f64::from(extras.l2_length.unwrap_or(1.0));
        let scale = (self.rotation.dim() as f64).sqrt() / length;

        // Encode and return packed vector.
        self.pack_vector(buf.iter().map(|&val| val * scale), extras)
    }

    /// Similarity score between two vectors that were both encoded with this
    /// quantizer. Returns an approximate `<v1, v2>` for Dot and `cos(θ)` for
    /// Cosine.
    ///
    /// For asymmetric scoring (original vector against quantized vector), refer to [`Self::score_precomputed`]
    /// and precompute the query first.
    pub fn score_symmetric(&self, v1: &[u8], v2: &[u8]) -> f32 {
        let (extra_v1, iter1) = self.unpack_vector(v1);
        let (extra_v2, iter2) = self.unpack_vector(v2);
        let raw_dot = dot_impl(iter1, iter2);

        let v1_l2 = extra_v1.l2_length.unwrap_or(1.0);
        let v2_l2 = extra_v2.l2_length.unwrap_or(1.0);

        // Both sides were scaled by sqrt(dim)/||v|| during quantize; restore
        // magnitudes with l2, undo the sqrt(dim)² = dim inflation.
        raw_dot * v1_l2 * v2_l2 / self.rotation.dim() as f32
    }

    /// Precompute the Hadamard rotation of `query` so subsequent
    /// [`Self::score_precomputed`] calls skip the per-call rotation.
    pub fn precompute_query(&self, query: &[f32]) -> Precomputed {
        let mut rotated: Vec<f64> = query.iter().map(|&x| f64::from(x)).collect();
        self.rotation.apply(&mut rotated);
        Precomputed(rotated)
    }

    /// Similarity score with a query that has already been rotated via
    /// [`Self::precompute_query`]. Returns an approximate `<query, v>` for Dot
    /// and `cos(θ)` for Cosine.
    pub fn score_precomputed(&self, query: &Precomputed, vec: &[u8]) -> f32 {
        let (vector_extras, unpacked) = self.unpack_vector(vec);
        let dot = dot_impl(query.as_slice().iter().copied(), unpacked);

        let l2 = vector_extras.l2_length.unwrap_or(1.0);

        // Only the stored vector carries the sqrt(dim)/||v|| scaling, so we
        // compensate by multiplying by ||v|| and dividing by sqrt(dim).
        dot * l2 / self.dim_sqrt
    }
}

/// Raw dot implementation between two vectors, `left` and `right`.
#[inline]
fn dot_impl<I, J>(left: I, right: J) -> f32
where
    I: Iterator<Item = f64>,
    J: Iterator<Item = f64>,
{
    left.zip(right).map(|(q, u)| q * u).sum::<f64>() as f32
}

#[cfg(test)]
mod tests {
    use common::bitpacking::BitReader;
    use rand::prelude::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;
    use crate::VectorParameters;
    use crate::turboquant::encoding::TqVectorExtras;

    fn make_tq(dim: usize, bits: TQBits, distance: DistanceType) -> TurboQuantizer {
        let metadata = Metadata {
            vector_parameters: VectorParameters {
                dim,
                distance_type: distance,
                invert: false,
                deprecated_count: None,
            },
            bits,
            mode: TQMode::Normal,
        };
        TurboQuantizer::new_from_metadata(&metadata)
    }

    /// Build a vector pair that has a given magnitude of similarity, tuned by `similarity`.
    /// `similarity = 1` means identical and `similarity = 0` means independent random vectors.
    fn generate_random_vector_pair_with_similarity(
        dim: usize,
        similarity: f32,
        rng: &mut rand::prelude::StdRng,
    ) -> (Vec<f32>, Vec<f32>) {
        use rand::RngExt;
        let a: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
        let noise: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
        let b: Vec<f32> = a
            .iter()
            .zip(noise.iter())
            .map(|(&x, &n)| similarity * x + (1.0 - similarity) * n)
            .collect();
        (a, b)
    }

    /// Build a single random vector with components uniformly in `[-1, 1]`.
    fn random_vector(dim: usize, rng: &mut StdRng) -> Vec<f32> {
        (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect()
    }

    fn l2_norm(v: &[f32]) -> f64 {
        v.iter()
            .map(|&x| f64::from(x) * f64::from(x))
            .sum::<f64>()
            .sqrt()
    }

    /// Score an original vector against a quantized one.
    fn asymmetric_score_helper(tq: &TurboQuantizer, query: &[f32], vec: &[u8]) -> f32 {
        let precomputed = tq.precompute_query(query);
        tq.score_precomputed(&precomputed, vec)
    }

    /// Normalize `v` onto the unit sphere — required input for the Cosine
    /// quantizer path.
    fn normalize_vector(v: &[f32]) -> Vec<f32> {
        let len = l2_norm(v) as f32;
        v.iter().map(|&x| x / len).collect()
    }

    /// Helper: unpack all centroid indices from a quantized byte vector.
    fn unpack_indices(packed: &[u8], dim: usize, bits: TQBits) -> Vec<u8> {
        let mut reader = BitReader::new(packed);
        reader.set_bits(bits.bit_size());
        (0..dim).map(|_| reader.read()).collect()
    }

    #[inline]
    fn dot_f32_impl<I, J>(left: I, right: J) -> f32
    where
        I: Iterator<Item = f32>,
        J: Iterator<Item = f32>,
    {
        left.zip(right).map(|(q, u)| q * u).sum::<f32>()
    }

    /// Extreme-magnitude inputs (including values near f32::MAX) must still
    /// produce in-range centroid indices rather than panicking or wrapping.
    #[test]
    fn quantize_extreme_values() {
        let dim = 128;
        let mut buf = vec![0.0f64; dim];

        for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
            let tq = make_tq(dim, bits, DistanceType::Cosine);
            let n_centroids = 1u8 << bits.bit_size();

            for &val in &[1000.0f32, -1000.0, f32::MAX / 2.0, f32::MIN / 2.0] {
                let vec = vec![val; dim];
                let result = tq.quantize(&vec, &mut buf);

                let indices = unpack_indices(&result, dim, bits);
                for &idx in &indices {
                    assert!(idx < n_centroids, "index {idx} out of range for {bits:?}");
                }
            }
        }
    }

    /// Output byte length must be ceil(dim * bit_size / 8).
    #[test]
    fn quantize_output_byte_length() {
        let dims = [64, 128, 300, 384, 512, 768, 1024, 1536];
        let bit_widths = [TQBits::Bits1, TQBits::Bits2, TQBits::Bits4];

        for &bits in &bit_widths {
            for &dim in &dims {
                let mut buf = vec![0.0f64; dim];
                let tq = make_tq(dim, bits, DistanceType::Cosine);
                let vec = vec![0.1; dim];
                let result = tq.quantize(&vec, &mut buf);
                let expected_bytes = tq.quantized_size();
                assert_eq!(
                    result.len(),
                    expected_bytes,
                    "dim={dim}, bits={bits:?}: expected {expected_bytes} bytes, got {}",
                    result.len()
                );
            }
        }
    }

    /// Quantizing the same vector twice must produce identical output.
    #[test]
    fn quantize_deterministic() {
        use rand::prelude::StdRng;
        use rand::{RngExt, SeedableRng};

        let mut rng = StdRng::seed_from_u64(123);

        for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
            for &dim in &[128, 300, 768] {
                let mut buf = vec![0.0f64; dim];
                let tq = make_tq(dim, bits, DistanceType::Cosine);
                let vec: Vec<f32> = (0..dim).map(|_| rng.random_range(-2.0..2.0)).collect();

                let r1 = tq.quantize(&vec, &mut buf);
                let r2 = tq.quantize(&vec, &mut buf);
                assert_eq!(r1, r2, "dim={dim}, bits={bits:?}: non-deterministic output");
            }
        }
    }

    /// A zero vector, after rotation, stays zero. All indices should map to the
    /// middle boundary region (centroids are symmetric around 0).
    #[test]
    fn quantize_zero_vector() {
        for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
            let n_centroids = 1u8 << bits.bit_size();
            // For symmetric centroids around 0, zero maps to either of the two
            // middle indices. With boundaries being midpoints of consecutive
            // centroids, 0.0 lands at boundary n_centroids/2 - 1 or n_centroids/2.
            let middle_low = n_centroids / 2 - 1;
            let middle_high = n_centroids / 2;

            for &dim in &[128, 256, 512] {
                let mut buf = vec![0.0f64; dim];
                let tq = make_tq(dim, bits, DistanceType::Cosine);
                let vec = vec![0.0; dim];
                let result = tq.quantize(&vec, &mut buf);
                let indices = unpack_indices(&result, dim, bits);

                for (d, &idx) in indices.iter().enumerate() {
                    assert!(
                        idx == middle_low || idx == middle_high,
                        "dim={dim}, bits={bits:?}, d={d}: zero-vector index {idx} \
                         not in [{middle_low}, {middle_high}]"
                    );
                }
            }
        }
    }

    /// Non-power-of-2 dimensions should work correctly (the rotation decomposes
    /// into power-of-2 chunks internally).
    #[test]
    fn quantize_non_power_of_two_dims() {
        use rand::prelude::StdRng;
        use rand::{RngExt, SeedableRng};

        let mut rng = StdRng::seed_from_u64(42);
        // Focus on small and odd dims where the Hadamard decomposition
        // into power-of-2 chunks is most likely to trip. The larger
        // non-pow-2 sizes are already covered by quantize_output_byte_length.
        let odd_dims = [3, 50, 127, 700, 1025];

        for &dim in &odd_dims {
            for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
                let tq = make_tq(dim, bits, DistanceType::Cosine);
                let n_centroids = 1u8 << bits.bit_size();
                let vec: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();

                let mut buf = vec![0.0f64; dim];
                let result = tq.quantize(&vec, &mut buf);

                // Correct length.
                let expected_bytes = tq.quantized_size();
                assert_eq!(result.len(), expected_bytes, "dim={dim}, bits={bits:?}");

                // All indices in range.
                let indices = unpack_indices(&result, dim, bits);
                for &idx in &indices {
                    assert!(
                        idx < n_centroids,
                        "dim={dim}, bits={bits:?}: index {idx} out of range"
                    );
                }
            }
        }
    }

    /// Feeding the centroid values yielded by `unpack_vector` back into
    /// `pack_vector` must reproduce the same bytes — and decode to the same
    /// values — across a variety of dims and bit widths.
    #[test]
    fn pack_unpack_vector_roundtrip() {
        use rand::prelude::StdRng;
        use rand::{RngExt, SeedableRng};

        let mut rng = StdRng::seed_from_u64(321);

        for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
            let centroids = bits.get_centroids();
            let n_centroids = 1u8 << bits.bit_size();

            for &dim in &[1, 64, 127, 128, 300, 768, 1025] {
                let tq = make_tq(dim, bits, DistanceType::Cosine);
                let indices: Vec<u8> = (0..dim).map(|_| rng.random_range(0..n_centroids)).collect();
                let values: Vec<f64> = indices
                    .iter()
                    .map(|&idx| f64::from(centroids[idx as usize]))
                    .collect();

                let packed = tq.pack_vector(values.iter().copied(), TqVectorExtras::default());

                let out: Vec<f64> = tq.unpack_vector(&packed).1.collect();

                for (i, (&expected, &value)) in values.iter().zip(out.iter()).enumerate() {
                    assert_eq!(
                        value, expected,
                        "dim={dim}, bits={bits:?}, i={i}: \
                         decoded to {value}, expected {expected}"
                    );
                }
            }
        }
    }

    /// Quantized scores (symmetric and asymmetric paths) stay within tolerance
    /// of the true dot/cosine similarity across a range of pair similarities.
    #[test]
    fn score_approximates_true_similarity() {
        for dim in [128, 300, 512, 1000, 1024, 2000, 4000] {
            let bits = TQBits::Bits4;
            let mut rng = StdRng::seed_from_u64(42);

            for &distance in &[DistanceType::Dot, DistanceType::Cosine] {
                let tq = make_tq(dim, bits, distance);
                let mut buf = vec![0.0f64; dim];

                for &similarity in &[0.2f32, 0.5, 0.8] {
                    let (a_raw, b_raw) =
                        generate_random_vector_pair_with_similarity(dim, similarity, &mut rng);

                    // Cosine path requires unit-norm inputs.
                    let (a, b) = match distance {
                        DistanceType::Cosine => {
                            (normalize_vector(&a_raw), normalize_vector(&b_raw))
                        }
                        _ => (a_raw, b_raw),
                    };

                    let true_score = dot_f32_impl(a.iter().copied(), b.iter().copied());

                    let a_q = tq.quantize(&a, &mut buf);
                    let b_q = tq.quantize(&b, &mut buf);

                    let sym = tq.score_symmetric(&a_q, &b_q);
                    let asym = asymmetric_score_helper(&tq, &a, &b_q);

                    // Cosine scores are bounded in [-1, 1]; Dot scales with ||a||*||b||.
                    let scale = match distance {
                        DistanceType::Cosine => 1.0,
                        _ => (l2_norm(&a) * l2_norm(&b)) as f32,
                    };
                    let tol = 0.05 * scale;

                    assert!(
                        (sym - true_score).abs() < tol,
                        "symmetric: distance={distance:?}, similarity={similarity}, \
                     got {sym}, expected {true_score} (tol {tol})"
                    );
                    assert!(
                        (asym - true_score).abs() < tol,
                        "asymmetric: distance={distance:?}, similarity={similarity}, \
                     got {asym}, expected {true_score} (tol {tol})"
                    );
                }
            }
        }
    }

    /// score(v, v) must recover ‖v‖² for Dot and 1.0 for Cosine, across both
    /// symmetric and asymmetric scoring paths.
    #[test]
    fn score_self_similarity() {
        let bits = TQBits::Bits4;

        for dim in [128, 300, 512, 1024, 2000] {
            let mut rng = StdRng::seed_from_u64(42);

            for &distance in &[DistanceType::Dot, DistanceType::Cosine] {
                let tq = make_tq(dim, bits, distance);
                let mut buf = vec![0.0f64; dim];

                let raw = random_vector(dim, &mut rng);
                let v = match distance {
                    DistanceType::Cosine => normalize_vector(&raw),
                    _ => raw,
                };

                let expected = match distance {
                    DistanceType::Cosine => 1.0,
                    _ => (l2_norm(&v) * l2_norm(&v)) as f32,
                };
                let tol = 0.05 * expected.abs().max(1.0);

                let v_q = tq.quantize(&v, &mut buf);

                let sym = tq.score_symmetric(&v_q, &v_q);
                let asym = asymmetric_score_helper(&tq, &v, &v_q);

                assert!(
                    (sym - expected).abs() < tol,
                    "symmetric: dim={dim}, distance={distance:?}, \
                     got {sym}, expected {expected} (tol {tol})"
                );
                assert!(
                    (asym - expected).abs() < tol,
                    "asymmetric: dim={dim}, distance={distance:?}, \
                     got {asym}, expected {expected} (tol {tol})"
                );
            }
        }
    }

    /// score(v, -v) must recover -‖v‖² for Dot and -1.0 for Cosine across both
    /// scoring paths — the sign correctness check on the negative end.
    #[test]
    fn score_antipodal_is_negative() {
        let bits = TQBits::Bits4;

        for dim in [128, 300, 512, 1024, 2000] {
            let mut rng = StdRng::seed_from_u64(42);

            for &distance in &[DistanceType::Dot, DistanceType::Cosine] {
                let tq = make_tq(dim, bits, distance);
                let mut buf = vec![0.0f64; dim];

                let raw = random_vector(dim, &mut rng);
                let v = match distance {
                    DistanceType::Cosine => normalize_vector(&raw),
                    _ => raw,
                };
                let neg_v: Vec<f32> = v.iter().map(|&x| -x).collect();

                let expected = match distance {
                    DistanceType::Cosine => -1.0,
                    _ => -(l2_norm(&v) * l2_norm(&v)) as f32,
                };
                let tol = 0.05 * expected.abs().max(1.0);

                let v_q = tq.quantize(&v, &mut buf);
                let neg_q = tq.quantize(&neg_v, &mut buf);

                let sym = tq.score_symmetric(&v_q, &neg_q);
                let asym = asymmetric_score_helper(&tq, &v, &neg_q);

                assert!(
                    (sym - expected).abs() < tol,
                    "symmetric: dim={dim}, distance={distance:?}, \
                     got {sym}, expected {expected} (tol {tol})"
                );
                assert!(
                    (asym - expected).abs() < tol,
                    "asymmetric: dim={dim}, distance={distance:?}, \
                     got {asym}, expected {expected} (tol {tol})"
                );
            }
        }
    }

    /// Higher bit widths must produce lower mean-absolute scoring error:
    /// MAE(Bits4) ≤ MAE(Bits2) ≤ MAE(Bits1) across a batch of random pairs.
    #[test]
    fn higher_bits_reduce_error() {
        let dim = 512;
        let n_pairs = 32;

        for &distance in &[DistanceType::Dot, DistanceType::Cosine] {
            let mae = |bits: TQBits| -> f32 {
                let mut rng = StdRng::seed_from_u64(42);
                let tq = make_tq(dim, bits, distance);
                let mut buf = vec![0.0f64; dim];

                let total: f32 = (0..n_pairs)
                    .map(|_| {
                        let (a_raw, b_raw) =
                            generate_random_vector_pair_with_similarity(dim, 0.5, &mut rng);
                        let (a, b) = match distance {
                            DistanceType::Cosine => {
                                (normalize_vector(&a_raw), normalize_vector(&b_raw))
                            }
                            _ => (a_raw, b_raw),
                        };
                        let truth = dot_f32_impl(a.iter().copied(), b.iter().copied());
                        let a_q = tq.quantize(&a, &mut buf);
                        let b_q = tq.quantize(&b, &mut buf);
                        (tq.score_symmetric(&a_q, &b_q) - truth).abs()
                    })
                    .sum();
                total / n_pairs as f32
            };

            let mae_1 = mae(TQBits::Bits1);
            let mae_2 = mae(TQBits::Bits2);
            let mae_4 = mae(TQBits::Bits4);

            assert!(
                mae_4 <= mae_2 && mae_2 <= mae_1,
                "distance={distance:?}: MAE not monotonic in bits — \
                 Bits1={mae_1}, Bits2={mae_2}, Bits4={mae_4}"
            );
        }
    }

    /// Scoring extreme-magnitude (but f32-in-range) vectors must produce finite
    /// results — not NaN or ±Inf — on both symmetric and asymmetric paths.
    #[test]
    fn score_extreme_magnitudes_finite() {
        let dim = 128;
        let bits = TQBits::Bits4;
        let mut buf = vec![0.0f64; dim];

        for &distance in &[DistanceType::Dot, DistanceType::Cosine] {
            let tq = make_tq(dim, bits, distance);

            for &val in &[1000.0f32, -1000.0, 1e6, -1e6] {
                let raw = vec![val; dim];
                let v = match distance {
                    DistanceType::Cosine => normalize_vector(&raw),
                    _ => raw,
                };

                let v_q = tq.quantize(&v, &mut buf);
                let sym = tq.score_symmetric(&v_q, &v_q);
                let asym = asymmetric_score_helper(&tq, &v, &v_q);

                assert!(
                    sym.is_finite(),
                    "symmetric: distance={distance:?}, val={val}: got {sym}"
                );
                assert!(
                    asym.is_finite(),
                    "asymmetric: distance={distance:?}, val={val}: got {asym}"
                );
            }
        }
    }

    /// Quantized scoring preserves candidate ordering: ranking by quantized
    /// scores matches ranking by true scores on all but a small fraction of
    /// pairwise comparisons.
    #[test]
    fn rank_preservation() {
        let dim = 512;
        let bits = TQBits::Bits4;
        let mut buf = vec![0.0f64; dim];

        for &distance in &[DistanceType::Dot, DistanceType::Cosine] {
            let mut rng = StdRng::seed_from_u64(42);
            let tq = make_tq(dim, bits, distance);

            let query_raw = random_vector(dim, &mut rng);
            let similarities: Vec<f32> = (1..=10).map(|i| i as f32 / 10.0).collect();
            let candidates_raw: Vec<Vec<f32>> = similarities
                .iter()
                .map(|&s| {
                    let noise = random_vector(dim, &mut rng);
                    query_raw
                        .iter()
                        .zip(&noise)
                        .map(|(&q, &n)| s * q + (1.0 - s) * n)
                        .collect()
                })
                .collect();

            let query = match distance {
                DistanceType::Cosine => normalize_vector(&query_raw),
                _ => query_raw,
            };
            let candidates: Vec<Vec<f32>> = candidates_raw
                .iter()
                .map(|c| match distance {
                    DistanceType::Cosine => normalize_vector(c),
                    _ => c.clone(),
                })
                .collect();

            let true_scores: Vec<f32> = candidates
                .iter()
                .map(|c| dot_f32_impl(query.iter().copied(), c.iter().copied()))
                .collect();
            let quant_scores: Vec<f32> = candidates
                .iter()
                .map(|c| {
                    let cq = tq.quantize(c, &mut buf);
                    asymmetric_score_helper(&tq, &query, &cq)
                })
                .collect();

            let n = candidates.len();
            let mut inversions = 0;
            for i in 0..n {
                for j in (i + 1)..n {
                    let true_sign = (true_scores[i] - true_scores[j]).signum();
                    let quant_sign = (quant_scores[i] - quant_scores[j]).signum();
                    if true_sign != 0.0 && true_sign != quant_sign {
                        inversions += 1;
                    }
                }
            }
            let total_pairs = n * (n - 1) / 2;

            assert!(
                inversions * 100 < 15 * total_pairs,
                "distance={distance:?}: {inversions}/{total_pairs} pairs inverted"
            );
        }
    }

    /// Dot scoring scales linearly with vector magnitude:
    /// score(q, k·v) ≈ k·⟨q,v⟩  and  score(k·q, k·v) ≈ k²·⟨q,v⟩.
    /// Guards that the stored l2_length extras correctly restore scale.
    /// (Cosine's contract requires unit-norm inputs, so scaling is out-of-scope.)
    #[test]
    fn score_linearity_dot() {
        let dim = 512;
        let bits = TQBits::Bits4;
        let mut rng = StdRng::seed_from_u64(42);
        let tq = make_tq(dim, bits, DistanceType::Dot);
        let mut buf = vec![0.0f64; dim];

        let (q, v) = generate_random_vector_pair_with_similarity(dim, 0.5, &mut rng);
        let true_dot = dot_f32_impl(q.iter().copied(), v.iter().copied());

        for &k in &[0.5f32, 2.0, 5.0] {
            let q_scaled: Vec<f32> = q.iter().map(|&x| x * k).collect();
            let v_scaled: Vec<f32> = v.iter().map(|&x| x * k).collect();

            let q_scaled_q = tq.quantize(&q_scaled, &mut buf);
            let v_scaled_q = tq.quantize(&v_scaled, &mut buf);

            let expected_asym = k * true_dot;
            let expected_sym = k * k * true_dot;

            let asym = asymmetric_score_helper(&tq, &q, &v_scaled_q);
            let sym = tq.score_symmetric(&q_scaled_q, &v_scaled_q);

            let tol_asym = 0.05 * (l2_norm(&q) * l2_norm(&v_scaled)) as f32;
            let tol_sym = 0.05 * (l2_norm(&q_scaled) * l2_norm(&v_scaled)) as f32;

            assert!(
                (asym - expected_asym).abs() < tol_asym,
                "asymmetric: k={k}, got {asym}, expected {expected_asym} (tol {tol_asym})"
            );
            assert!(
                (sym - expected_sym).abs() < tol_sym,
                "symmetric: k={k}, got {sym}, expected {expected_sym} (tol {tol_sym})"
            );
        }
    }

    /// Edge cases for the pack/unpack roundtrip: uniform all-min and all-max
    /// centroid values exercise the bit-packing boundaries.
    #[test]
    fn pack_unpack_vector_uniform_indices() {
        for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
            let centroids = bits.get_centroids();
            let max_idx = (1u8 << bits.bit_size()) - 1;

            for &dim in &[1, 8, 128, 513] {
                let tq = make_tq(dim, bits, DistanceType::Cosine);

                for &idx in &[0u8, max_idx] {
                    let expected = f64::from(centroids[idx as usize]);
                    let values = vec![expected; dim];
                    let packed = tq.pack_vector(values.iter().copied(), TqVectorExtras::default());

                    let out: Vec<f64> = tq.unpack_vector(&packed).1.collect();

                    for (i, &v) in out.iter().enumerate() {
                        assert_eq!(v, expected, "dim={dim}, bits={bits:?}, idx={idx}, i={i}");
                    }
                }
            }
        }
    }
}
