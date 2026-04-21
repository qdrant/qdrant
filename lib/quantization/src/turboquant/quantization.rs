use crate::DistanceType;
use crate::turboquant::rotation::HadamardRotation;
use crate::turboquant::{Metadata, TQBits, TQMode};

/// Quantize vectors using TurboQuant.
pub struct TurboQuantizer {
    pub(super) rotation: HadamardRotation,
    pub(super) bits: TQBits,
    pub(super) mode: TQMode,
    pub(super) distance: DistanceType,
}

/// A query with the Hadamard rotation already applied. Built via
/// [`TurboQuantizer::precompute_query`] and consumed by
/// [`TurboQuantizer::dot_precomputed`] to amortize the rotation cost across
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
        TurboQuantizer {
            rotation,
            bits,
            mode,
            distance,
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
        if !matches!(self.distance, DistanceType::Dot) {
            // TODO(turbo): implement for other metrics too.
            unimplemented!("Quantization currently only implemented for dot product");
        }

        debug_assert_eq!(vec.len(), buf.len());

        // Rotate the vector
        for (i, &component) in vec.iter().enumerate() {
            buf[i] = f64::from(component);
        }
        self.rotation.apply(buf);

        // Calculate data that needs to be stored additionally.
        let extras = self.calculate_extras(buf);

        // Find rotated vectors centroids.
        let boundaries = self.bits.get_centroid_boundaries();
        let encoded = buf
            .iter()
            .map(|&val| boundaries.partition_point(|&b| val > f64::from(b)) as u8);

        // Encode centroid indices and return packed vector.
        self.pack_vector(encoded, extras)
    }

    /// Dot product between two vectors that were both encoded with this
    /// quantizer.
    ///
    /// Unpacks each vector into rotated space and dots the results. The
    /// Hadamard rotation is orthogonal, so `<Rv1, Rv2> = <v1, v2>` — no
    /// inverse rotation is needed, and neither side plays the query role.
    pub fn dot_symmetric(&self, v1: &[u8], v2: &[u8]) -> f32 {
        // TODO(turbo): apply metadata from extra values.
        let (_extra_v1, iter1) = self.unpack_vector(v1);
        let (_extra_v2, iter2) = self.unpack_vector(v2);
        iter1.zip(iter2).map(|(a, b)| a * b).sum::<f64>() as f32
    }

    /// Dot product between a raw `query` and a quantizer-encoded `vec`.
    ///
    /// The query is rotated into the same Hadamard-rotated space as the stored
    /// vector before the dot is computed. Prefer [`Self::dot_precomputed`]
    /// when scoring many vectors against the same query.
    pub fn dot_asymmetric(&self, query: &[f32], vec: &[u8]) -> f32 {
        let mut rotated_query: Vec<_> = query.iter().map(|&i| f64::from(i)).collect();
        self.rotation.apply(&mut rotated_query);

        // TODO(turbo): apply metadata from extra value.
        let (_extra_vec, unpacked) = self.unpack_vector(vec);
        rotated_query
            .iter()
            .zip(unpacked)
            .map(|(&q, u)| q * u)
            .sum::<f64>() as f32
    }

    /// Precompute the Hadamard rotation of `query` so subsequent
    /// [`Self::dot_precomputed`] calls skip the per-call rotation.
    pub fn precompute_query(&self, query: &[f32]) -> Precomputed {
        let mut rotated: Vec<f64> = query.iter().map(|&x| f64::from(x)).collect();
        self.rotation.apply(&mut rotated);
        Precomputed(rotated)
    }

    /// Dot product with a query that has already been rotated via
    /// [`Self::precompute_query`].
    pub fn dot_precomputed(&self, query: &Precomputed, vec: &[u8]) -> f32 {
        let (_, unpacked) = self.unpack_vector(vec);
        query
            .as_slice()
            .iter()
            .zip(unpacked)
            .map(|(&q, u)| q * u)
            .sum::<f64>() as f32
    }
}

#[cfg(test)]
mod tests {
    use common::bitpacking::BitReader;

    use super::*;
    use crate::VectorParameters;
    use crate::turboquant::encoding::TqVectorExtras;
    use crate::turboquant::rotation::HadamardRotation;

    fn make_tq(dim: usize, bits: TQBits) -> TurboQuantizer {
        let metadata = Metadata {
            vector_parameters: VectorParameters {
                dim,
                distance_type: DistanceType::Dot,
                invert: false,
                deprecated_count: None,
            },
            bits,
            mode: TQMode::Normal,
        };
        TurboQuantizer::new_from_metadata(&metadata)
    }

    /// Build a pair `(a, b)` where `b = t * a + (1 - t) * noise`, so `t = 1`
    /// means identical and `t = 0` means independent random.
    fn generate_mixed_pair(
        dim: usize,
        t: f32,
        rng: &mut rand::prelude::StdRng,
    ) -> (Vec<f32>, Vec<f32>) {
        use rand::RngExt;
        let a: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
        let noise: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
        let b: Vec<f32> = a
            .iter()
            .zip(noise.iter())
            .map(|(&x, &n)| t * x + (1.0 - t) * n)
            .collect();
        (a, b)
    }

    fn true_dot(a: &[f32], b: &[f32]) -> f64 {
        a.iter()
            .zip(b)
            .map(|(&x, &y)| f64::from(x) * f64::from(y))
            .sum()
    }

    fn l2_norm(v: &[f32]) -> f64 {
        v.iter()
            .map(|&x| f64::from(x) * f64::from(x))
            .sum::<f64>()
            .sqrt()
    }

    /// Helper: unpack all centroid indices from a quantized byte vector.
    fn unpack_indices(packed: &[u8], dim: usize, bits: TQBits) -> Vec<u8> {
        let mut reader = BitReader::new(packed);
        reader.set_bits(bits.bit_size());
        (0..dim).map(|_| reader.read()).collect()
    }

    /// Extreme-magnitude inputs (including values near f32::MAX) must still
    /// produce in-range centroid indices rather than panicking or wrapping.
    #[test]
    fn quantize_extreme_values() {
        let dim = 128;
        let mut buf = vec![0.0f64; dim];

        for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
            let tq = make_tq(dim, bits);
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
                let tq = make_tq(dim, bits);
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
                let tq = make_tq(dim, bits);
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
                let tq = make_tq(dim, bits);
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

    /// After rotation, each dimension is quantized independently. Verify that
    /// the assigned centroid index is the nearest centroid for each rotated value.
    #[test]
    fn quantize_assigns_nearest_centroid() {
        use rand::prelude::StdRng;
        use rand::{RngExt, SeedableRng};

        let mut rng = StdRng::seed_from_u64(77);

        for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
            let centroids = bits.get_centroids();

            for &dim in &[128, 300, 512] {
                let mut buf = vec![0.0f64; dim];
                let tq = make_tq(dim, bits);
                let rot = HadamardRotation::new(dim);
                let vec: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();

                let result = tq.quantize(&vec, &mut buf);
                let indices = unpack_indices(&result, dim, bits);

                // Reproduce the rotation to get the values that were quantized.
                let mut rotated: Vec<f64> = vec.iter().map(|&v| f64::from(v)).collect();
                rot.apply(&mut rotated);

                for (d, (&idx, &val)) in indices.iter().zip(rotated.iter()).enumerate() {
                    let assigned_centroid = f64::from(centroids[idx as usize]);
                    let assigned_dist = (val - assigned_centroid).abs();

                    // No other centroid should be strictly closer.
                    for (j, &c) in centroids.iter().enumerate() {
                        let dist = (val - f64::from(c)).abs();
                        assert!(
                            assigned_dist <= dist + 1e-9,
                            "dim={dim}, bits={bits:?}, d={d}: index {idx} \
                             (centroid={assigned_centroid:.4}) is farther ({assigned_dist:.6}) \
                             than centroid[{j}]={c:.4} (dist={dist:.6}) for val={val:.6}"
                        );
                    }
                }
            }
        }
    }

    /// Reconstruct quantized vectors by mapping indices back to centroids and
    /// applying inverse rotation. Verify the reconstruction is a reasonable
    /// approximation: MSE should decrease with more bits.
    #[test]
    fn quantize_reconstruction_quality_improves_with_bits() {
        use rand::prelude::StdRng;
        use rand::{RngExt, SeedableRng};

        let dim = 512;
        let n_vectors = 50;
        let mut buf = vec![0.0f64; dim];
        let mut rng = StdRng::seed_from_u64(99);
        let rot = HadamardRotation::new(dim);

        let vectors: Vec<Vec<f32>> = (0..n_vectors)
            .map(|_| (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect())
            .collect();

        let mut mse_per_bits = Vec::new();

        for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
            let tq = make_tq(dim, bits);
            let centroids = bits.get_centroids();
            let mut total_mse = 0.0f64;

            for vec in &vectors {
                let packed = tq.quantize(vec, &mut buf);
                let indices = unpack_indices(&packed, dim, bits);

                // Reconstruct: map indices -> centroids, then inverse rotate.
                let mut reconstructed: Vec<f64> = indices
                    .iter()
                    .map(|&i| f64::from(centroids[i as usize]))
                    .collect();
                rot.apply_inverse(&mut reconstructed);

                // MSE between original and reconstructed.
                let mse: f64 = vec
                    .iter()
                    .zip(reconstructed.iter())
                    .map(|(&orig, &recon)| {
                        let diff = f64::from(orig) - recon;
                        diff * diff
                    })
                    .sum::<f64>()
                    / dim as f64;

                total_mse += mse;
            }

            let avg_mse = total_mse / f64::from(n_vectors);
            mse_per_bits.push((bits, avg_mse));
        }

        // More bits should give lower MSE.
        let mse_1bit = mse_per_bits[0].1;
        let mse_2bit = mse_per_bits[1].1;
        let mse_4bit = mse_per_bits[2].1;

        assert!(
            mse_2bit < mse_1bit,
            "2-bit MSE ({mse_2bit:.6}) should be less than 1-bit ({mse_1bit:.6})"
        );
        assert!(
            mse_4bit < mse_2bit,
            "4-bit MSE ({mse_4bit:.6}) should be less than 2-bit ({mse_2bit:.6})"
        );
    }

    /// Verify that dot product ordering is approximately preserved after
    /// quantization: if dot(q, a) > dot(q, b), the quantized approximation
    /// should agree in the majority of cases.
    #[test]
    fn quantize_preserves_dot_product_ordering() {
        use rand::prelude::StdRng;
        use rand::{RngExt, SeedableRng};

        let dim = 512;
        let n_comparisons = 500;
        let mut rng = StdRng::seed_from_u64(55);
        let rot = HadamardRotation::new(dim);

        let tq = make_tq(dim, TQBits::Bits4);
        let centroids = TQBits::Bits4.get_centroids();

        let mut concordant = 0usize;

        for _ in 0..n_comparisons {
            let query: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
            let vec_a: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
            let vec_b: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();

            // True dot products.
            let dot_a: f64 = query
                .iter()
                .zip(vec_a.iter())
                .map(|(&q, &a)| f64::from(q) * f64::from(a))
                .sum();
            let dot_b: f64 = query
                .iter()
                .zip(vec_b.iter())
                .map(|(&q, &b)| f64::from(q) * f64::from(b))
                .sum();

            // Approximate dot products using quantized vectors.
            let approx_dot = |vec: &[f32]| -> f64 {
                let mut buf = vec![0.0f64; vec.len()];
                let packed = tq.quantize(vec, &mut buf);
                let indices = unpack_indices(&packed, dim, TQBits::Bits4);
                let mut recon: Vec<f64> = indices
                    .iter()
                    .map(|&i| f64::from(centroids[i as usize]))
                    .collect();
                rot.apply_inverse(&mut recon);
                query
                    .iter()
                    .zip(recon.iter())
                    .map(|(&q, &r)| f64::from(q) * r)
                    .sum()
            };

            let approx_a = approx_dot(&vec_a);
            let approx_b = approx_dot(&vec_b);

            // Check if ordering is preserved.
            if (dot_a > dot_b) == (approx_a > approx_b) {
                concordant += 1;
            }
        }

        let concordance_rate = concordant as f64 / f64::from(n_comparisons);
        // With 4 bits and 512 dimensions, ordering should be preserved
        // in a strong majority of random comparisons.
        assert!(
            concordance_rate > 0.75,
            "concordance rate {concordance_rate:.2} is too low \
             ({concordant}/{n_comparisons})"
        );
    }

    /// Negating all elements of a vector should mirror the centroid indices.
    /// For symmetric centroids c_0 < c_1 < ... < c_{n-1}, negation maps
    /// index k -> (n-1-k) because the centroids are symmetric around 0.
    #[test]
    fn quantize_negation_mirrors_indices() {
        let dim = 128; // power of 2, so rotation is clean

        for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
            let max_index = (1u8 << bits.bit_size()) - 1;
            let tq = make_tq(dim, bits);

            // Use a uniform vector so rotation doesn't scramble things.
            let val = 0.5f32;
            let pos_vec = vec![val; dim];
            let neg_vec = vec![-val; dim];
            let mut buf = vec![0.0f64; dim];

            let pos_indices = unpack_indices(&tq.quantize(&pos_vec, &mut buf), dim, bits);
            let neg_indices = unpack_indices(&tq.quantize(&neg_vec, &mut buf), dim, bits);

            for (d, (&p, &n)) in pos_indices.iter().zip(neg_indices.iter()).enumerate() {
                assert_eq!(
                    p,
                    max_index - n,
                    "bits={bits:?}, d={d}: negation should mirror index \
                     (got pos={p}, neg={n}, max_index={max_index})"
                );
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
                let tq = make_tq(dim, bits);
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

    /// `unpack_vector` must recover the exact centroid values for every index
    /// written by `pack_vector`, across a variety of dims and bit widths.
    #[test]
    fn pack_unpack_vector_roundtrip() {
        use rand::prelude::StdRng;
        use rand::{RngExt, SeedableRng};

        let mut rng = StdRng::seed_from_u64(321);

        for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
            let centroids = bits.get_centroids();
            let n_centroids = 1u8 << bits.bit_size();

            for &dim in &[1, 64, 127, 128, 300, 768, 1025] {
                let tq = make_tq(dim, bits);
                let indices: Vec<u8> = (0..dim).map(|_| rng.random_range(0..n_centroids)).collect();

                let packed = tq.pack_vector(indices.iter().copied(), TqVectorExtras::default());

                let out: Vec<f64> = tq.unpack_vector(&packed).1.collect();

                for (i, (&idx, &value)) in indices.iter().zip(out.iter()).enumerate() {
                    let expected = f64::from(centroids[idx as usize]);
                    assert_eq!(
                        value, expected,
                        "dim={dim}, bits={bits:?}, i={i}: index {idx} \
                         decoded to {value}, expected {expected}"
                    );
                }
            }
        }
    }

    /// Edge cases for the pack/unpack roundtrip: uniform all-min and all-max
    /// index patterns exercise the bit-packing boundaries.
    #[test]
    fn pack_unpack_vector_uniform_indices() {
        for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
            let centroids = bits.get_centroids();
            let max_idx = (1u8 << bits.bit_size()) - 1;

            for &dim in &[1, 8, 128, 513] {
                let tq = make_tq(dim, bits);

                for &idx in &[0u8, max_idx] {
                    let indices = vec![idx; dim];
                    let packed = tq.pack_vector(indices.iter().copied(), TqVectorExtras::default());

                    let out: Vec<f64> = tq.unpack_vector(&packed).1.collect();

                    let expected = f64::from(centroids[idx as usize]);
                    for (i, &v) in out.iter().enumerate() {
                        assert_eq!(v, expected, "dim={dim}, bits={bits:?}, idx={idx}, i={i}");
                    }
                }
            }
        }
    }

    /// `dot(0, vec) = 0` for any quantized vector — a rotated zero query is
    /// still zero, so every component of the sum is zero.
    #[test]
    fn dot_zero_query_returns_zero() {
        let dim = 128;
        let mut buf = vec![0.0f64; dim];
        for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
            let tq = make_tq(dim, bits);
            let v: Vec<f32> = (0..dim).map(|i| (i as f32 * 0.1).sin()).collect();
            let packed = tq.quantize(&v, &mut buf);
            let zero = vec![0.0f32; dim];
            let result = tq.dot_asymmetric(&zero, &packed);
            assert_eq!(result, 0.0, "bits={bits:?}: dot(0, v) = {result}");
        }
    }

    /// `dot` is linear in the query:
    /// `dot(k*q1 + c*q2, v) = k*dot(q1, v) + c*dot(q2, v)`.
    /// Rotation and per-component summation are both linear, so this holds
    /// up to floating-point precision.
    #[test]
    fn dot_linear_in_query() {
        use rand::prelude::StdRng;
        use rand::{RngExt, SeedableRng};

        let mut rng = StdRng::seed_from_u64(11);
        let dim = 256;
        let mut buf = vec![0.0f64; dim];

        for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
            let tq = make_tq(dim, bits);
            let q1: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
            let q2: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
            let v: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
            let packed = tq.quantize(&v, &mut buf);

            let (k, c) = (0.7f32, -1.3f32);
            let q_combo: Vec<f32> = q1.iter().zip(&q2).map(|(a, b)| k * a + c * b).collect();

            let d_combo = tq.dot_asymmetric(&q_combo, &packed);
            let d_linear =
                k * tq.dot_asymmetric(&q1, &packed) + c * tq.dot_asymmetric(&q2, &packed);

            let tol = 1e-5 * d_combo.abs().max(d_linear.abs()).max(1.0);
            assert!(
                (d_combo - d_linear).abs() <= tol,
                "bits={bits:?}: dot(k*q1+c*q2, v)={d_combo} vs \
                 k*dot(q1,v)+c*dot(q2,v)={d_linear}"
            );
        }
    }

    /// `dot` must agree with the equivalent manual reconstruction: unpack the
    /// vector into centroids in rotated space, apply the inverse rotation to
    /// recover original-space values, then dot with the original query.
    ///
    /// The Hadamard rotation is orthogonal, so `<Rq, u> = <q, R⁻¹u>` — both
    /// paths should agree up to floating-point precision regardless of bits/dim.
    #[test]
    fn dot_matches_inverse_rotation_reconstruction() {
        use rand::prelude::StdRng;
        use rand::{RngExt, SeedableRng};

        let mut rng = StdRng::seed_from_u64(33);

        for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
            for &dim in &[128, 300, 512] {
                let tq = make_tq(dim, bits);
                let rot = HadamardRotation::new(dim);

                let q: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
                let v: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
                let mut buf = vec![0.0f64; dim];
                let packed = tq.quantize(&v, &mut buf);

                // Manual path: unpack -> inverse rotate -> dot in original space.
                let mut unpacked: Vec<f64> = tq.unpack_vector(&packed).1.collect();
                rot.apply_inverse(&mut unpacked);
                let manual: f32 = q
                    .iter()
                    .zip(unpacked.iter())
                    .map(|(&qi, &ui)| f64::from(qi) * ui)
                    .sum::<f64>() as f32;

                let fn_result = tq.dot_asymmetric(&q, &packed);

                let tol = 1e-5 * manual.abs().max(fn_result.abs()).max(1.0);
                assert!(
                    (manual - fn_result).abs() <= tol,
                    "dim={dim}, bits={bits:?}: dot={fn_result}, manual={manual}"
                );
            }
        }
    }

    /// For 4-bit quantization on reasonably large dimensions, the quantized
    /// dot product must closely approximate the true (pre-quantization) dot
    /// product. A badly-approximating `dot` (e.g. missing the query rotation)
    /// yields values uncorrelated with the truth and blows this bound out.
    #[test]
    fn dot_approximates_true_dot_product() {
        use rand::prelude::StdRng;
        use rand::{RngExt, SeedableRng};

        let dim = 512;
        let n_trials = 100;
        let mut rng = StdRng::seed_from_u64(44);
        let tq = make_tq(dim, TQBits::Bits4);
        let mut buf = vec![0.0f64; dim];

        let mut total_abs_err = 0.0f64;
        let mut total_abs_true = 0.0f64;

        for _ in 0..n_trials {
            let q: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
            let v: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();

            let true_dot: f64 = q
                .iter()
                .zip(&v)
                .map(|(&a, &b)| f64::from(a) * f64::from(b))
                .sum();

            let packed = tq.quantize(&v, &mut buf);
            let approx = f64::from(tq.dot_asymmetric(&q, &packed));

            total_abs_err += (true_dot - approx).abs();
            total_abs_true += true_dot.abs();
        }

        let rel_mae = total_abs_err / total_abs_true;
        assert!(
            rel_mae < 0.15,
            "Relative MAE {rel_mae} exceeds tolerance for Bits4, dim={dim}"
        );
    }

    /// `dot_symmetric` must (a) be commutative and (b) approximate the true
    /// dot product — even with quantization noise on *both* sides, the
    /// orthogonality of the Hadamard rotation keeps the result close to
    /// `<v1, v2>`.
    #[test]
    fn dot_symmetric_approximates_true_dot() {
        use rand::prelude::StdRng;
        use rand::{RngExt, SeedableRng};

        let dim = 512;
        let n_trials = 100;
        let mut rng = StdRng::seed_from_u64(66);
        let tq = make_tq(dim, TQBits::Bits4);
        let mut buf = vec![0.0f64; dim];

        let mut total_abs_err = 0.0f64;
        let mut total_abs_true = 0.0f64;

        for _ in 0..n_trials {
            let a: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
            let b: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();

            let true_dot: f64 = a
                .iter()
                .zip(&b)
                .map(|(&x, &y)| f64::from(x) * f64::from(y))
                .sum();

            let packed_a = tq.quantize(&a, &mut buf);
            let packed_b = tq.quantize(&b, &mut buf);

            let ab = tq.dot_symmetric(&packed_a, &packed_b);
            let ba = tq.dot_symmetric(&packed_b, &packed_a);
            assert_eq!(ab, ba, "dot_symmetric is not commutative: {ab} vs {ba}");

            total_abs_err += (f64::from(ab) - true_dot).abs();
            total_abs_true += true_dot.abs();
        }

        let rel_mae = total_abs_err / total_abs_true;
        // Both sides quantized → roughly double the error vs the asymmetric case,
        // but still well below 40% relative MAE for Bits4 at dim=512.
        assert!(
            rel_mae < 0.4,
            "Relative MAE {rel_mae} exceeds tolerance for Bits4, dim={dim}"
        );
    }

    /// `dot_symmetric(packed, packed)` approximates `||v||²` — a sharper
    /// check than the random-pair test, since self-dot is always positive
    /// and sensitive to sign errors in the unpack path.
    #[test]
    fn dot_symmetric_self_dot_approximates_norm_squared() {
        use rand::prelude::StdRng;
        use rand::{RngExt, SeedableRng};

        let dim = 512;
        let mut rng = StdRng::seed_from_u64(77);
        let tq = make_tq(dim, TQBits::Bits4);
        let mut buf = vec![0.0f64; dim];

        for _ in 0..20 {
            let v: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
            let norm_sq: f64 = v.iter().map(|&x| f64::from(x) * f64::from(x)).sum();

            let packed = tq.quantize(&v, &mut buf);
            let approx = f64::from(tq.dot_symmetric(&packed, &packed));

            let rel_err = (norm_sq - approx).abs() / norm_sq;
            assert!(
                rel_err < 0.15,
                "||v||²={norm_sq}, approx={approx}, rel_err={rel_err}"
            );
        }
    }

    // --- End-to-end tests for dot_symmetric -------------------------------
    //
    // Each test: build a TurboQuantizer, generate random pairs at various
    // similarity levels, compute both the original dot and the quantized
    // dot_symmetric, compare.
    // ---------------------------------------------------------------------

    /// Run pairs at 5 mixing levels from fully independent (`t = 0.0`) to
    /// identical (`t = 1.0`). Per-trial absolute error must be small relative
    /// to `||a|| * ||b||` — that scale-invariant bound holds uniformly even
    /// when the raw dot product itself is near zero.
    #[test]
    fn end_to_end_dot_symmetric_varied_similarity() {
        use rand::SeedableRng;
        use rand::prelude::StdRng;

        let dim = 512;
        let n_trials = 30;
        let mut rng = StdRng::seed_from_u64(2024);
        let tq = make_tq(dim, TQBits::Bits4);
        let mut buf = vec![0.0f64; dim];

        for &t in &[0.0f32, 0.25, 0.5, 0.75, 1.0] {
            let mut max_norm_err = 0.0f64;

            for _ in 0..n_trials {
                let (a, b) = generate_mixed_pair(dim, t, &mut rng);
                let true_d = true_dot(&a, &b);
                let scale = l2_norm(&a) * l2_norm(&b);

                let packed_a = tq.quantize(&a, &mut buf);
                let packed_b = tq.quantize(&b, &mut buf);
                let approx = f64::from(tq.dot_symmetric(&packed_a, &packed_b));

                max_norm_err = max_norm_err.max((true_d - approx).abs() / scale);
            }

            assert!(
                max_norm_err < 0.1,
                "t={t}: max |true-approx|/(||a||*||b||) = {max_norm_err}"
            );
        }
    }

    /// Over a shared set of varied-similarity pairs, accuracy must improve
    /// monotonically with bit width: MAE(4b) < MAE(2b) < MAE(1b).
    #[test]
    fn end_to_end_dot_symmetric_more_bits_is_better() {
        use rand::SeedableRng;
        use rand::prelude::StdRng;

        let dim = 512;
        let n_pairs = 80;
        let mut rng = StdRng::seed_from_u64(2025);
        let mut buf = vec![0.0f64; dim];

        // Fixed pair set (varying similarity) shared across all bit widths.
        let pairs: Vec<(Vec<f32>, Vec<f32>)> = (0..n_pairs)
            .map(|i| {
                let t = i as f32 / (n_pairs as f32 - 1.0);
                generate_mixed_pair(dim, t, &mut rng)
            })
            .collect();

        let mut mae_by_bits = Vec::new();
        for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
            let tq = make_tq(dim, bits);
            let mut total_abs_err = 0.0f64;
            for (a, b) in &pairs {
                let packed_a = tq.quantize(a, &mut buf);
                let packed_b = tq.quantize(b, &mut buf);
                let approx = f64::from(tq.dot_symmetric(&packed_a, &packed_b));
                total_abs_err += (true_dot(a, b) - approx).abs();
            }
            mae_by_bits.push((bits, total_abs_err / f64::from(n_pairs)));
        }

        assert!(
            mae_by_bits[1].1 < mae_by_bits[0].1,
            "2-bit MAE {} not better than 1-bit MAE {}",
            mae_by_bits[1].1,
            mae_by_bits[0].1
        );
        assert!(
            mae_by_bits[2].1 < mae_by_bits[1].1,
            "4-bit MAE {} not better than 2-bit MAE {}",
            mae_by_bits[2].1,
            mae_by_bits[1].1
        );
    }

    /// Rank ordering of dot products over a diverse set of pairs must be
    /// almost fully preserved after symmetric quantization — concordance
    /// over all pairwise comparisons should exceed 95% at 4 bits.
    #[test]
    fn end_to_end_dot_symmetric_preserves_ordering() {
        use rand::SeedableRng;
        use rand::prelude::StdRng;

        let dim = 512;
        let n_pairs = 60;
        let mut rng = StdRng::seed_from_u64(2026);
        let tq = make_tq(dim, TQBits::Bits4);
        let mut buf = vec![0.0f64; dim];

        let mut pairs_scored: Vec<(f64, f64)> = Vec::with_capacity(n_pairs);
        for i in 0..n_pairs {
            let t = i as f32 / (n_pairs as f32 - 1.0);
            let (a, b) = generate_mixed_pair(dim, t, &mut rng);
            let packed_a = tq.quantize(&a, &mut buf);
            let packed_b = tq.quantize(&b, &mut buf);
            let approx = f64::from(tq.dot_symmetric(&packed_a, &packed_b));
            pairs_scored.push((true_dot(&a, &b), approx));
        }

        let mut concordant = 0usize;
        let mut total = 0usize;
        for i in 0..n_pairs {
            for j in (i + 1)..n_pairs {
                let true_cmp = pairs_scored[i].0 - pairs_scored[j].0;
                let approx_cmp = pairs_scored[i].1 - pairs_scored[j].1;
                if true_cmp.signum() == approx_cmp.signum() {
                    concordant += 1;
                }
                total += 1;
            }
        }

        let concordance = concordant as f64 / total as f64;
        assert!(
            concordance > 0.95,
            "ordering concordance {concordance} below 0.95 ({concordant}/{total})"
        );
    }
}
