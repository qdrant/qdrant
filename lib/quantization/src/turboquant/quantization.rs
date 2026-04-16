use common::bitpacking::BitWriter;

use crate::DistanceType;
use crate::turboquant::rotation::HadamardRotation;
use crate::turboquant::{Metadata, TQBits, TQMode};

/// Quantize vectors using TurboQuant.
pub struct TurboQuantizer {
    rotation: HadamardRotation,
    bits: TQBits,
    _mode: TQMode,
    distance: DistanceType,
}

impl TurboQuantizer {
    /// Initialize a new TurboQuantizer.
    pub fn new(metadata: &Metadata) -> TurboQuantizer {
        let rotation = HadamardRotation::new(metadata.vector_parameters.dim);
        TurboQuantizer {
            rotation,
            bits: metadata.bits,
            _mode: metadata.mode,
            distance: metadata.vector_parameters.distance_type,
        }
    }

    /// Quantize a given vector with TurboQuant.
    pub fn quantize(&self, vec: &[f32]) -> Vec<u8> {
        if !matches!(self.distance, DistanceType::Dot) {
            unimplemented!("Quantization currently only implemented for dot product");
        }

        // Rotate the vector
        let mut rotated_vec = vec.iter().map(|i| f64::from(*i)).collect::<Vec<_>>();
        self.rotation.apply(&mut rotated_vec);

        // Find rotated vectors centroids.
        let boundaries = self.bits.get_centroid_boundaries();
        let encoded = rotated_vec
            .iter()
            .map(|&val| boundaries.partition_point(|&b| val > f64::from(b)) as u8);

        // Encode centroid indices and return packed vector.
        self.pack_vector(encoded, vec.len())
    }

    /// Bit-pack an iterator of centroid indices into a compact byte vector.
    fn pack_vector<I>(&self, centroids: I, dim: usize) -> Vec<u8>
    where
        I: Iterator<Item = u8>,
    {
        let mut out = Vec::with_capacity(self.bytes_required(dim));
        let mut bit_writer = BitWriter::new(&mut out);

        let bits = self.bits.bit_size();
        for item in centroids {
            bit_writer.write(item, bits);
        }
        bit_writer.finish();
        out
    }

    /// Returns the amount of bytes required for the given dimension.
    fn bytes_required(&self, dim: usize) -> usize {
        (dim * self.bits.bit_size() as usize).div_ceil(8)
    }
}

#[cfg(test)]
mod tests {
    use common::bitpacking::BitReader;

    use super::*;
    use crate::VectorParameters;
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
        TurboQuantizer::new(&metadata)
    }

    /// Helper: unpack all centroid indices from a quantized byte vector.
    fn unpack_indices(packed: &[u8], dim: usize, bits: TQBits) -> Vec<u8> {
        let mut reader = BitReader::new(packed);
        reader.set_bits(bits.bit_size());
        (0..dim).map(|_| reader.read()).collect()
    }

    /// Extreme values can cause the old f32-based implementation to disagree
    /// with the f64-based boundary search due to precision loss in the cast.
    /// Verify the new implementation is deterministic and produces valid output.
    #[test]
    fn quantize_extreme_values() {
        for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
            let tq = make_tq(128, bits);
            let n_centroids = 1u8 << bits.bit_size();

            for &val in &[1000.0f32, -1000.0, f32::MAX / 2.0, f32::MIN / 2.0] {
                let vec = vec![val; 128];
                let result = tq.quantize(&vec);

                // Deterministic: same input produces same output.
                assert_eq!(result, tq.quantize(&vec));

                // All packed indices must be in range [0, n_centroids).
                let indices = unpack_indices(&result, 128, bits);
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
                let tq = make_tq(dim, bits);
                let vec = vec![0.1; dim];
                let result = tq.quantize(&vec);
                let expected_bytes = tq.bytes_required(dim);
                assert_eq!(
                    result.len(),
                    expected_bytes,
                    "dim={dim}, bits={bits:?}: expected {expected_bytes} bytes, got {}",
                    result.len()
                );
            }
        }
    }

    /// All unpacked centroid indices must be in [0, 2^bits).
    #[test]
    fn quantize_indices_in_range() {
        use rand::prelude::StdRng;
        use rand::{RngExt, SeedableRng};

        for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
            let n_centroids = 1u8 << bits.bit_size();

            for &dim in &[64, 128, 300, 768] {
                let tq = make_tq(dim, bits);
                let mut rng = StdRng::seed_from_u64(42);

                for _ in 0..20 {
                    let vec: Vec<f32> = (0..dim).map(|_| rng.random_range(-3.0..3.0)).collect();
                    let result = tq.quantize(&vec);
                    let indices = unpack_indices(&result, dim, bits);

                    for (d, &idx) in indices.iter().enumerate() {
                        assert!(
                            idx < n_centroids,
                            "dim={dim}, bits={bits:?}, d={d}: index {idx} >= {n_centroids}"
                        );
                    }
                }
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
                let tq = make_tq(dim, bits);
                let vec: Vec<f32> = (0..dim).map(|_| rng.random_range(-2.0..2.0)).collect();

                let r1 = tq.quantize(&vec);
                let r2 = tq.quantize(&vec);
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
                let tq = make_tq(dim, bits);
                let vec = vec![0.0; dim];
                let result = tq.quantize(&vec);
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
                let tq = make_tq(dim, bits);
                let rot = HadamardRotation::new(dim);
                let vec: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();

                let result = tq.quantize(&vec);
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
                let packed = tq.quantize(vec);
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
                let packed = tq.quantize(vec);
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

    /// Different vectors should (usually) produce different quantized output.
    #[test]
    fn quantize_different_vectors_differ() {
        use rand::prelude::StdRng;
        use rand::{RngExt, SeedableRng};

        let mut rng = StdRng::seed_from_u64(1);
        let dim = 256;
        let tq = make_tq(dim, TQBits::Bits4);

        let vec_a: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
        let vec_b: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();

        let result_a = tq.quantize(&vec_a);
        let result_b = tq.quantize(&vec_b);

        assert_ne!(
            result_a, result_b,
            "two random vectors should not quantize identically"
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

            let pos_indices = unpack_indices(&tq.quantize(&pos_vec), dim, bits);
            let neg_indices = unpack_indices(&tq.quantize(&neg_vec), dim, bits);

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
        let odd_dims = [3, 50, 127, 300, 700, 1025, 1536];

        for &dim in &odd_dims {
            for &bits in &[TQBits::Bits1, TQBits::Bits2, TQBits::Bits4] {
                let tq = make_tq(dim, bits);
                let n_centroids = 1u8 << bits.bit_size();
                let vec: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();

                let result = tq.quantize(&vec);

                // Correct length.
                let expected_bytes = tq.bytes_required(dim);
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
}
