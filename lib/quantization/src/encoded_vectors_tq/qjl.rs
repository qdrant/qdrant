use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use super::encoded_vector::NORM_SIZE;

/// Seed for the QJL random projection matrix, distinct from ROTATION_SEED (42).
const QJL_SEED: u64 = 137;

/// Projection dimension for the "short" QJL variants.
pub(super) const QJL_SHORT_DIM: usize = 128;

/// QJL (Quantized Johnson-Lindenstrauss) projection for residual correction.
///
/// Stores a random Gaussian matrix S ∈ ℝ^{m×d} and its transpose Sᵀ,
/// both with i.i.d. N(0,1) entries (NOT orthogonalized).
/// Used to encode the quantization residual as sign bits and reconstruct
/// an approximate correction during decoding.
///
/// `m` = `projection_dim` (number of sign bits stored per vector).
/// `d` = `padded_dim` (dimensionality of the vector space).
/// When m == d this is the full QJL; when m < d this is the "short" variant.
///
/// Algorithm (from TurboQuant_prod, Algorithm 2):
/// - Encode: qjl = sign(S · residual), store with ||residual||₂
/// - Decode: correction = (√(π/2) / m) · ||residual||₂ · Sᵀ · qjl_signs
pub(super) struct QjlProjection {
    /// Row-major random Gaussian matrix S (projection_dim × padded_dim).
    /// Used for encoding: sign(S · residual).
    matrix: Vec<f32>,
    /// Row-major transposed matrix Sᵀ (padded_dim × projection_dim).
    /// Pre-computed for cache-friendly row-sequential access during decoding.
    matrix_t: Vec<f32>,
    padded_dim: usize,
    projection_dim: usize,
}

impl QjlProjection {
    pub fn new(padded_dim: usize, projection_dim: usize) -> Self {
        let matrix = generate_gaussian_matrix(QJL_SEED, projection_dim, padded_dim);
        let matrix_t = transpose(&matrix, projection_dim, padded_dim);
        Self {
            matrix,
            matrix_t,
            padded_dim,
            projection_dim,
        }
    }

    /// Compute sign(S · residual) packed as 1 bit per projection (MSB-first).
    /// Returns (||residual||₂, packed_qjl_bits).
    pub fn encode_residual(&self, residual: &[f32]) -> (f32, Vec<u8>) {
        let residual_norm = {
            let sq: f32 = residual.iter().map(|&x| x * x).sum();
            sq.sqrt()
        };

        let num_bytes = (self.projection_dim + 7) / 8;
        let mut bits = vec![0u8; num_bytes];

        for i in 0..self.projection_dim {
            let row_start = i * self.padded_dim;
            let mut dot = 0.0f32;
            for j in 0..self.padded_dim {
                dot += self.matrix[row_start + j] * residual[j];
            }
            // sign(dot): bit=1 for non-negative, bit=0 for negative
            if dot >= 0.0 {
                bits[i / 8] |= 1 << (7 - (i % 8));
            }
        }

        (residual_norm, bits)
    }

    /// Reconstruct QJL correction: (√(π/2) / m) · residual_norm · Sᵀ · qjl_signs.
    ///
    /// Uses pre-computed Sᵀ stored row-major for cache-friendly sequential access:
    /// row i of Sᵀ = column i of S, so (Sᵀ · signs)[i] = dot(Sᵀ[i], signs).
    pub fn decode_correction(&self, qjl_bits: &[u8], residual_norm: f32) -> Vec<f32> {
        let m = self.projection_dim as f32;
        let scale = (std::f32::consts::FRAC_PI_2).sqrt() / m * residual_norm;

        let mut correction = vec![0.0f32; self.padded_dim];

        for i in 0..self.padded_dim {
            let row_start = i * self.projection_dim;
            let mut sum = 0.0f32;
            for j in 0..self.projection_dim {
                let sign = if (qjl_bits[j / 8] >> (7 - (j % 8))) & 1 != 0 {
                    1.0
                } else {
                    -1.0
                };
                sum += self.matrix_t[row_start + j] * sign;
            }
            correction[i] = scale * sum;
        }

        correction
    }

    /// Precompute S · weighted_query for O(d) per-vector correction scoring.
    ///
    /// Called once per query (O(m·d)), then pass the result to `correction_dot`
    /// for each database vector (O(m) each) instead of calling `decode_correction` (O(m·d) each).
    ///
    /// `weighted_query[i]` should be `rotated_query[i] / scales[i]` when TQ+ is active,
    /// or just `rotated_query[i]` otherwise.
    pub fn project_query(&self, weighted_query: &[f32]) -> Vec<f32> {
        let mut result = vec![0.0f32; self.projection_dim];
        for i in 0..self.projection_dim {
            let row_start = i * self.padded_dim;
            let mut dot = 0.0f32;
            for j in 0..self.padded_dim {
                dot += self.matrix[row_start + j] * weighted_query[j];
            }
            result[i] = dot;
        }
        result
    }

    /// O(m) dot product of the QJL correction with a pre-projected query.
    ///
    /// Returns `(√(π/2) / m) · residual_norm · ⟨projected_query, signs⟩`.
    pub fn correction_dot(
        &self,
        qjl_bits: &[u8],
        residual_norm: f32,
        projected_query: &[f32],
    ) -> f32 {
        let m = self.projection_dim as f32;
        let scale = (std::f32::consts::FRAC_PI_2).sqrt() / m * residual_norm;
        let mut sum = 0.0f32;
        for j in 0..self.projection_dim {
            let sign = if (qjl_bits[j / 8] >> (7 - (j % 8))) & 1 != 0 {
                1.0
            } else {
                -1.0
            };
            sum += projected_query[j] * sign;
        }
        scale * sum
    }

    /// Extra bytes per encoded vector when QJL is active:
    /// residual_norm (f32, 4 bytes) + QJL bit vector (⌈projection_dim/8⌉ bytes).
    pub fn extra_bytes(&self) -> usize {
        NORM_SIZE + (self.projection_dim + 7) / 8
    }
}

/// Transpose a row-major matrix (rows × cols) → (cols × rows).
fn transpose(matrix: &[f32], rows: usize, cols: usize) -> Vec<f32> {
    let mut t = vec![0.0f32; rows * cols];
    for i in 0..rows {
        for j in 0..cols {
            t[j * rows + i] = matrix[i * cols + j];
        }
    }
    t
}

/// Generate a random Gaussian matrix (NOT orthogonalized) with i.i.d. N(0,1) entries.
/// Shape: rows × cols, row-major.
fn generate_gaussian_matrix(seed: u64, rows: usize, cols: usize) -> Vec<f32> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut matrix = vec![0.0f32; rows * cols];
    for val in matrix.iter_mut() {
        *val = random_normal(&mut rng);
    }
    matrix
}

/// Standard normal random number via Box-Muller transform.
fn random_normal(rng: &mut StdRng) -> f32 {
    loop {
        let u1: f32 = rng.random();
        let u2: f32 = rng.random();
        if u1 > 1e-10 {
            return (-2.0 * u1.ln()).sqrt() * (2.0 * std::f32::consts::PI * u2).cos();
        }
    }
}
