//! Randomized Walsh-Hadamard transform for TurboQuant.
//!
//! The transform is: y = (1/√d) · H_d · diag(signs) · x
//! where H_d is the Walsh-Hadamard matrix and signs are random ±1 (Rademacher).

use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};

/// Chunk size for the block-wise Hadamard transform.
const CHUNK_SIZE: usize = 256;

/// Generate deterministic Rademacher vector (±1) of length `dim`.
fn generate_random_signs(dim: usize, seed: u64) -> Vec<f32> {
    let mut rng = SmallRng::seed_from_u64(seed);
    (0..dim)
        .map(|_| {
            if rng.random_bool(0.5) {
                1.0f32
            } else {
                -1.0f32
            }
        })
        .collect()
}

/// In-place Fast Walsh-Hadamard Transform on a flat f32 buffer of length `n` (must be power of 2).
fn fwht_inplace(x: &mut [f32]) {
    let n = x.len();
    debug_assert!(n.is_power_of_two());
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

/// Pad dimension up to the next multiple of `CHUNK_SIZE`.
fn padded_dim(dim: usize) -> usize {
    dim.div_ceil(CHUNK_SIZE) * CHUNK_SIZE
}

/// Manages the randomized Hadamard transform state.
///
/// Instead of padding the full vector to the next power of 2, the vector is
/// split into fixed-size chunks of 256 elements and the Hadamard transform is
/// applied to each chunk independently. The last chunk is zero-padded to 256
/// if needed. This avoids blowing up large dimensions to the next power of 2.
pub struct HadamardTransform {
    dim: usize,
    padded_dim: usize,
    signs: Vec<f32>,
    scale: f32,
}

impl HadamardTransform {
    pub fn new(dim: usize, seed: u64) -> Self {
        let pdim = padded_dim(dim);
        let signs = generate_random_signs(pdim, seed);
        let scale = 1.0 / (CHUNK_SIZE as f32).sqrt();
        Self {
            dim,
            padded_dim: pdim,
            signs,
            scale,
        }
    }

    pub fn padded_dim(&self) -> usize {
        self.padded_dim
    }

    pub(crate) fn signs(&self) -> &[f32] {
        &self.signs
    }

    pub(crate) fn scale(&self) -> f32 {
        self.scale
    }

    /// Forward transform: apply per-chunk `scale · H · diag(signs) · x`.
    ///
    /// Input `x` has length `self.dim`; output has length `self.padded_dim`.
    pub fn forward(&self, x: &[f32]) -> Vec<f32> {
        debug_assert_eq!(x.len(), self.dim);
        let mut buf = vec![0.0f32; self.padded_dim];

        // Copy input and apply random signs.
        for (i, &val) in x.iter().enumerate() {
            buf[i] = val * self.signs[i];
        }
        // Padded positions stay zero.

        // Apply FWHT to each 256-element chunk independently.
        for chunk in buf.chunks_exact_mut(CHUNK_SIZE) {
            fwht_inplace(chunk);
        }

        // Scale.
        for v in &mut buf {
            *v *= self.scale;
        }
        buf
    }

    /// Inverse transform applied per-chunk.
    ///
    /// Input `y` has length `self.padded_dim`; output has length `self.padded_dim`
    /// (caller trims to original dim).
    pub fn inverse(&self, y: &[f32]) -> Vec<f32> {
        debug_assert_eq!(y.len(), self.padded_dim);
        let mut buf = y.to_vec();

        // Apply FWHT to each 256-element chunk independently.
        for chunk in buf.chunks_exact_mut(CHUNK_SIZE) {
            fwht_inplace(chunk);
        }

        for (i, v) in buf.iter_mut().enumerate() {
            *v *= self.scale * self.signs[i];
        }
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_identity() {
        for &dim in &[64, 128, 256, 300, 512, 700] {
            let ht = HadamardTransform::new(dim, 42);
            let x: Vec<f32> = (0..dim).map(|i| i as f32).collect();
            let y = ht.forward(&x);
            assert_eq!(y.len(), ht.padded_dim());
            let z = ht.inverse(&y);
            for i in 0..dim {
                assert!(
                    (x[i] - z[i]).abs() < 1e-3,
                    "dim={dim} mismatch at {i}: {} vs {}",
                    x[i],
                    z[i]
                );
            }
        }
    }

    #[test]
    fn norm_preservation() {
        for &dim in &[128, 256, 300, 512] {
            let ht = HadamardTransform::new(dim, 7);
            let x: Vec<f32> = (0..dim).map(|i| (i as f32).sin()).collect();
            let y = ht.forward(&x);
            let norm_x: f32 = x.iter().map(|v| v * v).sum::<f32>().sqrt();
            let norm_y: f32 = y.iter().map(|v| v * v).sum::<f32>().sqrt();
            assert!(
                (norm_x - norm_y).abs() < 1e-3,
                "dim={dim} norms differ: {norm_x} vs {norm_y}"
            );
        }
    }

    #[test]
    fn padded_dim_is_multiple_of_chunk_size() {
        assert_eq!(padded_dim(1), 256);
        assert_eq!(padded_dim(256), 256);
        assert_eq!(padded_dim(257), 512);
        assert_eq!(padded_dim(1024), 1024);
        assert_eq!(padded_dim(1025), 1280);
    }
}
