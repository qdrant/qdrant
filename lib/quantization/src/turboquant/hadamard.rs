//! Randomized Walsh-Hadamard transform for TurboQuant.
//!
//! The transform is: y = (1/√d) · H_d · diag(signs) · x
//! where H_d is the Walsh-Hadamard matrix and signs are random ±1 (Rademacher).

use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};

/// Next power of 2 ≥ n.
fn next_power_of_2(n: usize) -> usize {
    n.next_power_of_two()
}

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

/// Manages the randomized Hadamard transform state.
pub struct HadamardTransform {
    dim: usize,
    padded_dim: usize,
    signs: Vec<f32>,
    scale: f32,
}

impl HadamardTransform {
    pub fn new(dim: usize, seed: u64) -> Self {
        let padded_dim = next_power_of_2(dim);
        let signs = generate_random_signs(padded_dim, seed);
        let scale = 1.0 / (padded_dim as f32).sqrt();
        Self {
            dim,
            padded_dim,
            signs,
            scale,
        }
    }

    pub fn padded_dim(&self) -> usize {
        self.padded_dim
    }

    /// Forward transform: y = scale · H · diag(signs) · x
    ///
    /// Input `x` has length `self.dim`; output has length `self.padded_dim`.
    pub fn forward(&self, x: &[f32]) -> Vec<f32> {
        debug_assert_eq!(x.len(), self.dim);
        let mut buf = vec![0.0f32; self.padded_dim];

        // Copy input and apply random signs.
        for (i, &val) in x.iter().enumerate() {
            buf[i] = val * self.signs[i];
        }
        // Padded positions stay zero (signs applied to zero = zero).

        fwht_inplace(&mut buf);

        // Scale.
        for v in &mut buf {
            *v *= self.scale;
        }
        buf
    }

    /// Inverse transform: x = diag(signs) · H · (scale · y)
    ///
    /// Input `y` has length `self.padded_dim`; output has length `self.padded_dim`
    /// (caller trims to original dim).
    pub fn inverse(&self, y: &[f32]) -> Vec<f32> {
        debug_assert_eq!(y.len(), self.padded_dim);
        let mut buf = y.to_vec();

        // H applied to y, then scaled: since H^{-1} = H/d and scale = 1/√d,
        // inverse = diag(signs) · (scale · H · y).
        fwht_inplace(&mut buf);
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
        let dim = 64;
        let ht = HadamardTransform::new(dim, 42);
        let x: Vec<f32> = (0..dim).map(|i| i as f32).collect();
        let y = ht.forward(&x);
        let z = ht.inverse(&y);
        for i in 0..dim {
            assert!(
                (x[i] - z[i]).abs() < 1e-4,
                "mismatch at {i}: {} vs {}",
                x[i],
                z[i]
            );
        }
    }

    #[test]
    fn norm_preservation() {
        let dim = 128;
        let ht = HadamardTransform::new(dim, 7);
        let x: Vec<f32> = (0..dim).map(|i| (i as f32).sin()).collect();
        let y = ht.forward(&x);
        let norm_x: f32 = x.iter().map(|v| v * v).sum::<f32>().sqrt();
        let norm_y: f32 = y.iter().map(|v| v * v).sum::<f32>().sqrt();
        assert!(
            (norm_x - norm_y).abs() < 1e-3,
            "norms differ: {norm_x} vs {norm_y}"
        );
    }
}
