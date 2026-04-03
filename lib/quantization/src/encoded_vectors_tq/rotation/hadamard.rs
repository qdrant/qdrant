use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// Chunk size for chunked Walsh-Hadamard Transform. Must be a power of 2.
/// Padded dimension is always a multiple of this value.
const WHT_CHUNK_SIZE: usize = 256;

pub(crate) struct HadamardRotation {
    signs1: Vec<f64>,
    signs2: Vec<f64>,
    /// Three independent permutations applied between the four WHT passes.
    permutations: [(Vec<usize>, Vec<usize>); 3],
    padded_dim: usize,
}

impl HadamardRotation {
    pub fn new(seed: u64, dim: usize) -> Self {
        let padded_dim = compute_padded_dim(dim);
        let mut rng = StdRng::seed_from_u64(seed);
        let signs1 = generate_signs(&mut rng, padded_dim);
        let signs2 = generate_signs(&mut rng, padded_dim);
        let perm1 = generate_permutation(&mut rng, padded_dim);
        let perm2 = generate_permutation(&mut rng, padded_dim);
        let perm3 = generate_permutation(&mut rng, padded_dim);
        Self {
            signs1,
            signs2,
            permutations: [perm1, perm2, perm3],
            padded_dim,
        }
    }

    pub fn padded_dim(&self) -> usize {
        self.padded_dim
    }

    pub fn apply(&self, x: &[f32]) -> Vec<f32> {
        let mut buf = vec![0.0f64; self.padded_dim];
        for (b, &v) in buf.iter_mut().zip(x.iter()) {
            *b = v as f64;
        }
        for (b, &s) in buf.iter_mut().zip(self.signs1.iter()) {
            *b *= s;
        }
        // WHT · P3 · WHT · P2 · WHT · P1 · WHT
        for chunk in buf.chunks_mut(WHT_CHUNK_SIZE) {
            walsh_hadamard_transform(chunk);
        }
        for perm in &self.permutations {
            apply_permutation(&mut buf, &perm.0);
            for chunk in buf.chunks_mut(WHT_CHUNK_SIZE) {
                walsh_hadamard_transform(chunk);
            }
        }
        for (b, &s) in buf.iter_mut().zip(self.signs2.iter()) {
            *b *= s;
        }
        buf.iter().map(|&v| v as f32).collect()
    }

    pub fn apply_inverse(&self, y: &[f32], original_dim: usize) -> Vec<f32> {
        let mut buf = vec![0.0f64; self.padded_dim];
        let len = y.len().min(self.padded_dim);
        for (b, &v) in buf.iter_mut().zip(y[..len].iter()) {
            *b = v as f64;
        }
        for (b, &s) in buf.iter_mut().zip(self.signs2.iter()) {
            *b *= s;
        }
        // WHT · P1⁻¹ · WHT · P2⁻¹ · WHT · P3⁻¹ · WHT
        for chunk in buf.chunks_mut(WHT_CHUNK_SIZE) {
            walsh_hadamard_transform(chunk);
        }
        for perm in self.permutations.iter().rev() {
            apply_permutation(&mut buf, &perm.1);
            for chunk in buf.chunks_mut(WHT_CHUNK_SIZE) {
                walsh_hadamard_transform(chunk);
            }
        }
        for (b, &s) in buf.iter_mut().zip(self.signs1.iter()) {
            *b *= s;
        }
        buf[..original_dim].iter().map(|&v| v as f32).collect()
    }
}

/// Round up to the nearest multiple of `WHT_CHUNK_SIZE`.
pub(crate) fn compute_padded_dim(dim: usize) -> usize {
    (dim + WHT_CHUNK_SIZE - 1) / WHT_CHUNK_SIZE * WHT_CHUNK_SIZE
}

/// Generate a random sign vector (±1.0).
fn generate_signs(rng: &mut StdRng, n: usize) -> Vec<f64> {
    (0..n)
        .map(|_| if rng.random::<bool>() { 1.0f64 } else { -1.0f64 })
        .collect()
}

/// Apply a permutation out-of-place: element at index `i` moves to `perm[i]`.
fn apply_permutation(buf: &mut [f64], perm: &[usize]) {
    let mut tmp = vec![0.0f64; buf.len()];
    for (i, &p) in perm.iter().enumerate() {
        tmp[p] = buf[i];
    }
    buf.copy_from_slice(&tmp);
}

/// Generate a random permutation and its inverse using Fisher-Yates shuffle.
fn generate_permutation(rng: &mut StdRng, n: usize) -> (Vec<usize>, Vec<usize>) {
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

/// In-place normalized Walsh-Hadamard Transform in f64. Self-inverse: WHT(WHT(x)) = x.
/// Input length must be a power of 2.
fn walsh_hadamard_transform(x: &mut [f64]) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        for dim in [128, 200, 384, 512, 768, 1536] {
            let rot = HadamardRotation::new(42, dim);
            let x: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.01 + 0.5).collect();
            let y = rot.apply(&x);
            let x2 = rot.apply_inverse(&y, dim);
            let max_err: f32 = x.iter().zip(x2.iter())
                .map(|(a, b)| (a - b).abs())
                .fold(0.0f32, f32::max);
            println!("dim={dim}: padded={}, max_roundtrip_err={max_err:.2e}", rot.padded_dim());
            assert!(max_err < 1e-5, "roundtrip error too large for dim={dim}: {max_err}");
        }
    }

    #[test]
    fn test_variance_uniformity() {
        let dim = 384;
        let rot = HadamardRotation::new(42, dim);
        // Input with energy concentrated in first block
        let mut x = vec![0.0f32; dim];
        for i in 0..128 {
            x[i] = 1.0;
        }
        let y = rot.apply(&x);
        // Check variance across 256-element blocks
        let chunk_size = WHT_CHUNK_SIZE.min(rot.padded_dim());
        let num_chunks = rot.padded_dim() / chunk_size;
        let mut chunk_energies = Vec::new();
        for c in 0..num_chunks {
            let start = c * chunk_size;
            let end = start + chunk_size;
            let energy: f64 = y[start..end].iter().map(|&v| (v as f64) * (v as f64)).sum();
            chunk_energies.push(energy);
        }
        let total_energy: f64 = chunk_energies.iter().sum();
        let expected_per_chunk = total_energy / num_chunks as f64;
        println!("dim={dim}, padded={}, chunks={num_chunks}", rot.padded_dim());
        for (i, e) in chunk_energies.iter().enumerate() {
            let ratio = e / expected_per_chunk;
            println!("  chunk {i}: energy={e:.4}, ratio={ratio:.4}");
        }
        // With good mixing, each chunk should have roughly equal energy
        for (i, e) in chunk_energies.iter().enumerate() {
            let ratio = e / expected_per_chunk;
            assert!(
                (0.5..2.0).contains(&ratio),
                "chunk {i} energy ratio {ratio:.4} is too far from 1.0"
            );
        }
    }
}
